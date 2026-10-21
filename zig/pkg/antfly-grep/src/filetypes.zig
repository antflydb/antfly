// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const Options = @import("options.zig").Options;
pub const Definition = struct { name: []const u8, globs: []const []const u8 };
// Catalog generated from ripgrep 15.2 --type-list; no runtime dependency.
pub const definitions = [_]Definition{
    .{ .name = "ada", .globs = &.{ "*.adb", "*.ads" } },
    .{ .name = "agda", .globs = &.{ "*.agda", "*.lagda" } },
    .{ .name = "aidl", .globs = &.{"*.aidl"} },
    .{ .name = "alire", .globs = &.{"alire.toml"} },
    .{ .name = "amake", .globs = &.{ "*.bp", "*.mk" } },
    .{ .name = "asciidoc", .globs = &.{ "*.adoc", "*.asc", "*.asciidoc" } },
    .{ .name = "asm", .globs = &.{ "*.S", "*.asm", "*.s" } },
    .{ .name = "asp", .globs = &.{ "*.ascx", "*.ascx.cs", "*.ascx.vb", "*.asp", "*.aspx", "*.aspx.cs", "*.aspx.vb" } },
    .{ .name = "ats", .globs = &.{ "*.ats", "*.dats", "*.hats", "*.sats" } },
    .{ .name = "avro", .globs = &.{ "*.avdl", "*.avpr", "*.avsc" } },
    .{ .name = "awk", .globs = &.{"*.awk"} },
    .{ .name = "bat", .globs = &.{"*.bat"} },
    .{ .name = "batch", .globs = &.{"*.bat"} },
    .{ .name = "bazel", .globs = &.{ "*.BUILD", "*.bazel", "*.bazelrc", "*.bzl", "BUILD", "MODULE.bazel", "WORKSPACE", "WORKSPACE.bazel", "WORKSPACE.bzlmod" } },
    .{ .name = "bitbake", .globs = &.{ "*.bb", "*.bbappend", "*.bbclass", "*.conf", "*.inc" } },
    .{ .name = "boxlang", .globs = &.{ "*.bx", "*.bxm", "*.bxs" } },
    .{ .name = "brotli", .globs = &.{"*.br"} },
    .{ .name = "buildstream", .globs = &.{"*.bst"} },
    .{ .name = "bzip2", .globs = &.{ "*.bz2", "*.tbz2" } },
    .{ .name = "c", .globs = &.{ "*.[chH]", "*.[chH].in", "*.cats" } },
    .{ .name = "cabal", .globs = &.{"*.cabal"} },
    .{ .name = "candid", .globs = &.{"*.did"} },
    .{ .name = "carp", .globs = &.{"*.carp"} },
    .{ .name = "cbor", .globs = &.{"*.cbor"} },
    .{ .name = "ceylon", .globs = &.{"*.ceylon"} },
    .{ .name = "cfml", .globs = &.{ "*.cfc", "*.cfm" } },
    .{ .name = "clojure", .globs = &.{ "*.clj", "*.cljc", "*.cljs", "*.cljx" } },
    .{ .name = "cmake", .globs = &.{ "*.cmake", "CMakeLists.txt" } },
    .{ .name = "cmd", .globs = &.{ "*.bat", "*.cmd" } },
    .{ .name = "cml", .globs = &.{"*.cml"} },
    .{ .name = "coffeescript", .globs = &.{"*.coffee"} },
    .{ .name = "config", .globs = &.{ "*.cfg", "*.conf", "*.config", "*.ini" } },
    .{ .name = "container", .globs = &.{ "*Containerfile*", "*Dockerfile*" } },
    .{ .name = "coq", .globs = &.{"*.v"} },
    .{ .name = "cpp", .globs = &.{ "*.[ChH]", "*.[ChH].in", "*.[ch]pp", "*.[ch]pp.in", "*.[ch]xx", "*.[ch]xx.in", "*.cc", "*.cc.in", "*.hh", "*.hh.in", "*.inl" } },
    .{ .name = "creole", .globs = &.{"*.creole"} },
    .{ .name = "crystal", .globs = &.{ "*.cr", "*.ecr", "Projectfile", "shard.yml" } },
    .{ .name = "cs", .globs = &.{"*.cs"} },
    .{ .name = "csharp", .globs = &.{"*.cs"} },
    .{ .name = "cshtml", .globs = &.{"*.cshtml"} },
    .{ .name = "csproj", .globs = &.{"*.csproj"} },
    .{ .name = "css", .globs = &.{ "*.css", "*.scss" } },
    .{ .name = "csv", .globs = &.{"*.csv"} },
    .{ .name = "cuda", .globs = &.{ "*.cu", "*.cuh" } },
    .{ .name = "cython", .globs = &.{ "*.pxd", "*.pxi", "*.pyx" } },
    .{ .name = "d", .globs = &.{"*.d"} },
    .{ .name = "dart", .globs = &.{"*.dart"} },
    .{ .name = "devicetree", .globs = &.{ "*.dts", "*.dtsi", "*.dtso" } },
    .{ .name = "dhall", .globs = &.{"*.dhall"} },
    .{ .name = "diff", .globs = &.{ "*.diff", "*.patch" } },
    .{ .name = "dita", .globs = &.{ "*.dita", "*.ditamap", "*.ditaval" } },
    .{ .name = "docker", .globs = &.{"*Dockerfile*"} },
    .{ .name = "dockercompose", .globs = &.{ "docker-compose.*.yml", "docker-compose.yml" } },
    .{ .name = "dts", .globs = &.{ "*.dts", "*.dtsi" } },
    .{ .name = "dvc", .globs = &.{ "*.dvc", "Dvcfile" } },
    .{ .name = "ebuild", .globs = &.{ "*.ebuild", "*.eclass" } },
    .{ .name = "edn", .globs = &.{"*.edn"} },
    .{ .name = "elisp", .globs = &.{"*.el"} },
    .{ .name = "elixir", .globs = &.{ "*.eex", "*.ex", "*.exs", "*.heex", "*.leex", "*.livemd" } },
    .{ .name = "elm", .globs = &.{"*.elm"} },
    .{ .name = "erb", .globs = &.{"*.erb"} },
    .{ .name = "erlang", .globs = &.{ "*.erl", "*.hrl" } },
    .{ .name = "fennel", .globs = &.{"*.fnl"} },
    .{ .name = "fidl", .globs = &.{"*.fidl"} },
    .{ .name = "fish", .globs = &.{"*.fish"} },
    .{ .name = "flatbuffers", .globs = &.{"*.fbs"} },
    .{ .name = "fortran", .globs = &.{ "*.F", "*.F77", "*.F90", "*.F95", "*.f", "*.f77", "*.f90", "*.f95", "*.pfo" } },
    .{ .name = "fsharp", .globs = &.{ "*.fs", "*.fsi", "*.fsx" } },
    .{ .name = "fut", .globs = &.{"*.fut"} },
    .{ .name = "gap", .globs = &.{ "*.g", "*.gap", "*.gd", "*.gi", "*.tst" } },
    .{ .name = "gdscript", .globs = &.{"*.gd"} },
    .{ .name = "gleam", .globs = &.{"*.gleam"} },
    .{ .name = "gn", .globs = &.{ "*.gn", "*.gni" } },
    .{ .name = "go", .globs = &.{"*.go"} },
    .{ .name = "gprbuild", .globs = &.{"*.gpr"} },
    .{ .name = "gradle", .globs = &.{ "*.gradle", "*.gradle.kts", "gradle-wrapper.*", "gradle.properties", "gradlew", "gradlew.bat" } },
    .{ .name = "graphql", .globs = &.{ "*.graphql", "*.graphqls" } },
    .{ .name = "groovy", .globs = &.{ "*.gradle", "*.groovy" } },
    .{ .name = "gzip", .globs = &.{ "*.gz", "*.tgz" } },
    .{ .name = "h", .globs = &.{ "*.h", "*.hh", "*.hpp" } },
    .{ .name = "haml", .globs = &.{"*.haml"} },
    .{ .name = "hare", .globs = &.{"*.ha"} },
    .{ .name = "haskell", .globs = &.{ "*.c2hs", "*.cpphs", "*.hs", "*.hsc", "*.lhs" } },
    .{ .name = "hbs", .globs = &.{"*.hbs"} },
    .{ .name = "hs", .globs = &.{ "*.hs", "*.lhs" } },
    .{ .name = "html", .globs = &.{ "*.ejs", "*.htm", "*.html" } },
    .{ .name = "hurl", .globs = &.{"*.hurl"} },
    .{ .name = "hy", .globs = &.{"*.hy"} },
    .{ .name = "idris", .globs = &.{ "*.idr", "*.lidr" } },
    .{ .name = "janet", .globs = &.{"*.janet"} },
    .{ .name = "java", .globs = &.{ "*.java", "*.jsp", "*.jspx", "*.properties" } },
    .{ .name = "jinja", .globs = &.{ "*.j2", "*.jinja", "*.jinja2" } },
    .{ .name = "jl", .globs = &.{"*.jl"} },
    .{ .name = "js", .globs = &.{ "*.cjs", "*.js", "*.jsx", "*.mjs", "*.vue" } },
    .{ .name = "json", .globs = &.{ "*.json", "*.sarif", "composer.lock" } },
    .{ .name = "jsonl", .globs = &.{"*.jsonl"} },
    .{ .name = "julia", .globs = &.{"*.jl"} },
    .{ .name = "jupyter", .globs = &.{ "*.ipynb", "*.jpynb" } },
    .{ .name = "k", .globs = &.{"*.k"} },
    .{ .name = "kconfig", .globs = &.{ "Kconfig", "Kconfig.*" } },
    .{ .name = "kotlin", .globs = &.{ "*.kt", "*.kts" } },
    .{ .name = "lean", .globs = &.{"*.lean"} },
    .{ .name = "less", .globs = &.{"*.less"} },
    .{ .name = "license", .globs = &.{ "*[.-]LICEN[CS]E*", "AGPL-*[0-9]*", "APACHE-*[0-9]*", "BSD-*[0-9]*", "CC-BY-*", "COPYING", "COPYING[.-]*", "COPYRIGHT", "COPYRIGHT[.-]*", "EULA", "EULA[.-]*", "GFDL-*[0-9]*", "GNU-*[0-9]*", "GPL-*[0-9]*", "LGPL-*[0-9]*", "LICEN[CS]E", "LICEN[CS]E[.-]*", "MIT-*[0-9]*", "MPL-*[0-9]*", "NOTICE", "NOTICE[.-]*", "OFL-*[0-9]*", "PATENTS", "PATENTS[.-]*", "UNLICEN[CS]E", "UNLICEN[CS]E[.-]*", "agpl[.-]*", "gpl[.-]*", "lgpl[.-]*", "licen[cs]e", "licen[cs]e.*" } },
    .{ .name = "lilypond", .globs = &.{ "*.ily", "*.ly" } },
    .{ .name = "lisp", .globs = &.{ "*.el", "*.jl", "*.lisp", "*.lsp", "*.sc", "*.scm" } },
    .{ .name = "llvm", .globs = &.{"*.ll"} },
    .{ .name = "lock", .globs = &.{ "*.lock", "package-lock.json" } },
    .{ .name = "log", .globs = &.{"*.log"} },
    .{ .name = "lua", .globs = &.{"*.lua"} },
    .{ .name = "lz4", .globs = &.{"*.lz4"} },
    .{ .name = "lzma", .globs = &.{"*.lzma"} },
    .{ .name = "m4", .globs = &.{ "*.ac", "*.m4" } },
    .{ .name = "make", .globs = &.{ "*.mak", "*.mk", "Makefile.*", "[Gg][Nn][Uu]makefile", "[Gg][Nn][Uu]makefile.am", "[Gg][Nn][Uu]makefile.in", "[Mm]akefile", "[Mm]akefile.am", "[Mm]akefile.in" } },
    .{ .name = "mako", .globs = &.{ "*.mako", "*.mao" } },
    .{ .name = "man", .globs = &.{ "*.[0-9][cEFMmpSx]", "*.[0-9lnpx]" } },
    .{ .name = "markdown", .globs = &.{ "*.markdown", "*.md", "*.mdown", "*.mdwn", "*.mdx", "*.mkd", "*.mkdn" } },
    .{ .name = "matlab", .globs = &.{"*.m"} },
    .{ .name = "md", .globs = &.{ "*.markdown", "*.md", "*.mdown", "*.mdwn", "*.mdx", "*.mkd", "*.mkdn" } },
    .{ .name = "meson", .globs = &.{ "meson.build", "meson.options", "meson_options.txt" } },
    .{ .name = "minified", .globs = &.{ "*.min.css", "*.min.html", "*.min.js" } },
    .{ .name = "mint", .globs = &.{"*.mint"} },
    .{ .name = "mk", .globs = &.{"mkfile"} },
    .{ .name = "ml", .globs = &.{"*.ml"} },
    .{ .name = "mojo", .globs = &.{"*.mojo"} },
    .{ .name = "motoko", .globs = &.{"*.mo"} },
    .{ .name = "msbuild", .globs = &.{ "*.csproj", "*.fsproj", "*.proj", "*.props", "*.sln", "*.slnf", "*.targets", "*.vcxproj" } },
    .{ .name = "nim", .globs = &.{ "*.nim", "*.nimble", "*.nimf", "*.nims" } },
    .{ .name = "nix", .globs = &.{"*.nix"} },
    .{ .name = "objc", .globs = &.{ "*.h", "*.m" } },
    .{ .name = "objcpp", .globs = &.{ "*.h", "*.mm" } },
    .{ .name = "ocaml", .globs = &.{ "*.ml", "*.mli", "*.mll", "*.mly" } },
    .{ .name = "org", .globs = &.{ "*.org", "*.org_archive" } },
    .{ .name = "pants", .globs = &.{"BUILD"} },
    .{ .name = "pascal", .globs = &.{ "*.dpr", "*.inc", "*.lpr", "*.pas", "*.pp" } },
    .{ .name = "pdf", .globs = &.{"*.pdf"} },
    .{ .name = "perl", .globs = &.{ "*.PL", "*.perl", "*.pl", "*.plh", "*.plx", "*.pm", "*.t" } },
    .{ .name = "php", .globs = &.{ "*.php", "*.php3", "*.php4", "*.php5", "*.php7", "*.php8", "*.pht", "*.phtml" } },
    .{ .name = "pkgbuild", .globs = &.{"PKGBUILD"} },
    .{ .name = "po", .globs = &.{"*.po"} },
    .{ .name = "pod", .globs = &.{"*.pod"} },
    .{ .name = "postscript", .globs = &.{ "*.eps", "*.ps" } },
    .{ .name = "prolog", .globs = &.{ "*.P", "*.pl", "*.pro", "*.prolog" } },
    .{ .name = "proto", .globs = &.{"*.proto"} },
    .{ .name = "protobuf", .globs = &.{"*.proto"} },
    .{ .name = "ps", .globs = &.{ "*.cdxml", "*.ps1", "*.ps1xml", "*.psd1", "*.psm1" } },
    .{ .name = "puppet", .globs = &.{ "*.epp", "*.erb", "*.pp", "*.rb" } },
    .{ .name = "purs", .globs = &.{"*.purs"} },
    .{ .name = "py", .globs = &.{ "*.py", "*.pyi" } },
    .{ .name = "python", .globs = &.{ "*.py", "*.pyi" } },
    .{ .name = "qmake", .globs = &.{ "*.prf", "*.pri", "*.pro" } },
    .{ .name = "qml", .globs = &.{"*.qml"} },
    .{ .name = "qrc", .globs = &.{"*.qrc"} },
    .{ .name = "qui", .globs = &.{"*.ui"} },
    .{ .name = "r", .globs = &.{ "*.R", "*.Rmd", "*.Rnw", "*.r", "*.rmd", "*.rnw" } },
    .{ .name = "racket", .globs = &.{"*.rkt"} },
    .{ .name = "raku", .globs = &.{ "*.p6", "*.pl6", "*.pm6", "*.raku", "*.rakudoc", "*.rakumod", "*.rakutest" } },
    .{ .name = "rdoc", .globs = &.{"*.rdoc"} },
    .{ .name = "readme", .globs = &.{ "*README", "README*" } },
    .{ .name = "reasonml", .globs = &.{ "*.re", "*.rei" } },
    .{ .name = "red", .globs = &.{ "*.r", "*.red", "*.reds" } },
    .{ .name = "rescript", .globs = &.{ "*.res", "*.resi" } },
    .{ .name = "robot", .globs = &.{"*.robot"} },
    .{ .name = "rocq", .globs = &.{"*.v"} },
    .{ .name = "rst", .globs = &.{"*.rst"} },
    .{ .name = "ruby", .globs = &.{ "*.gemspec", "*.rake", "*.rb", "*.rbw", ".irbrc", "Gemfile", "Rakefile", "config.ru" } },
    .{ .name = "rust", .globs = &.{"*.rs"} },
    .{ .name = "sass", .globs = &.{ "*.sass", "*.scss" } },
    .{ .name = "scala", .globs = &.{ "*.sbt", "*.scala" } },
    .{ .name = "scdoc", .globs = &.{ "*.scd", "*.scdoc" } },
    .{ .name = "seed7", .globs = &.{ "*.s7i", "*.sd7" } },
    .{ .name = "sh", .globs = &.{ "*.bash", "*.bashrc", "*.csh", "*.cshrc", "*.env", "*.ksh", "*.kshrc", "*.sh", "*.tcsh", "*.zsh", ".bash_login", ".bash_logout", ".bash_profile", ".bashrc", ".cshrc", ".env", ".kshrc", ".login", ".logout", ".profile", ".tcshrc", ".zlogin", ".zlogout", ".zprofile", ".zshenv", ".zshrc", "bash_login", "bash_logout", "bash_profile", "bashrc", "profile", "zlogin", "zlogout", "zprofile", "zshenv", "zshrc" } },
    .{ .name = "slim", .globs = &.{ "*.skim", "*.slim", "*.slime" } },
    .{ .name = "smarty", .globs = &.{"*.tpl"} },
    .{ .name = "sml", .globs = &.{ "*.sig", "*.sml" } },
    .{ .name = "solidity", .globs = &.{"*.sol"} },
    .{ .name = "soy", .globs = &.{"*.soy"} },
    .{ .name = "spark", .globs = &.{"*.spark"} },
    .{ .name = "spec", .globs = &.{"*.spec"} },
    .{ .name = "sql", .globs = &.{ "*.psql", "*.sql" } },
    .{ .name = "ssa", .globs = &.{"*.ssa"} },
    .{ .name = "stylus", .globs = &.{"*.styl"} },
    .{ .name = "sv", .globs = &.{ "*.h", "*.sv", "*.svh", "*.v", "*.vg" } },
    .{ .name = "svelte", .globs = &.{ "*.svelte", "*.svelte.ts" } },
    .{ .name = "svg", .globs = &.{"*.svg"} },
    .{ .name = "swift", .globs = &.{"*.swift"} },
    .{ .name = "swig", .globs = &.{ "*.def", "*.i" } },
    .{ .name = "systemd", .globs = &.{ "*.automount", "*.conf", "*.device", "*.link", "*.mount", "*.path", "*.scope", "*.service", "*.slice", "*.socket", "*.swap", "*.target", "*.timer" } },
    .{ .name = "taskpaper", .globs = &.{"*.taskpaper"} },
    .{ .name = "tcl", .globs = &.{"*.tcl"} },
    .{ .name = "tex", .globs = &.{ "*.bib", "*.cls", "*.dtx", "*.ins", "*.ltx", "*.sty", "*.tex" } },
    .{ .name = "texinfo", .globs = &.{"*.texi"} },
    .{ .name = "textile", .globs = &.{"*.textile"} },
    .{ .name = "tf", .globs = &.{ "*.terraform.lock.hcl", "*.terraformrc", "*.tf", "*.tf.json", "*.tfrc", "*.tfvars", "*.tfvars.json", "terraform.rc" } },
    .{ .name = "thrift", .globs = &.{"*.thrift"} },
    .{ .name = "toml", .globs = &.{ "*.toml", "Cargo.lock" } },
    .{ .name = "ts", .globs = &.{ "*.cts", "*.mts", "*.ts", "*.tsx" } },
    .{ .name = "twig", .globs = &.{"*.twig"} },
    .{ .name = "txt", .globs = &.{"*.txt"} },
    .{ .name = "typescript", .globs = &.{ "*.cts", "*.mts", "*.ts", "*.tsx" } },
    .{ .name = "typoscript", .globs = &.{ "*.ts", "*.typoscript" } },
    .{ .name = "typst", .globs = &.{"*.typ"} },
    .{ .name = "usd", .globs = &.{ "*.usd", "*.usda", "*.usdc" } },
    .{ .name = "v", .globs = &.{ "*.v", "*.vsh" } },
    .{ .name = "vala", .globs = &.{"*.vala"} },
    .{ .name = "vb", .globs = &.{"*.vb"} },
    .{ .name = "vcl", .globs = &.{"*.vcl"} },
    .{ .name = "verilog", .globs = &.{ "*.sv", "*.svh", "*.v", "*.vh" } },
    .{ .name = "vhdl", .globs = &.{ "*.vhd", "*.vhdl" } },
    .{ .name = "vim", .globs = &.{ "*.vim", ".gvimrc", ".vimrc", "_gvimrc", "_vimrc", "gvimrc", "vimrc" } },
    .{ .name = "vimscript", .globs = &.{ "*.vim", ".gvimrc", ".vimrc", "_gvimrc", "_vimrc", "gvimrc", "vimrc" } },
    .{ .name = "vue", .globs = &.{"*.vue"} },
    .{ .name = "webidl", .globs = &.{ "*.idl", "*.webidl", "*.widl" } },
    .{ .name = "wgsl", .globs = &.{"*.wgsl"} },
    .{ .name = "wiki", .globs = &.{ "*.mediawiki", "*.wiki" } },
    .{ .name = "xml", .globs = &.{ "*.dtd", "*.rng", "*.sch", "*.xhtml", "*.xjb", "*.xml", "*.xml.dist", "*.xsd", "*.xsl", "*.xslt" } },
    .{ .name = "xz", .globs = &.{ "*.txz", "*.xz" } },
    .{ .name = "yacc", .globs = &.{"*.y"} },
    .{ .name = "yaml", .globs = &.{ "*.yaml", "*.yml" } },
    .{ .name = "yang", .globs = &.{"*.yang"} },
    .{ .name = "z", .globs = &.{"*.Z"} },
    .{ .name = "zig", .globs = &.{"*.zig"} },
    .{ .name = "zsh", .globs = &.{ "*.zsh", ".zlogin", ".zlogout", ".zprofile", ".zshenv", ".zshrc", "zlogin", "zlogout", "zprofile", "zshenv", "zshrc" } },
    .{ .name = "zstd", .globs = &.{ "*.zst", "*.zstd" } },
};
pub fn known(o: *const Options, name: []const u8) bool {
    if (std.mem.eql(u8, name, "all")) return true;
    for (definitions) |d| if (std.mem.eql(u8, name, d.name)) return true;
    for (o.type_add.items) |add| {
        const colon = std.mem.indexOfScalar(u8, add, ':') orelse continue;
        if (std.mem.eql(u8, name, add[0..colon])) return true;
    }
    return false;
}
pub fn globs(a: std.mem.Allocator, o: *const Options, name: []const u8) ![]const []const u8 {
    return globsDepth(a, o, name, 0);
}
fn globsDepth(a: std.mem.Allocator, o: *const Options, name: []const u8, depth: usize) anyerror![]const []const u8 {
    if (depth > 16) return error.CyclicTypeDefinition;
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(a);
    for (definitions) |d| if (std.mem.eql(u8, name, d.name) or std.mem.eql(u8, name, "all")) {
        var cleared = false;
        for (o.type_clear.items) |clear| if (std.mem.eql(u8, clear, d.name)) {
            cleared = true;
        };
        if (!cleared) try out.appendSlice(a, d.globs);
    };
    for (o.type_add.items) |add| {
        const colon = std.mem.indexOfScalar(u8, add, ':') orelse return error.InvalidTypeDefinition;
        if (std.mem.eql(u8, name, add[0..colon]) or std.mem.eql(u8, name, "all")) {
            const value = add[colon + 1 ..];
            if (std.mem.startsWith(u8, value, "include:")) {
                var names = std.mem.splitScalar(u8, value[8..], ',');
                while (names.next()) |included| {
                    if (!known(o, included)) return error.UnknownFileType;
                    const patterns = try globsDepth(a, o, included, depth + 1);
                    defer a.free(patterns);
                    try out.appendSlice(a, patterns);
                }
            } else try out.append(a, value);
        }
    }
    return out.toOwnedSlice(a);
}
