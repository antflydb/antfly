#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-completions}"
rm -rf "$output_dir"
mkdir -p "$output_dir"

commands="data metadata standalone swarm inference serverless lite ha table index artifact query lookup load insert delete agents backup restore auth internal cloud help version"

cat >"$output_dir/antfly.bash" <<EOF
_antfly() {
  local cur prev
  COMPREPLY=()
  cur="\${COMP_WORDS[COMP_CWORD]}"
  prev="\${COMP_WORDS[COMP_CWORD-1]}"
  if (( COMP_CWORD == 1 )); then
    COMPREPLY=(\$(compgen -W "$commands" -- "\$cur"))
    return
  fi
  case "\${COMP_WORDS[1]}" in
    table|index) COMPREPLY=(\$(compgen -W "create drop list get" -- "\$cur")) ;;
    artifact) COMPREPLY=(\$(compgen -W "list get put delete reprocess job" -- "\$cur")) ;;
    agents) COMPREPLY=(\$(compgen -W "retrieval query-builder" -- "\$cur")) ;;
    auth) COMPREPLY=(\$(compgen -W "me users permissions roles row-filters subjects api-keys" -- "\$cur")) ;;
    inference) COMPREPLY=(\$(compgen -W "run embed classify generate chat compile-artifact export quantize run-artifact transcribe read extract compare finetune smoke list pull convert" -- "\$cur")) ;;
    lite) COMPREPLY=(\$(compgen -W "init status info batch lookup scan query index enrichment schema run-until-idle backup export snapshot restore import promote check compact vacuum serve" -- "\$cur")) ;;
    serverless) COMPREPLY=(\$(compgen -W "api query maintenance combined" -- "\$cur")) ;;
    internal) COMPREPLY=(\$(compgen -W "metadata" -- "\$cur")) ;;
  esac
}
complete -F _antfly antfly
EOF

cat >"$output_dir/antfly.zsh" <<EOF
#compdef antfly

_antfly() {
  local -a commands subcommands
  commands=(
    'data:Run a data node'
    'metadata:Run a metadata node'
    'standalone:Run a standalone server'
    'swarm:Run a standalone server (legacy alias)'
    'inference:Manage the inference runtime'
    'serverless:Run serverless commands'
    'lite:Manage embedded Antfly Lite databases'
    'ha:Manage local hot-standby HA'
    'table:Manage tables'
    'index:Manage indexes'
    'artifact:Manage generated artifacts'
    'query:Query table data'
    'lookup:Look up a document by key'
    'load:Bulk-load NDJSON data'
    'insert:Insert a document'
    'delete:Delete a document'
    'agents:Run AI agents'
    'backup:Back up tables'
    'restore:Restore tables'
    'auth:Manage users and authorization'
    'internal:Run internal cluster commands'
    'cloud:Delegate to the Antfly Cloud CLI'
    'help:Show command help'
    'version:Show the Antfly version'
  )
  if (( CURRENT == 2 )); then
    _describe 'command' commands
    return
  fi

  case "\$words[2]" in
    table|index)
      subcommands=(create drop list get)
      ;;
    artifact)
      subcommands=(list get put delete reprocess job)
      ;;
    agents)
      subcommands=(retrieval query-builder)
      ;;
    auth)
      subcommands=(me users permissions roles row-filters subjects api-keys)
      ;;
    inference)
      subcommands=(run embed classify generate chat compile-artifact export quantize run-artifact transcribe read extract compare finetune smoke list pull convert)
      ;;
    lite)
      subcommands=(init status info batch lookup scan query index enrichment schema run-until-idle backup export snapshot restore import promote check compact vacuum serve)
      ;;
    serverless)
      subcommands=(api query maintenance combined)
      ;;
    internal)
      subcommands=(metadata)
      ;;
  esac
  if (( \${#subcommands[@]} )); then
    _describe 'subcommand' subcommands
  fi
}

compdef _antfly antfly
EOF

cat >"$output_dir/antfly.fish" <<EOF
complete -c antfly -f
complete -c antfly -n '__fish_use_subcommand' -a '$commands'
complete -c antfly -n '__fish_seen_subcommand_from table index' -a 'create drop list get'
complete -c antfly -n '__fish_seen_subcommand_from artifact' -a 'list get put delete reprocess job'
complete -c antfly -n '__fish_seen_subcommand_from agents' -a 'retrieval query-builder'
complete -c antfly -n '__fish_seen_subcommand_from auth' -a 'me users permissions roles row-filters subjects api-keys'
complete -c antfly -n '__fish_seen_subcommand_from inference' -a 'run embed classify generate chat compile-artifact export quantize run-artifact transcribe read extract compare finetune smoke list pull convert'
complete -c antfly -n '__fish_seen_subcommand_from lite' -a 'init status info batch lookup scan query index enrichment schema run-until-idle backup export snapshot restore import promote check compact vacuum serve'
complete -c antfly -n '__fish_seen_subcommand_from serverless' -a 'api query maintenance combined'
complete -c antfly -n '__fish_seen_subcommand_from internal' -a 'metadata'
EOF
