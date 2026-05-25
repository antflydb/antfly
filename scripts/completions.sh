#!/bin/sh
# scripts/completions.sh
set -e
rm -rf completions
mkdir completions
# https://carlosbecker.com/posts/golang-completions-cobra/
for sh in bash zsh fish; do
	(cd go/pkg/antfly && go run ./cmd completion "$sh" >"../../../completions/antfly.$sh")
done
