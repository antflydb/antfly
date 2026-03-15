#!/bin/sh
# scripts/completions.sh
set -e
rm -rf completions
mkdir completions
# https://carlosbecker.com/posts/golang-completions-cobra/
for sh in bash zsh fish; do
	go run ./cmd/antfly completion "$sh" >"completions/antfly.$sh"
done
