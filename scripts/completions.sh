#!/bin/sh
# scripts/completions.sh
set -e
rm -rf completions
mkdir completions
go build -o completions/antfly-completions ./cmd/antfly
# https://carlosbecker.com/posts/golang-completions-cobra/
for sh in bash zsh fish; do
	./completions/antfly-completions completion "$sh" >"completions/antfly.$sh"
done
rm -f completions/antfly-completions
