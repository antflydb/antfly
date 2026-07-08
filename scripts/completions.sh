#!/bin/sh
# scripts/completions.sh
set -e
rm -rf completions
mkdir completions
echo "building antfly for shell completions"
go build -tags "${ANTFLY_COMPLETIONS_TAGS:-afrelease}" -o completions/antfly-completions ./cmd/antfly
# https://carlosbecker.com/posts/golang-completions-cobra/
for sh in bash zsh fish; do
	echo "generating $sh completion"
	./completions/antfly-completions completion "$sh" >"completions/antfly.$sh"
done
rm -f completions/antfly-completions
echo "shell completions generated"
