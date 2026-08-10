#!/usr/bin/env bash
# Scaffold a new Loom pipeline from the skill's template.
#
#   .claude/skills/create-new-pipeline/scripts/new-pipeline.sh <name> [dir]
#
# <name> is kebab-case and becomes the directory, the command name, and the
# pipeline name. [dir] defaults to examples/<name>.
#
# The result compiles and its tests pass before you have edited anything, so
# `go test ./<dir>` is a working baseline you can break deliberately.
set -euo pipefail

name="${1:-}"
if [[ -z "$name" ]]; then
	echo "usage: new-pipeline.sh <name> [dir]" >&2
	exit 2
fi
if [[ ! "$name" =~ ^[a-z][a-z0-9-]*$ ]]; then
	echo "name must be kebab-case (lowercase, digits, hyphens): got '$name'" >&2
	exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
template="$here/../assets/template"
root="$(cd "$here/../../../.." && pwd)"
dest="${2:-$root/examples/$name}"

if [[ -e "$dest" ]]; then
	echo "refusing to overwrite existing $dest" >&2
	exit 1
fi

mkdir -p "$dest"
cp "$template/main.go" "$template/main_test.go" "$template/README.md" "$dest/"

# Precise replacements only — 'example' also appears inside the examples/ path.
for f in "$dest/main.go" "$dest/main_test.go" "$dest/README.md"; do
	sed -i.bak \
		-e "s|Command example is|Command $name is|" \
		-e "s|\./examples/example|./examples/$name|g" \
		-e "s|pipeline\.New(\"example\")|pipeline.New(\"$name\")|" \
		-e "s|^# example$|# $name|" \
		"$f"
	rm -f "$f.bak"
done

echo "scaffolded $dest"
echo
echo "next:"
echo "  go test ./${dest#"$root/"}      # green before you change anything"
echo "  edit build() — the stages are the design"
