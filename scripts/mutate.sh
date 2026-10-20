#!/usr/bin/env bash
# applies each reintroduced bug in mutations/ and reports how often the tests catch it
set -euo pipefail
cd "$(dirname "$0")/.."
runs=${RUNS:-20}
[ $# -eq 0 ] && set -- mutations/*.patch

for patch in "$@"; do
	git apply "$patch"
	trap 'git apply -R "$patch"' EXIT
	out=$(go test -race -count="$runs" -v ./... 2>&1 || true)
	git apply -R "$patch"
	trap - EXIT
	if grep -q '\[build failed\]' <<<"$out"; then
		echo "$patch does not build:" >&2
		echo "$out" >&2
		exit 1
	fi
	echo "$(basename "$patch" .patch):"
	grep '^--- FAIL' <<<"$out" | awk '{print $3}' | sort | uniq -c | awk -v r="$runs" '{print "  failed " $1 "/" r "  " $2}' || echo "  not caught"
done
