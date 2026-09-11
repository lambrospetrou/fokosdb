#!/usr/bin/env bash

# Prints an unused error_id segment for a new code: 6 characters from a-hjkmnp-z2-9, which drops i, l, o,
# 0 and 1 so that no two characters look alike. The uniqueness test in errors.test.ts is the final check.

set -eu
cd "$(dirname "$0")/.."

while true; do
	segment=$(LC_ALL=C tr -dc 'a-hjkmnp-z2-9' </dev/urandom | head -c6)
	grep -rqF "$segment" packages/*/src || break
done
echo "$segment"
