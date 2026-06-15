#!/usr/bin/env bash
# Grep-based backstop enforcing the KeyCodec contract:
#   - charCodeAt / codePointAt / localeCompare must not appear outside key-codec.ts
#     (they are raw string-comparison primitives that break on astral / multi-byte keys)
#   - NUL-joiner key pattern (`${x}\0${y}`) must not appear anywhere
#     (binary keys may legally contain 0x00, making NUL a non-collision-proof separator)
set -euo pipefail

FAIL=0
SRC="src"
KEY_CODEC="src/lib/partition-topology/key-codec.ts"

# charCodeAt / codePointAt / localeCompare outside key-codec.ts
FOUND=$(grep -rn --include="*.ts" -E "charCodeAt|codePointAt|localeCompare" "$SRC" | grep -v "^${KEY_CODEC}:" || true)
if [ -n "$FOUND" ]; then
	echo "ERROR: raw string-comparison primitives used outside key-codec.ts:" >&2
	echo "$FOUND" >&2
	FAIL=1
fi

# NUL-joiner pattern in source (not just comments — skip lines starting with // or *)
NUL_FOUND=$(grep -rn --include="*.ts" -E '`\$\{[^`]*\}\\0\$\{' "$SRC" | grep -vE '^\s*//' || true)
if [ -n "$NUL_FOUND" ]; then
	echo "ERROR: NUL-joiner key pattern found (use KeyCodec.pairKey instead):" >&2
	echo "$NUL_FOUND" >&2
	FAIL=1
fi

if [ $FAIL -eq 0 ]; then
	echo "Key invariant checks passed."
fi
exit $FAIL
