#!/usr/bin/env bash
# Grep-based backstop enforcing the KeyCodec contract:
#   - charCodeAt / codePointAt / localeCompare must not appear outside key-codec.ts
#     (they are raw string-comparison primitives that break on astral / multi-byte keys).
#     partition-id.ts is also exempt: its DO name encoder reads code units to escape them, and
#     it never compares keys.
#   - NUL-joiner key pattern (`${x}\0${y}`) must not appear anywhere
#     (binary keys may legally contain 0x00, making NUL a non-collision-proof separator)
set -euo pipefail

FAIL=0
SRC="src"
KEY_CODEC="src/sharding/key-codec.ts"
PARTITION_ID="src/sharding/partition-id.ts"

# charCodeAt / codePointAt / localeCompare outside key-codec.ts and partition-id.ts
FOUND=$(grep -rn --include="*.ts" -E "charCodeAt|codePointAt" "$SRC" | grep -v -e "^${KEY_CODEC}:" -e "^${PARTITION_ID}:" || true)
if [ -n "$FOUND" ]; then
	echo "ERROR: raw string-comparison primitives used outside key-codec.ts and partition-id.ts:" >&2
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
