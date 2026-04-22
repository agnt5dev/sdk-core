#!/bin/bash
# Download QuickJS WASI binary for the WasmSandbox.
#
# Usage:
#   ./download.sh
#   AGNT5_QUICKJS_WASM_PATH=$(pwd)/qjs-wasi.wasm cargo test --features wasm-sandbox
#
# Source: https://github.com/quickjs-ng/quickjs/releases

set -euo pipefail

VERSION="v0.13.0"
URL="https://github.com/quickjs-ng/quickjs/releases/download/${VERSION}/qjs-wasi.wasm"
OUTPUT="$(dirname "$0")/qjs-wasi.wasm"

if [ -f "$OUTPUT" ]; then
    echo "qjs-wasi.wasm already exists at $OUTPUT"
    exit 0
fi

echo "Downloading QuickJS WASI ${VERSION}..."
curl -L -o "$OUTPUT" "$URL"

echo "Downloaded to $OUTPUT ($(wc -c < "$OUTPUT" | tr -d ' ') bytes)"
echo ""
echo "To use with WasmSandbox:"
echo "  export AGNT5_QUICKJS_WASM_PATH=$OUTPUT"
