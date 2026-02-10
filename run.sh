#!/usr/bin/env bash
# Wrapper script to launch the S3 MCP server.
# Ensures uv is found even when the parent process has a limited PATH
# (e.g. when spawned by Claude Desktop or other MCP clients).

set -euo pipefail

# Common installation locations for uv
extra_paths=(
    "$HOME/.local/bin"
    "$HOME/.cargo/bin"
    "/usr/local/bin"
    "/opt/homebrew/bin"
)

for p in "${extra_paths[@]}"; do
    [[ -d "$p" ]] && export PATH="$p:$PATH"
done

if ! command -v uv &>/dev/null; then
    echo "Error: uv not found. Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

exec uv --directory "$SCRIPT_DIR" run s3-mcp-server "$@"
