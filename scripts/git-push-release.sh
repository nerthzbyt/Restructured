#!/usr/bin/env bash
# Push release a GitHub cuando git portable no encuentra remote-https.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ -d "$HOME/.local/usr/lib/git-core" ]; then
  export GIT_EXEC_PATH="$HOME/.local/usr/lib/git-core"
fi

GIT="${GIT:-git}"
if command -v /usr/bin/git >/dev/null 2>&1; then
  GIT=/usr/bin/git
elif [ -x "$HOME/.local/usr/bin/git" ]; then
  GIT="$HOME/.local/usr/bin/git"
fi

echo "Using: $GIT (GIT_EXEC_PATH=${GIT_EXEC_PATH:-unset})"
echo "Branch: $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
echo ""

"$GIT" push origin main
"$GIT" push origin feat/linux-platform-v5.1.0 2>/dev/null || true
"$GIT" push origin v5.1.0-linux 2>/dev/null || true

echo ""
echo "OK. Verifica: https://github.com/nerthzbyt/Restructured/commits/main"
echo "Pages: Settings → Pages → Source: GitHub Actions"