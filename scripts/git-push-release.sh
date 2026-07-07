#!/usr/bin/env bash
# Push release a GitHub cuando git portable no encuentra remote-https.
# Autenticación opcional: export GITHUB_TOKEN=ghp_... (no commitear el token).
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

REMOTE="${GIT_REMOTE:-origin}"
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  REMOTE="https://x-access-token:${GITHUB_TOKEN}@github.com/nerthzbyt/Restructured.git"
fi

echo "Using: $GIT (GIT_EXEC_PATH=${GIT_EXEC_PATH:-unset})"
echo "Branch: $("$GIT" rev-parse --abbrev-ref HEAD) @ $("$GIT" rev-parse --short HEAD)"
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  echo "Remote: https://github.com/nerthzbyt/Restructured.git (token auth)"
else
  echo "Remote: $REMOTE"
fi
echo ""

"$GIT" push "$REMOTE" main
"$GIT" push "$REMOTE" feat/linux-platform-v5.1.0 2>/dev/null || true
"$GIT" push "$REMOTE" v5.1.0-linux 2>/dev/null || true

echo ""
echo "OK. Verifica: https://github.com/nerthzbyt/Restructured/commits/main"
echo "Pages: https://nerthzbyt.github.io/Restructured/"
echo "CI: https://github.com/nerthzbyt/Restructured/actions"