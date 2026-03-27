#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PUSH_REMOTE="${PUSH_REMOTE:-origin}"
CONFIRM=false

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --confirm)
      CONFIRM=true
      shift
      ;;
    --push-remote)
      PUSH_REMOTE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $(basename "$0") [--confirm] [--push-remote <remote>]" >&2
      exit 1
      ;;
  esac
done

cd "${REPO_ROOT}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Release requires a clean git worktree." >&2
  exit 1
fi

git fetch --tags --force
VERSION="$(python3 tools/dist/build-release.py print-version | tail -n 1)"
TAG="v${VERSION}"

echo "Planned release tag: ${TAG}"
bash tools/docs/render-nbnv2-docs.sh --check
dotnet build NBNv2.sln -c Release --disable-build-servers
dotnet test NBNv2.sln -c Release --disable-build-servers

if [[ "${CONFIRM}" != "true" ]]; then
  echo "Validation succeeded. Re-run with --confirm to create and push ${TAG}."
  exit 0
fi

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "Tag already exists: ${TAG}" >&2
  exit 1
fi

git tag -a "${TAG}" -m "NBN ${VERSION}"
git push "${PUSH_REMOTE}" "${TAG}"
echo "Pushed release tag ${TAG} to ${PUSH_REMOTE}."
