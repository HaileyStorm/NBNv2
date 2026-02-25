#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROJECT_PATH="${REPO_ROOT}/src/Nbn.Runtime.WorkerNode/Nbn.Runtime.WorkerNode.csproj"

CONFIGURATION="${CONFIGURATION:-Release}"
OUTPUT_ROOT="${OUTPUT_ROOT:-artifacts/dist/worker-node}"
SELF_CONTAINED="${SELF_CONTAINED:-true}"

if [[ ! -f "${PROJECT_PATH}" ]]; then
  echo "WorkerNode project not found: ${PROJECT_PATH}" >&2
  exit 1
fi

if [[ "$#" -gt 0 ]]; then
  RIDS=("$@")
else
  RIDS=("linux-x64" "win-x64")
fi

echo "Publishing Nbn.Runtime.WorkerNode"
echo "  configuration: ${CONFIGURATION}"
echo "  self-contained: ${SELF_CONTAINED}"
echo "  rids: ${RIDS[*]}"

for rid in "${RIDS[@]}"; do
  if [[ -z "${rid}" ]]; then
    continue
  fi

  output_dir="${REPO_ROOT}/${OUTPUT_ROOT}/${rid}"
  mkdir -p "${output_dir}"

  echo "Publishing RID ${rid} to ${output_dir} ..."
  dotnet publish "${PROJECT_PATH}" \
    -c "${CONFIGURATION}" \
    --disable-build-servers \
    -r "${rid}" \
    --self-contained "${SELF_CONTAINED}" \
    /p:PublishSingleFile=true \
    /p:PublishTrimmed=false \
    /p:IncludeNativeLibrariesForSelfExtract=true \
    /p:DebugType=None \
    /p:DebugSymbols=false \
    -o "${output_dir}"
done

echo "WorkerNode publish complete."
echo "Artifacts root: ${REPO_ROOT}/${OUTPUT_ROOT}"
