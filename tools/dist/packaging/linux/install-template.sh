#!/usr/bin/env bash
set -euo pipefail

DISPLAY_NAME="__DISPLAY_NAME__"
DEFAULT_INSTALL_ROOT="__DEFAULT_INSTALL_ROOT__"
ALIASES=(__ALIASES__)
INSTALL_ROOT="${DEFAULT_INSTALL_ROOT}"
BIN_DIR="/usr/bin"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix|--install-root)
      INSTALL_ROOT="$2"
      shift 2
      ;;
    --bin-dir)
      BIN_DIR="$2"
      shift 2
      ;;
    --no-bin-links)
      BIN_DIR=""
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

PAYLOAD_LINE="$(awk '/^__ARCHIVE_BELOW__$/ { print NR + 1; exit 0; }' "$0")"
if [[ -z "${PAYLOAD_LINE}" ]]; then
  echo "Installer payload marker not found." >&2
  exit 1
fi

TEMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TEMP_DIR}"
}
trap cleanup EXIT

tail -n +"${PAYLOAD_LINE}" "$0" | tar -xz -C "${TEMP_DIR}"

mkdir -p "${INSTALL_ROOT}"
cp -a "${TEMP_DIR}/payload/." "${INSTALL_ROOT}/"

if [[ -n "${BIN_DIR}" ]]; then
  mkdir -p "${BIN_DIR}"
  for alias in "${ALIASES[@]}"; do
    ln -sfn "${INSTALL_ROOT}/bin/${alias}" "${BIN_DIR}/${alias}"
  done
fi

echo "Installed ${DISPLAY_NAME} to ${INSTALL_ROOT}."
if [[ -n "${BIN_DIR}" ]]; then
  echo "Linked aliases into ${BIN_DIR}."
else
  echo "Skipped command symlink creation."
fi
echo "Use ${INSTALL_ROOT}/uninstall.sh ${BIN_DIR} to remove it."
exit 0
__ARCHIVE_BELOW__
