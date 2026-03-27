#!/usr/bin/env bash
set -euo pipefail

DISPLAY_NAME="__DISPLAY_NAME__"
DEFAULT_INSTALL_ROOT="__DEFAULT_INSTALL_ROOT__"
ALIASES=(__ALIASES__)
INSTALL_ROOT="${DEFAULT_INSTALL_ROOT}"
BIN_DIR="/usr/bin"
DESKTOP_DIR="/usr/share/applications"
ICON_ROOT="/usr/share/icons"
INSTALL_DESKTOP_ENTRY=true

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
    --desktop-dir)
      DESKTOP_DIR="$2"
      shift 2
      ;;
    --icon-dir)
      ICON_ROOT="$2"
      shift 2
      ;;
    --no-bin-links)
      BIN_DIR=""
      shift
      ;;
    --no-desktop-entry)
      INSTALL_DESKTOP_ENTRY=false
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

desktop_template="${INSTALL_ROOT}/share/applications/nbn-workbench.desktop.template"
desktop_file="${DESKTOP_DIR}/nbn-workbench.desktop"
icon_source="${INSTALL_ROOT}/share/icons/hicolor/256x256/apps/nbn-workbench.png"
icon_target="${ICON_ROOT}/hicolor/256x256/apps/nbn-workbench.png"
if [[ "${INSTALL_DESKTOP_ENTRY}" == "true" && -f "${desktop_template}" ]]; then
  escaped_install_root="${INSTALL_ROOT//&/\\&}"
  if [[ -f "${icon_source}" && -n "${ICON_ROOT}" ]]; then
    mkdir -p "$(dirname "${icon_target}")"
    cp -f "${icon_source}" "${icon_target}"
  fi
  if [[ -n "${DESKTOP_DIR}" ]]; then
    mkdir -p "${DESKTOP_DIR}"
    sed "s|__INSTALL_ROOT__|${escaped_install_root}|g" "${desktop_template}" > "${desktop_file}"
  fi
  if command -v update-desktop-database >/dev/null 2>&1 && [[ -n "${DESKTOP_DIR}" && -d "${DESKTOP_DIR}" ]]; then
    update-desktop-database "${DESKTOP_DIR}" >/dev/null 2>&1 || true
  fi
  if command -v gtk-update-icon-cache >/dev/null 2>&1 && [[ -n "${ICON_ROOT}" && -d "${ICON_ROOT}/hicolor" ]]; then
    gtk-update-icon-cache -q -t "${ICON_ROOT}/hicolor" >/dev/null 2>&1 || true
  fi
fi

echo "Installed ${DISPLAY_NAME} to ${INSTALL_ROOT}."
if [[ -n "${BIN_DIR}" ]]; then
  echo "Linked aliases into ${BIN_DIR}."
else
  echo "Skipped command symlink creation."
fi
if [[ "${INSTALL_DESKTOP_ENTRY}" == "true" && -f "${desktop_template}" ]]; then
  echo "Installed Workbench desktop entry into ${DESKTOP_DIR}."
fi
echo "Use ${INSTALL_ROOT}/uninstall.sh ${BIN_DIR} ${DESKTOP_DIR} ${ICON_ROOT} to remove it."
exit 0
__ARCHIVE_BELOW__
