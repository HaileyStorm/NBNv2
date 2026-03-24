#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash tools/dev/pull-windows-workbench-logs.sh --host <windows-host> --user <windows-user> [options]

Options:
  --host <host>           Windows laptop hostname or IP address.
  --user <user>           Windows SSH username.
  --port <port>           SSH port. Default: 22.
  --remote-dir <path>     Remote log directory. Default: %LOCALAPPDATA%\Nbn.Workbench\logs
  --dest <dir>            Local destination directory. Default: ~/Downloads/NBNLogs
  --keep-zip              Keep the downloaded zip alongside the extracted logs.
  --help                  Show this help text.

Requirements:
  - OpenSSH Server enabled on the Windows laptop.
  - SSH authentication already working from this machine to the laptop.

Examples:
  bash tools/dev/pull-windows-workbench-logs.sh --host 192.168.0.103 --user Haile
  bash tools/dev/pull-windows-workbench-logs.sh --host laptop-host --user Haile --dest ~/Downloads/NBNLogs-laptop
EOF
}

host=""
user=""
port="22"
remote_dir=""
dest_dir="${HOME}/Downloads/NBNLogs"
keep_zip=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      host="${2:-}"
      shift 2
      ;;
    --user)
      user="${2:-}"
      shift 2
      ;;
    --port)
      port="${2:-}"
      shift 2
      ;;
    --remote-dir)
      remote_dir="${2:-}"
      shift 2
      ;;
    --dest)
      dest_dir="${2:-}"
      shift 2
      ;;
    --keep-zip)
      keep_zip=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$host" || -z "$user" ]]; then
  echo "--host and --user are required." >&2
  usage >&2
  exit 1
fi

if ! command -v ssh >/dev/null 2>&1; then
  echo "ssh is required but not installed." >&2
  exit 1
fi

if ! command -v scp >/dev/null 2>&1; then
  echo "scp is required but not installed." >&2
  exit 1
fi

ps_quote() {
  printf "%s" "$1" | sed "s/'/''/g"
}

extract_zip() {
  local zip_path="$1"
  local out_dir="$2"

  rm -rf "$out_dir"
  mkdir -p "$out_dir"

  if command -v unzip >/dev/null 2>&1; then
    unzip -oq "$zip_path" -d "$out_dir"
    return 0
  fi

  if command -v bsdtar >/dev/null 2>&1; then
    bsdtar -xf "$zip_path" -C "$out_dir"
    return 0
  fi

  echo "Neither unzip nor bsdtar is available for extracting $zip_path." >&2
  return 1
}

remote_dir_escaped="$(ps_quote "$remote_dir")"

read -r -d '' ps_script <<EOF || true
\$ErrorActionPreference = 'Stop'
\$remoteDir = '${remote_dir_escaped}'
if ([string]::IsNullOrWhiteSpace(\$remoteDir)) {
    \$remoteDir = Join-Path \$env:LOCALAPPDATA 'Nbn.Workbench\\logs'
}
if (-not (Test-Path -LiteralPath \$remoteDir)) {
    throw "Log directory not found: \$remoteDir"
}
\$computer = if ([string]::IsNullOrWhiteSpace(\$env:COMPUTERNAME)) { 'windows-host' } else { \$env:COMPUTERNAME }
\$zipPath = Join-Path \$env:TEMP ("nbnlogs-" + \$computer + ".zip")
if (Test-Path -LiteralPath \$zipPath) {
    Remove-Item -LiteralPath \$zipPath -Force
}
Compress-Archive -Path (Join-Path \$remoteDir '*') -DestinationPath \$zipPath -Force
Write-Output (\$zipPath -replace '\\\\', '/')
EOF

encoded_command="$(printf '%s' "$ps_script" | iconv -f UTF-8 -t UTF-16LE | base64 -w0)"
target="${user}@${host}"

mkdir -p "$(dirname "$dest_dir")"
scratch_dir="$(mktemp -d)"
trap 'rm -rf "$scratch_dir"' EXIT

remote_zip="$(
  ssh -p "$port" "$target" "powershell -NoProfile -EncodedCommand $encoded_command" \
    | tr -d '\r' \
    | tail -n 1
)"

if [[ -z "$remote_zip" ]]; then
  echo "Failed to determine remote zip path from $target." >&2
  exit 1
fi

local_zip="${scratch_dir}/$(basename "$remote_zip")"
scp -P "$port" "$target:$remote_zip" "$local_zip"

cleanup_path_escaped="$(ps_quote "$remote_zip")"
read -r -d '' cleanup_script <<EOF || true
\$ErrorActionPreference = 'Stop'
\$zipPath = '${cleanup_path_escaped}'
if (Test-Path -LiteralPath \$zipPath) {
    Remove-Item -LiteralPath \$zipPath -Force
}
EOF

cleanup_encoded="$(printf '%s' "$cleanup_script" | iconv -f UTF-8 -t UTF-16LE | base64 -w0)"
ssh -p "$port" "$target" "powershell -NoProfile -EncodedCommand $cleanup_encoded" >/dev/null

extract_zip "$local_zip" "$dest_dir"

if [[ "$keep_zip" -eq 1 ]]; then
  cp "$local_zip" "${dest_dir%/}.zip"
fi

printf 'Pulled Windows Workbench logs from %s into %s\n' "$target" "$dest_dir"
