#!/usr/bin/env bash
set -euo pipefail

host=""
user=""
repo_path='C:\Users\Haile\OneDrive\Documents\NBNv2'
do_pull=1
do_build=1
do_test=1

usage() {
    cat <<'EOF'
Usage:
  bash tools/dev/pull-windows-build-test.sh --host <ip-or-host> --user <windows-user> [options]

Options:
  --repo-path <windows-path>  Remote repo path. Default:
                              C:\Users\Haile\OneDrive\Documents\NBNv2
  --no-pull                   Skip `git pull --rebase`
  --no-build                  Skip `dotnet build`
  --no-test                   Skip `dotnet test`
EOF
}

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
        --repo-path)
            repo_path="${2:-}"
            shift 2
            ;;
        --no-pull)
            do_pull=0
            shift
            ;;
        --no-build)
            do_build=0
            shift
            ;;
        --no-test)
            do_test=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$host" || -z "$user" ]]; then
    usage >&2
    exit 2
fi

pull_flag='$false'
build_flag='$false'
test_flag='$false'

if [[ $do_pull -eq 1 ]]; then
    pull_flag='$true'
fi

if [[ $do_build -eq 1 ]]; then
    build_flag='$true'
fi

if [[ $do_test -eq 1 ]]; then
    test_flag='$true'
fi

remote_script=$(cat <<EOF
\$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath '$repo_path'
if ($pull_flag) { git pull --rebase }
if ($build_flag) { dotnet build -c Release --disable-build-servers }
if ($test_flag) { dotnet test -c Release --disable-build-servers }
git status --short --branch
EOF
)

encoded_script=$(
    printf '%s' "$remote_script" \
        | iconv -f UTF-8 -t UTF-16LE \
        | base64 -w0
)

ssh \
    -o PreferredAuthentications=publickey \
    -o PasswordAuthentication=no \
    -o IdentitiesOnly=yes \
    -i "${HOME}/.ssh/id_ed25519" \
    -o StrictHostKeyChecking=accept-new \
    "${user}@${host}" \
    "powershell -NoProfile -ExecutionPolicy Bypass -EncodedCommand $encoded_script"
