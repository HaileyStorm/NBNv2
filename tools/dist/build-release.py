#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Iterable
from xml.etree import ElementTree


SEMVER_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
RELEASE_TAG_RE = re.compile(r"^v(?P<version>\d+\.\d+\.\d+)$")


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / "NBNv2.sln").exists() and (candidate / "Directory.Build.props").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {start}.")


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = find_repo_root(SCRIPT_DIR)
CONFIG_PATH = REPO_ROOT / "tools" / "dist" / "release-config.json"


@dataclass(frozen=True)
class Bundle:
    name: str
    artifact_prefix: str
    display_name: str
    linux_install_root: str
    windows_install_dir_name: str
    include_runbooks: bool


@dataclass(frozen=True)
class Application:
    id: str
    project: Path
    executable: str
    alias: str
    install_subdir: str
    bundles: tuple[str, ...]

    def binary_name(self, platform: str) -> str:
        return self.executable + ".exe" if platform == "windows" else self.executable


@dataclass(frozen=True)
class ReleaseConfig:
    version_file: Path
    tag_prefix: str
    runbooks: tuple[Path, ...]
    bundles: dict[str, Bundle]
    applications: tuple[Application, ...]


@dataclass(frozen=True)
class PublishedApplication:
    application: Application
    output_dir: Path
    executable_path: Path


def load_release_config() -> ReleaseConfig:
    raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    bundles = {
        name: Bundle(
            name=name,
            artifact_prefix=value["artifact_prefix"],
            display_name=value["display_name"],
            linux_install_root=value["linux_install_root"],
            windows_install_dir_name=value["windows_install_dir_name"],
            include_runbooks=bool(value.get("include_runbooks", False)),
        )
        for name, value in raw["bundles"].items()
    }
    applications = tuple(
        Application(
            id=item["id"],
            project=Path(item["project"]),
            executable=item["executable"],
            alias=item["alias"],
            install_subdir=item["install_subdir"],
            bundles=tuple(item["bundles"]),
        )
        for item in raw["applications"]
    )
    return ReleaseConfig(
        version_file=REPO_ROOT / raw["version_file"],
        tag_prefix=raw["tag_prefix"],
        runbooks=tuple(REPO_ROOT / path for path in raw.get("runbooks", [])),
        bundles=bundles,
        applications=applications,
    )


def parse_version_text(version_text: str) -> tuple[int, int, int]:
    match = SEMVER_RE.fullmatch(version_text.strip())
    if match is None:
        raise ValueError(f"Invalid version '{version_text}'. Expected <major>.<minor>.<patch>.")
    return int(match["major"]), int(match["minor"]), int(match["patch"])


def compute_file_version(version_text: str) -> str:
    major, minor, patch = parse_version_text(version_text)
    return f"{major}.{minor}.{patch}.0"


def run(
    command: list[str],
    *,
    cwd: Path = REPO_ROOT,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        check=False,
        text=True,
        capture_output=capture_output,
    )
    if result.returncode != 0:
        detail = ""
        if capture_output:
            detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}"
            + (f"\n{detail}" if detail else "")
        )
    return result


def resolve_next_version(config: ReleaseConfig) -> str:
    raw = json.loads(config.version_file.read_text(encoding="utf-8"))
    major = int(raw["major"])
    minor = int(raw["minor"])
    if major != 2:
        raise ValueError("release/version.json major must remain 2 in this repository.")

    tag_pattern = f"{config.tag_prefix}{major}.{minor}.*"
    result = run(["git", "tag", "--list", tag_pattern], capture_output=True)
    patches: list[int] = []
    for tag in result.stdout.splitlines():
        tag = tag.strip()
        if not tag:
            continue
        if not tag.startswith(config.tag_prefix):
            continue
        try:
            tag_major, tag_minor, tag_patch = parse_version_text(tag[len(config.tag_prefix):])
        except ValueError:
            continue
        if tag_major == major and tag_minor == minor:
            patches.append(tag_patch)

    patch = 0 if not patches else max(patches) + 1
    return f"{major}.{minor}.{patch}"


def validate_release_tag(config: ReleaseConfig, tag: str) -> str:
    match = RELEASE_TAG_RE.fullmatch(tag.strip())
    if match is None:
        raise ValueError(f"Release tag '{tag}' must match v<major>.<minor>.<patch>.")

    version = match["version"]
    tag_major, tag_minor, _ = parse_version_text(version)
    raw = json.loads(config.version_file.read_text(encoding="utf-8"))
    if tag_major != int(raw["major"]) or tag_minor != int(raw["minor"]):
        raise ValueError(
            f"Release tag '{tag}' does not match release/version.json line {raw['major']}.{raw['minor']}.x."
        )

    return version


def discover_executable_projects() -> set[str]:
    projects: set[str] = set()
    for root_name in ("src", "tools"):
        for path in (REPO_ROOT / root_name).rglob("*.csproj"):
            tree = ElementTree.parse(path)
            output_type = None
            for element in tree.iter():
                if element.tag.endswith("OutputType"):
                    output_type = (element.text or "").strip()
                    break
            if output_type in {"Exe", "WinExe"}:
                projects.add(path.relative_to(REPO_ROOT).as_posix())
    return projects


def validate_config(config: ReleaseConfig) -> None:
    if not config.version_file.exists():
        raise ValueError(f"Version file not found: {config.version_file}")

    raw_version = json.loads(config.version_file.read_text(encoding="utf-8"))
    if int(raw_version["major"]) != 2:
        raise ValueError("release/version.json major must remain 2.")

    for runbook in config.runbooks:
        if not runbook.exists():
            raise ValueError(f"Configured runbook does not exist: {runbook}")

    bundle_names = set(config.bundles)
    if {"suite", "worker"} - bundle_names:
        raise ValueError("release-config.json must define suite and worker bundles.")

    aliases: set[str] = set()
    ids: set[str] = set()
    configured_projects: set[str] = set()
    for application in config.applications:
        if application.id in ids:
            raise ValueError(f"Duplicate application id '{application.id}'.")
        if application.alias in aliases:
            raise ValueError(f"Duplicate application alias '{application.alias}'.")
        if not (REPO_ROOT / application.project).exists():
            raise ValueError(f"Configured project does not exist: {application.project}")
        if not application.bundles:
            raise ValueError(f"Application '{application.id}' must belong to at least one bundle.")
        if not set(application.bundles).issubset(bundle_names):
            raise ValueError(f"Application '{application.id}' references an unknown bundle.")
        ids.add(application.id)
        aliases.add(application.alias)
        configured_projects.add(application.project.as_posix())

    executable_projects = discover_executable_projects()
    if executable_projects != configured_projects:
        missing = sorted(executable_projects - configured_projects)
        extra = sorted(configured_projects - executable_projects)
        details: list[str] = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if extra:
            details.append("extra: " + ", ".join(extra))
        raise ValueError("release-config.json executable inventory mismatch: " + "; ".join(details))

    worker_aliases = [app.alias for app in config.applications if "worker" in app.bundles and app.alias != "nbn-worker"]
    if worker_aliases:
        raise ValueError("Worker bundle must only expose the nbn-worker alias.")


def ensure_clean_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def publish_application(
    application: Application,
    *,
    platform: str,
    rid: str,
    version: str,
    publish_root: Path,
) -> PublishedApplication:
    output_dir = publish_root / application.id
    ensure_clean_directory(output_dir)
    file_version = compute_file_version(version)
    run(
        [
            "dotnet",
            "publish",
            str(REPO_ROOT / application.project),
            "-c",
            "Release",
            "--disable-build-servers",
            "-r",
            rid,
            "--self-contained",
            "true",
            "/p:PublishSingleFile=true",
            "/p:PublishTrimmed=false",
            "/p:IncludeNativeLibrariesForSelfExtract=true",
            "/p:DebugType=None",
            "/p:DebugSymbols=false",
            f"/p:Version={version}",
            f"/p:AssemblyVersion={file_version}",
            f"/p:FileVersion={file_version}",
            f"/p:InformationalVersion={version}",
            "-o",
            str(output_dir),
        ]
    )

    executable_path = output_dir / application.binary_name(platform)
    if not executable_path.exists():
        raise RuntimeError(
            f"Publish output for '{application.id}' missing expected executable {executable_path.name}."
        )

    return PublishedApplication(application=application, output_dir=output_dir, executable_path=executable_path)


def bundle_applications(config: ReleaseConfig, bundle_name: str) -> tuple[Application, ...]:
    return tuple(app for app in config.applications if bundle_name in app.bundles)


def write_windows_wrapper(path: Path, target_relative_path: str) -> None:
    relative = str(PureWindowsPath("..") / PureWindowsPath(target_relative_path))
    text = "\n".join(
        [
            "@echo off",
            "setlocal",
            'set "SCRIPT_DIR=%~dp0"',
            f'"%SCRIPT_DIR%{relative}" %*',
            "",
        ]
    )
    path.write_text(text, encoding="utf-8", newline="\n")


def write_linux_wrapper(path: Path, target_relative_path: str) -> None:
    relative = (PurePosixPath("..") / PurePosixPath(target_relative_path)).as_posix()
    text = textwrap.dedent(
        f"""\
        #!/usr/bin/env bash
        set -euo pipefail

        SOURCE="${{BASH_SOURCE[0]}}"
        while [ -h "$SOURCE" ]; do
          DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
          TARGET="$(readlink "$SOURCE")"
          if [[ "$TARGET" != /* ]]; then
            SOURCE="$DIR/$TARGET"
          else
            SOURCE="$TARGET"
          fi
        done

        SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        exec "${{SCRIPT_DIR}}/{relative}" "$@"
        """
    )
    path.write_text(text, encoding="utf-8", newline="\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def write_layout_readme(bundle: Bundle, version: str, aliases: Iterable[str], destination: Path) -> None:
    alias_lines = "\n".join(f"  - {alias}" for alias in aliases)
    text = textwrap.dedent(
        f"""\
        {bundle.display_name} {version}

        Installed command aliases:
        {alias_lines}

        Workbench installed mode resolves managed runtime launches from runtime-manifest.json first and PATH aliases second.
        Portable worker archives do not modify PATH; run the alias from this extracted layout's bin directory.
        """
    )
    destination.write_text(text, encoding="utf-8", newline="\n")


def stage_bundle_layout(
    config: ReleaseConfig,
    *,
    bundle_name: str,
    bundle: Bundle,
    platform: str,
    version: str,
    published: dict[str, PublishedApplication],
    layout_root: Path,
) -> tuple[list[str], dict[str, str]]:
    ensure_clean_directory(layout_root)
    manifest_commands: dict[str, dict[str, str]] = {}

    aliases: list[str] = []
    for application in bundle_applications(config, bundle_name):
        published_app = published[application.id]
        destination = layout_root / application.install_subdir
        shutil.copytree(published_app.output_dir, destination, dirs_exist_ok=True)

        relative_binary_path = (
            PurePosixPath(application.install_subdir) / application.binary_name(platform)
        ).as_posix()
        manifest_commands[application.alias] = {"path": relative_binary_path}
        aliases.append(application.alias)

    if bundle.include_runbooks:
        runbooks_root = layout_root / "share" / "runbooks"
        runbooks_root.mkdir(parents=True, exist_ok=True)
        for runbook in config.runbooks:
            shutil.copy2(runbook, runbooks_root / runbook.name)

    write_layout_readme(bundle, version, aliases, layout_root / "README.txt")

    bin_root = layout_root / "bin"
    bin_root.mkdir(parents=True, exist_ok=True)
    for alias, command in manifest_commands.items():
        if platform == "windows":
            write_windows_wrapper(bin_root / f"{alias}.cmd", command["path"])
        else:
            write_linux_wrapper(bin_root / alias, command["path"])

    manifest_path = layout_root / "runtime-manifest.json"
    manifest_path.write_text(
        json.dumps({"commands": manifest_commands}, indent=2) + "\n",
        encoding="utf-8",
    )

    return aliases, {alias: command["path"] for alias, command in manifest_commands.items()}


def build_deb_control(bundle: Bundle, version: str) -> str:
    package_name = bundle.artifact_prefix
    description = f"{bundle.display_name} distributed runtime release."
    return textwrap.dedent(
        f"""\
        Package: {package_name}
        Version: {version}
        Section: utils
        Priority: optional
        Architecture: amd64
        Maintainer: NBN Contributors <opensource@nbn.invalid>
        Description: {description}
         Manual tag-triggered NBN release bundle with manifest-driven installed launches.
        """
    )


def build_run_uninstall_script(bundle: Bundle, aliases: Iterable[str]) -> str:
    alias_lines = "\n".join(f'ALIASES+=("{alias}")' for alias in aliases)
    return textwrap.dedent(
        f"""\
        #!/usr/bin/env bash
        set -euo pipefail

        BIN_DIR="${{1:-/usr/local/bin}}"
        INSTALL_ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
        declare -a ALIASES=()
        {alias_lines}

        for alias in "${{ALIASES[@]}}"; do
          link_path="${{BIN_DIR}}/${{alias}}"
          if [ -L "$link_path" ] && [ "$(readlink -f "$link_path")" = "${{INSTALL_ROOT}}/bin/${{alias}}" ]; then
            rm -f "$link_path"
          fi
        done

        rm -rf "$INSTALL_ROOT"
        echo "Removed {bundle.display_name} from $INSTALL_ROOT"
        """
    )


def create_linux_deb(
    *,
    bundle: Bundle,
    version: str,
    layout_root: Path,
    aliases: list[str],
    artifacts_root: Path,
) -> Path:
    artifact_path = artifacts_root / f"{bundle.artifact_prefix}_{version}_amd64.deb"
    with tempfile.TemporaryDirectory(prefix=f"{bundle.name}-deb-") as temp_dir:
        stage_root = Path(temp_dir) / "package"
        install_root = stage_root / bundle.linux_install_root.lstrip("/")
        install_root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(layout_root, install_root, dirs_exist_ok=True)

        bin_root = stage_root / "usr" / "bin"
        bin_root.mkdir(parents=True, exist_ok=True)
        for alias in aliases:
            link_target = PurePosixPath(bundle.linux_install_root) / "bin" / alias
            link_path = bin_root / alias
            if link_path.exists() or link_path.is_symlink():
                link_path.unlink()
            os.symlink(link_target.as_posix(), link_path)

        debian_root = stage_root / "DEBIAN"
        debian_root.mkdir(parents=True, exist_ok=True)
        (debian_root / "control").write_text(build_deb_control(bundle, version), encoding="utf-8")

        run(
            [
                "dpkg-deb",
                "--build",
                "--root-owner-group",
                str(stage_root),
                str(artifact_path),
            ]
        )

    return artifact_path


def create_linux_run_installer(
    *,
    bundle: Bundle,
    version: str,
    layout_root: Path,
    aliases: list[str],
    artifacts_root: Path,
) -> Path:
    artifact_path = artifacts_root / f"{bundle.artifact_prefix}-{version}-linux-x64-installer.run"
    template_path = REPO_ROOT / "tools" / "dist" / "packaging" / "linux" / "install-template.sh"
    template = template_path.read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory(prefix=f"{bundle.name}-run-") as temp_dir:
        payload_root = Path(temp_dir) / "payload"
        shutil.copytree(layout_root, payload_root, dirs_exist_ok=True)
        uninstall_path = payload_root / "uninstall.sh"
        uninstall_path.write_text(build_run_uninstall_script(bundle, aliases), encoding="utf-8", newline="\n")
        uninstall_path.chmod(uninstall_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        archive_path = Path(temp_dir) / "payload.tar.gz"
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(payload_root, arcname="payload")

        alias_literal = " ".join(f'"{alias}"' for alias in aliases)
        installer_text = (
            template.replace("__DISPLAY_NAME__", bundle.display_name)
            .replace("__DEFAULT_INSTALL_ROOT__", bundle.linux_install_root)
            .replace("__ALIASES__", alias_literal)
        )
        artifact_path.write_bytes(installer_text.encode("utf-8") + archive_path.read_bytes())
        artifact_path.chmod(artifact_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return artifact_path


def create_zip_archive(source_root: Path, archive_path: Path, archive_root_name: str) -> None:
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source_root.rglob("*")):
            relative = path.relative_to(source_root)
            archive_name = Path(archive_root_name) / relative
            archive.write(path, archive_name.as_posix())


def create_tar_archive(source_root: Path, archive_path: Path, archive_root_name: str) -> None:
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source_root, arcname=archive_root_name)


def create_worker_portable_archive(
    *,
    platform: str,
    version: str,
    layout_root: Path,
    bundle: Bundle,
    artifacts_root: Path,
) -> Path:
    root_name = f"{bundle.artifact_prefix}-{version}-{ 'win-x64' if platform == 'windows' else 'linux-x64' }-portable"
    if platform == "windows":
        archive_path = artifacts_root / f"{root_name}.zip"
        create_zip_archive(layout_root, archive_path, root_name)
    else:
        archive_path = artifacts_root / f"{root_name}.tar.gz"
        create_tar_archive(layout_root, archive_path, root_name)
    return archive_path


def resolve_iscc_path(value: str | None) -> str:
    if value:
        return value
    if os.name == "nt":
        program_files_x86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
        candidate = Path(program_files_x86) / "Inno Setup 6" / "ISCC.exe"
        if candidate.exists():
            return str(candidate)
    return "ISCC.exe"


def create_windows_installer(
    *,
    bundle: Bundle,
    version: str,
    layout_root: Path,
    artifacts_root: Path,
    iscc_path: str | None,
) -> Path:
    output_base_name = f"{bundle.artifact_prefix}-{version}-win-x64-setup"
    template_path = REPO_ROOT / "tools" / "dist" / "packaging" / "windows" / f"{bundle.artifact_prefix}.iss"
    run(
        [
            resolve_iscc_path(iscc_path),
            str(template_path),
            f"/DSourceDir={layout_root}",
            f"/DOutputDir={artifacts_root}",
            f"/DAppVersion={version}",
            f"/DOutputBaseFilename={output_base_name}",
            f"/DInstallDirName={bundle.windows_install_dir_name}",
        ]
    )
    artifact_path = artifacts_root / f"{output_base_name}.exe"
    if not artifact_path.exists():
        raise RuntimeError(f"Expected Inno Setup output not found: {artifact_path}")
    return artifact_path


def build_platform_artifacts(
    *,
    config: ReleaseConfig,
    platform: str,
    version: str,
    output_root: Path,
    bundle_names: tuple[str, ...],
    iscc_path: str | None,
) -> list[Path]:
    rid = "win-x64" if platform == "windows" else "linux-x64"
    publish_root = output_root / "publish" / platform
    artifacts_root = output_root / "artifacts" / platform
    ensure_clean_directory(publish_root)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    applications: dict[str, Application] = {}
    for bundle_name in bundle_names:
        for application in bundle_applications(config, bundle_name):
            applications[application.id] = application

    published = {
        app_id: publish_application(
            application,
            platform=platform,
            rid=rid,
            version=version,
            publish_root=publish_root,
        )
        for app_id, application in applications.items()
    }

    artifact_paths: list[Path] = []
    for bundle_name in bundle_names:
        bundle = config.bundles[bundle_name]
        layout_root = output_root / "layout" / platform / bundle_name
        aliases, _ = stage_bundle_layout(
            config,
            bundle_name=bundle_name,
            bundle=bundle,
            platform=platform,
            version=version,
            published=published,
            layout_root=layout_root,
        )
        if platform == "windows":
            artifact_paths.append(
                create_windows_installer(
                    bundle=bundle,
                    version=version,
                    layout_root=layout_root,
                    artifacts_root=artifacts_root,
                    iscc_path=iscc_path,
                )
            )
        else:
            artifact_paths.append(
                create_linux_deb(
                    bundle=bundle,
                    version=version,
                    layout_root=layout_root,
                    aliases=aliases,
                    artifacts_root=artifacts_root,
                )
            )
            artifact_paths.append(
                create_linux_run_installer(
                    bundle=bundle,
                    version=version,
                    layout_root=layout_root,
                    aliases=aliases,
                    artifacts_root=artifacts_root,
                )
            )

        if bundle_name == "worker":
            artifact_paths.append(
                create_worker_portable_archive(
                    platform=platform,
                    version=version,
                    layout_root=layout_root,
                    bundle=bundle,
                    artifacts_root=artifacts_root,
                )
            )

    return artifact_paths


def command_print_version(_: argparse.Namespace) -> int:
    config = load_release_config()
    validate_config(config)
    print(resolve_next_version(config))
    return 0


def command_validate_tag(args: argparse.Namespace) -> int:
    config = load_release_config()
    validate_config(config)
    print(validate_release_tag(config, args.tag))
    return 0


def command_validate_config(_: argparse.Namespace) -> int:
    config = load_release_config()
    validate_config(config)
    print("release-config.json is valid.")
    return 0


def command_build_platform(args: argparse.Namespace) -> int:
    config = load_release_config()
    validate_config(config)
    version = args.version.strip()
    parse_version_text(version)
    output_root = (REPO_ROOT / args.output_root).resolve()
    bundle_names = tuple(args.bundle) if args.bundle else tuple(config.bundles.keys())
    artifact_paths = build_platform_artifacts(
        config=config,
        platform=args.platform,
        version=version,
        output_root=output_root,
        bundle_names=bundle_names,
        iscc_path=args.iscc,
    )
    for artifact_path in artifact_paths:
        print(artifact_path.relative_to(REPO_ROOT).as_posix())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build NBN release layouts and artifacts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    print_version = subparsers.add_parser("print-version", help="Compute the next release version from release/version.json and git tags.")
    print_version.set_defaults(func=command_print_version)

    validate_tag = subparsers.add_parser("validate-tag", help="Validate a pushed git tag against release/version.json.")
    validate_tag.add_argument("--tag", required=True, help="Tag to validate, for example v2.0.0.")
    validate_tag.set_defaults(func=command_validate_tag)

    validate_config_parser = subparsers.add_parser("validate-config", help="Validate release-config.json against the repo inventory.")
    validate_config_parser.set_defaults(func=command_validate_config)

    build_platform = subparsers.add_parser("build-platform", help="Publish and package release artifacts for one platform.")
    build_platform.add_argument("--platform", choices=("windows", "linux"), required=True)
    build_platform.add_argument("--version", required=True, help="Release version, for example 2.0.0.")
    build_platform.add_argument(
        "--output-root",
        required=True,
        help="Output root relative to the repo root.",
    )
    build_platform.add_argument(
        "--bundle",
        action="append",
        choices=("suite", "worker"),
        help="Bundle(s) to package. Defaults to all bundles.",
    )
    build_platform.add_argument(
        "--iscc",
        help="Optional path to ISCC.exe for Windows packaging.",
    )
    build_platform.set_defaults(func=command_build_platform)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
