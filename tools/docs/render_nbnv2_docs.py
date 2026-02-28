#!/usr/bin/env python3
"""Render docs/INDEX.md include markers into docs/NBNv2.md deterministically."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

INCLUDE_LINE_RE = re.compile(r'^\s*<!--\s*NBN:INCLUDE\s+path="([^"]+)"\s*-->\s*$')
MANIFEST_ENTRY_RE = re.compile(r"^\d+\.\s+(.+?)\s*$")
ASSEMBLY_HEADER = "## Assembly order (INDEX include sequence)"
MOJIBAKE_RE = re.compile(r"(Ã¢â‚¬|â€¢|â€œ|â€[\x9d˜™“”¦]|Ã—|âˆˆ|â†\x90)")


def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def assert_no_mojibake(text: str, path_label: str) -> None:
    for line_number, line in enumerate(text.split("\n"), start=1):
        if MOJIBAKE_RE.search(line):
            fail(
                f"{path_label}:{line_number} contains mojibake text "
                "(for example â€¢/â€œ/â€™). Fix source encoding before rendering."
            )


def read_utf8(path: Path) -> str:
    try:
        return normalize_newlines(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"Missing file: {path}")
    except UnicodeDecodeError as exc:
        fail(f"File is not UTF-8 decodable: {path} ({exc})")
    except OSError as exc:
        fail(f"Failed to read file: {path} ({exc})")


def normalize_rel_token(token: str, source_label: str, line_number: int) -> str:
    normalized = token.strip().replace("\\", "/")
    if not normalized:
        fail(f"{source_label}:{line_number} has an empty include path.")
    if normalized.startswith("/") or normalized.startswith("\\"):
        fail(f"{source_label}:{line_number} include path must be repository-root relative: {token}")
    if re.match(r"^[A-Za-z]:", normalized):
        fail(f"{source_label}:{line_number} include path must not be drive-absolute: {token}")
    if normalized.startswith("//"):
        fail(f"{source_label}:{line_number} include path must not be UNC/URL-like: {token}")
    return normalized


def assert_within_repo(repo_root: Path, relative_path: str, source_label: str, line_number: int) -> Path:
    candidate = (repo_root / relative_path).resolve()
    repo_abs = repo_root.resolve()
    try:
        candidate.relative_to(repo_abs)
    except ValueError:
        fail(
            f"{source_label}:{line_number} include path escapes repository root: "
            f"{relative_path} -> {candidate}"
        )
    if not candidate.is_file():
        fail(f"{source_label}:{line_number} include target does not exist: {relative_path}")
    return candidate


def parse_index_includes(index_text: str) -> list[tuple[int, str]]:
    includes: list[tuple[int, str]] = []
    for line_number, line in enumerate(index_text.split("\n"), start=1):
        stripped = line.strip()
        if "NBN:INCLUDE" not in line:
            continue
        if not stripped.startswith("<!--"):
            # Allow literal marker examples embedded in markdown text/code spans.
            continue
        match = INCLUDE_LINE_RE.fullmatch(line)
        if match is None:
            fail(
                f"docs/INDEX.md:{line_number} has an invalid include marker. "
                "Expected: <!-- NBN:INCLUDE path=\"...\" -->"
            )
        relative_path = normalize_rel_token(match.group(1), "docs/INDEX.md", line_number)
        includes.append((line_number, relative_path))

    if not includes:
        fail("docs/INDEX.md contains no include markers.")
    return includes


def parse_manifest_order(manifest_text: str) -> list[str]:
    lines = manifest_text.split("\n")
    section_start: int | None = None
    for idx, line in enumerate(lines, start=1):
        if line.strip() == ASSEMBLY_HEADER:
            section_start = idx + 1
            break

    if section_start is None:
        fail(
            "docs/manifest/NBNv2-DocumentMap.md is missing the "
            f"'{ASSEMBLY_HEADER}' section."
        )

    order: list[str] = []
    for idx in range(section_start, len(lines) + 1):
        line = lines[idx - 1]
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        if not stripped:
            continue
        match = MANIFEST_ENTRY_RE.fullmatch(stripped)
        if match is None:
            fail(
                f"docs/manifest/NBNv2-DocumentMap.md:{idx} is not a valid "
                "numbered assembly-order entry."
            )
        order.append(normalize_rel_token(match.group(1), "docs/manifest/NBNv2-DocumentMap.md", idx))

    if not order:
        fail("docs/manifest/NBNv2-DocumentMap.md assembly order is empty.")
    return order


def enforce_order_parity(index_includes: list[tuple[int, str]], manifest_order: list[str]) -> None:
    index_paths = [path for _, path in index_includes]
    if len(index_paths) != len(manifest_order):
        fail(
            "Include count mismatch between docs/INDEX.md and "
            "docs/manifest/NBNv2-DocumentMap.md "
            f"(index={len(index_paths)}, manifest={len(manifest_order)})."
        )

    for idx, (index_path, manifest_path) in enumerate(zip(index_paths, manifest_order), start=1):
        if index_path != manifest_path:
            fail(
                "Include order mismatch between docs/INDEX.md and "
                "docs/manifest/NBNv2-DocumentMap.md "
                f"at position {idx}: index='{index_path}', manifest='{manifest_path}'."
            )


def render(index_includes: list[tuple[int, str]], include_contents: dict[int, str]) -> str:
    # INDEX acts as an assembly map; output emits only included payload content.
    parts: list[str] = []
    for line_number, _ in index_includes:
        parts.append(normalize_newlines(include_contents[line_number]).rstrip("\n"))

    rendered = "\n\n".join(parts).rstrip("\n") + "\n"
    return normalize_newlines(rendered)


def resolve_path(repo_root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_repo_root = script_dir.parent.parent.resolve()

    parser = argparse.ArgumentParser(
        description="Render docs/INDEX.md include markers into docs/NBNv2.md."
    )
    parser.add_argument("--check", action="store_true", help="Fail if docs/NBNv2.md is stale.")
    parser.add_argument("--repo-root", default=str(default_repo_root), help="Repository root path.")
    parser.add_argument("--index", default="docs/INDEX.md", help="INDEX template path.")
    parser.add_argument("--manifest", default="docs/manifest/NBNv2-DocumentMap.md", help="Manifest path.")
    parser.add_argument("--output", default="docs/NBNv2.md", help="Rendered output path.")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    index_path = resolve_path(repo_root, args.index)
    manifest_path = resolve_path(repo_root, args.manifest)
    output_path = resolve_path(repo_root, args.output)

    index_text = read_utf8(index_path)
    manifest_text = read_utf8(manifest_path)
    assert_no_mojibake(index_text, index_path.as_posix())
    assert_no_mojibake(manifest_text, manifest_path.as_posix())

    index_includes = parse_index_includes(index_text)
    manifest_order = parse_manifest_order(manifest_text)
    enforce_order_parity(index_includes, manifest_order)

    include_contents: dict[int, str] = {}
    for line_number, relative_path in index_includes:
        target_path = assert_within_repo(repo_root, relative_path, "docs/INDEX.md", line_number)
        include_text = read_utf8(target_path)
        assert_no_mojibake(include_text, target_path.as_posix())
        include_contents[line_number] = include_text

    rendered = render(index_includes, include_contents)

    if args.check:
        existing = ""
        if output_path.exists():
            existing = read_utf8(output_path)
        if existing != rendered:
            fail(
                "docs/NBNv2.md is stale. Re-render with:\n"
                "  - Windows: pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1\n"
                "  - Linux/macOS: bash tools/docs/render-nbnv2-docs.sh"
            )
        print("docs/NBNv2.md is up to date.")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)

    print(f"Rendered {output_path.relative_to(repo_root).as_posix()} from {len(index_includes)} include markers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
