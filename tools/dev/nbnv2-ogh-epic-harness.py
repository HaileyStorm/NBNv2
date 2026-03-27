#!/usr/bin/env python3
"""Temporary Codex harness for completing the NBNv2-ogh epic.

The harness uses a fixed issue schedule with at most three concurrent worker
slots. Each worker runs in an isolated ignored clone under
`.artifacts-temp/epic-harness/slots`, while the main repository remains the
single integration point for Beads updates, commits, and pushes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_ISSUE = "NBNv2-ogh"
MAX_SLOTS = 3
PROGRESS_RE = re.compile(r"(?:About\s+)?(\d+)% complete\b", re.IGNORECASE)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


REPO_ROOT = repo_root()
RUN_ROOT = REPO_ROOT / ".artifacts-temp" / "epic-harness"
SLOTS_ROOT = RUN_ROOT / "slots"
LOGS_ROOT = RUN_ROOT / "logs"
STATE_PATH = RUN_ROOT / "state.json"
SCHEMA_PATH = RUN_ROOT / "worker-output-schema.json"
SESSION_ROOT = Path.home() / ".codex" / "sessions"


SCHEDULE: list[list[str]] = [
    ["NBNv2-ogh.1"],
    ["NBNv2-ogh.2"],
    ["NBNv2-ogh.3", "NBNv2-ogh.4", "NBNv2-ogh.5"],
    ["NBNv2-ogh.6", "NBNv2-ogh.10", "NBNv2-ogh.14"],
    ["NBNv2-ogh.7", "NBNv2-ogh.8"],
    ["NBNv2-ogh.9", "NBNv2-ogh.11", "NBNv2-ogh.15"],
    ["NBNv2-ogh.16"],
    ["NBNv2-ogh.12", "NBNv2-ogh.13"],
    ["NBNv2-ogh.17"],
    ["NBNv2-ogh.18"],
    ["NBNv2-ogh.19"],
]


EXPECTED_BLOCKS: dict[str, list[str]] = {
    "NBNv2-ogh.1": [],
    "NBNv2-ogh.2": ["NBNv2-ogh.1"],
    "NBNv2-ogh.3": ["NBNv2-ogh.1", "NBNv2-ogh.2"],
    "NBNv2-ogh.4": ["NBNv2-ogh.1", "NBNv2-ogh.2"],
    "NBNv2-ogh.5": ["NBNv2-ogh.1", "NBNv2-ogh.2"],
    "NBNv2-ogh.6": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5"],
    "NBNv2-ogh.7": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3"],
    "NBNv2-ogh.8": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"],
    "NBNv2-ogh.9": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5", "NBNv2-ogh.7"],
    "NBNv2-ogh.10": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"],
    "NBNv2-ogh.11": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5", "NBNv2-ogh.10"],
    "NBNv2-ogh.12": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5", "NBNv2-ogh.10", "NBNv2-ogh.11"],
    "NBNv2-ogh.13": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5", "NBNv2-ogh.6", "NBNv2-ogh.9"],
    "NBNv2-ogh.14": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"],
    "NBNv2-ogh.15": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.14"],
    "NBNv2-ogh.16": ["NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.14"],
    "NBNv2-ogh.17": ["NBNv2-ogh.1", "NBNv2-ogh.12", "NBNv2-ogh.14"],
    "NBNv2-ogh.18": [
        "NBNv2-ogh.1",
        "NBNv2-ogh.2",
        "NBNv2-ogh.3",
        "NBNv2-ogh.4",
        "NBNv2-ogh.5",
        "NBNv2-ogh.6",
        "NBNv2-ogh.7",
        "NBNv2-ogh.8",
        "NBNv2-ogh.9",
        "NBNv2-ogh.10",
        "NBNv2-ogh.11",
        "NBNv2-ogh.12",
        "NBNv2-ogh.13",
        "NBNv2-ogh.14",
        "NBNv2-ogh.15",
        "NBNv2-ogh.16",
        "NBNv2-ogh.17",
    ],
    "NBNv2-ogh.19": [
        "NBNv2-ogh.1",
        "NBNv2-ogh.2",
        "NBNv2-ogh.3",
        "NBNv2-ogh.4",
        "NBNv2-ogh.5",
        "NBNv2-ogh.6",
        "NBNv2-ogh.7",
        "NBNv2-ogh.8",
        "NBNv2-ogh.9",
        "NBNv2-ogh.10",
        "NBNv2-ogh.11",
        "NBNv2-ogh.12",
        "NBNv2-ogh.13",
        "NBNv2-ogh.14",
        "NBNv2-ogh.15",
        "NBNv2-ogh.16",
        "NBNv2-ogh.17",
        "NBNv2-ogh.18",
    ],
}


RECOMMENDED_VALIDATION: dict[str, list[str]] = {
    "NBNv2-ogh.1": [
        "dotnet build NBNv2.sln -c Release --disable-build-servers",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.2": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Format",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Shared",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Proto.ProtoCompatibilityTests",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.3": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.RuntimeArtifacts",
        "dotnet test tests/Nbn.Tests.CrossProcessArtifactWorker/Nbn.Tests.CrossProcessArtifactWorker.csproj -c Release --disable-build-servers",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.4": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Brain",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Observability",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.5": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.SettingsMonitor",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Shared.SettingsMonitorReporterTests",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.6": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.HiveMind",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.7": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.RegionHost",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.8": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Integration",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.9": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.WorkerNode",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.10": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Reproduction",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.11": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Speciation",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.12": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Tools.Evolution",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.13": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Tools.PerfProbe",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.14": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.15": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench.Designer",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.16": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench.Viz",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.17": [
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Workbench",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.18": [
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
    "NBNv2-ogh.19": [
        "bash tools/docs/render-nbnv2-docs.sh --check",
        "dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Proto.ProtoCompatibilityTests",
        "dotnet build NBNv2.sln -c Release --disable-build-servers",
        "dotnet test NBNv2.sln -c Release --disable-build-servers",
    ],
}


class HarnessError(RuntimeError):
    pass


@dataclass
class IssueInfo:
    issue_id: str
    title: str
    status: str
    description: str
    design: str
    acceptance_criteria: str
    block_dependencies: list[str]


@dataclass
class WorkerResult:
    issue_id: str
    outcome: str
    commit_sha: str | None
    summary: str
    tests_run: list[str]
    docs_changed: list[str]
    blockers: list[str]
    full_suite_passed: bool
    root_close_candidate: bool
    notes: str


@dataclass
class WorkerRun:
    issue: IssueInfo
    slot_name: str
    slot_dir: Path
    branch_name: str
    base_head: str
    stdout_log_path: Path
    result_path: Path
    prompt_path: Path
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    thread_id: str | None = None
    session_file: Path | None = None
    session_offset: int = 0
    progress_percent: int | None = None
    progress_message: str = "starting"
    last_message: str = ""
    exit_code: int | None = None
    result: WorkerResult | None = None
    recovery_attempted: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class State:
    baseline_commit: str
    started_utc: str
    integrated: list[dict[str, Any]]
    has_docs_changes: bool
    run_root: str


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_command(
    args: list[str],
    *,
    cwd: Path = REPO_ROOT,
    input_text: str | None = None,
    capture_output: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        input=input_text,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if check and completed.returncode != 0:
        stderr = completed.stderr.strip() if completed.stderr else ""
        stdout = completed.stdout.strip() if completed.stdout else ""
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise HarnessError(f"Command failed in {cwd}: {' '.join(args)}\n{detail}")
    return completed


def git_head(cwd: Path = REPO_ROOT) -> str:
    return run_command(["git", "rev-parse", "HEAD"], cwd=cwd).stdout.strip()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def ensure_clean_main_repo() -> None:
    status = run_command(["git", "status", "--porcelain"], cwd=REPO_ROOT).stdout.strip()
    if status:
        raise HarnessError(
            "The main repo worktree is dirty. Commit or stash changes before running the epic harness.\n"
            f"{status}"
        )


def ensure_runtime_layout() -> None:
    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    SLOTS_ROOT.mkdir(parents=True, exist_ok=True)
    LOGS_ROOT.mkdir(parents=True, exist_ok=True)
    write_json(
        SCHEMA_PATH,
        {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "issue_id",
                "outcome",
                "commit_sha",
                "summary",
                "tests_run",
                "docs_changed",
                "blockers",
                "full_suite_passed",
                "root_close_candidate",
                "notes",
            ],
            "properties": {
                "issue_id": {"type": "string"},
                "outcome": {"type": "string", "enum": ["ready", "blocked", "failed"]},
                "commit_sha": {"type": ["string", "null"]},
                "summary": {"type": "string"},
                "tests_run": {"type": "array", "items": {"type": "string"}},
                "docs_changed": {"type": "array", "items": {"type": "string"}},
                "blockers": {"type": "array", "items": {"type": "string"}},
                "full_suite_passed": {"type": "boolean"},
                "root_close_candidate": {"type": "boolean"},
                "notes": {"type": "string"},
            },
        },
    )


def ensure_root_working() -> bool:
    return False


def remove_root_working(created_here: bool) -> None:
    del created_here


def load_state(baseline_commit: str) -> State:
    if not STATE_PATH.exists():
        state = State(
            baseline_commit=baseline_commit,
            started_utc=utc_now(),
            integrated=[],
            has_docs_changes=False,
            run_root=str(RUN_ROOT),
        )
        save_state(state)
        return state

    payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return State(
        baseline_commit=payload.get("baseline_commit", baseline_commit),
        started_utc=payload.get("started_utc") or payload.get("saved_at") or utc_now(),
        integrated=list(payload.get("integrated", [])),
        has_docs_changes=bool(payload.get("has_docs_changes", False)),
        run_root=payload.get("run_root", str(RUN_ROOT)),
    )


def save_state(state: State) -> None:
    write_json(
        STATE_PATH,
        {
            "baseline_commit": state.baseline_commit,
            "started_utc": state.started_utc,
            "integrated": state.integrated,
            "has_docs_changes": state.has_docs_changes,
            "run_root": state.run_root,
        },
    )


def fetch_issue(issue_id: str) -> IssueInfo:
    payload = json.loads(run_command(["bd", "show", issue_id, "--json"]).stdout)[0]
    blocks = sorted(
        item["id"]
        for item in payload.get("dependencies", [])
        if item.get("dependency_type") == "blocks" and item["id"].startswith(f"{ROOT_ISSUE}.")
    )
    return IssueInfo(
        issue_id=payload["id"],
        title=payload["title"],
        status=payload["status"],
        description=payload.get("description", "").strip(),
        design=payload.get("design", "").strip(),
        acceptance_criteria=payload.get("acceptance_criteria", "").strip(),
        block_dependencies=blocks,
    )


def load_issue_map() -> dict[str, IssueInfo]:
    issue_ids = sorted({issue_id for batch in SCHEDULE for issue_id in batch})
    return {issue_id: fetch_issue(issue_id) for issue_id in issue_ids}


def list_child_statuses() -> dict[str, str]:
    payload = json.loads(
        run_command(["bd", "list", "--parent", ROOT_ISSUE, "--all", "--json", "--limit", "0"]).stdout
    )
    return {item["id"]: item["status"] for item in payload}


def verify_schedule(issue_map: dict[str, IssueInfo]) -> None:
    if sorted(issue_map) != sorted(EXPECTED_BLOCKS):
        raise HarnessError("The current epic children do not match the harness schedule.")
    completed_before: set[str] = set()
    for batch in SCHEDULE:
        if len(batch) > MAX_SLOTS:
            raise HarnessError(f"Batch exceeds {MAX_SLOTS} slots: {batch}")
        for issue_id in batch:
            actual_blocks = issue_map[issue_id].block_dependencies
            expected_blocks = EXPECTED_BLOCKS[issue_id]
            if sorted(actual_blocks) != sorted(expected_blocks):
                raise HarnessError(
                    f"Dependency drift for {issue_id}.\n"
                    f"Expected: {expected_blocks}\n"
                    f"Actual:   {actual_blocks}"
                )
            missing = [item for item in expected_blocks if item not in completed_before]
            if missing:
                raise HarnessError(
                    f"Schedule starts {issue_id} before prior batches complete: {missing}"
                )
        completed_before.update(batch)


def print_plan(issue_map: dict[str, IssueInfo], baseline_commit: str) -> None:
    print(f"root issue: {ROOT_ISSUE}")
    print(f"baseline commit: {baseline_commit}")
    print("fixed schedule:")
    for index, batch in enumerate(SCHEDULE, start=1):
        rendered = ", ".join(f"{issue_id} ({issue_map[issue_id].status})" for issue_id in batch)
        print(f"  {index:02d}. {rendered}")


def prepare_slot_clone(slot_name: str, issue: IssueInfo, base_head: str) -> tuple[Path, str]:
    slot_dir = SLOTS_ROOT / slot_name
    if slot_dir.exists():
        shutil.rmtree(slot_dir)
    run_command(["git", "clone", "--shared", "--no-tags", str(REPO_ROOT), str(slot_dir)])
    branch_name = f"harness/{issue.issue_id}"
    run_command(["git", "checkout", "-B", branch_name, base_head], cwd=slot_dir)
    exclude_path = slot_dir / ".git" / "info" / "exclude"
    existing = exclude_path.read_text(encoding="utf-8") if exclude_path.exists() else ""
    if ".working" not in existing.splitlines():
        with exclude_path.open("a", encoding="utf-8") as handle:
            if existing and not existing.endswith("\n"):
                handle.write("\n")
            handle.write(".working\n")
    (slot_dir / ".working").write_text(
        f"Reserved by the NBNv2-ogh epic harness for {issue.issue_id}.\n",
        encoding="utf-8",
    )
    return slot_dir, branch_name


def worker_prompt(issue: IssueInfo, siblings: list[str], *, recovery_reason: str | None = None) -> str:
    validation_block = "\n".join(f"- `{command}`" for command in RECOMMENDED_VALIDATION[issue.issue_id])
    sibling_text = ", ".join(sorted(item for item in siblings if item != issue.issue_id)) or "none"
    root_close_line = (
        "Set `root_close_candidate` to `true` only for `NBNv2-ogh.19` when your issue is complete and has no blockers."
        if issue.issue_id == "NBNv2-ogh.19"
        else "Set `root_close_candidate` to `false`."
    )
    recovery_block = ""
    if recovery_reason:
        recovery_block = textwrap.dedent(
            f"""

            Recovery context:
            - The previous worker attempt did not finish cleanly.
            - Reason: {recovery_reason}
            - Inspect the existing clone state and the saved logs in `.artifacts-temp/epic-harness/logs`.
            """
        ).strip()

    return textwrap.dedent(
        f"""
        You are the isolated worker for Beads issue `{issue.issue_id}` in a temporary local clone.

        Scope rules:
        - Complete only `{issue.issue_id}`.
        - Parallel siblings in this fixed batch: {sibling_text}.
        - Do not edit files owned by sibling issues unless this issue explicitly lists them.
        - Stay inside this clone; `.working` is already reserved for you here.
        - Do not edit `.beads/`.
        - Do not run `bd close`, `bd sync`, or any other Beads mutation command.
        - Do not run `git pull`, `git push`, `git rebase`, `git merge`, or any remote-mutating git command.
        - You may use repo-required subagents inside this clone when the repo AGENTS rules call for them.
        - Leave exactly one local commit only when the issue is fully ready. If blocked or failed, do not create a commit.

        The harness owns:
        - the main repo checkout
        - serial integration/cherry-pick
        - Beads comments and closure
        - final push

        NBN invariants to preserve:
        - input region stays `0`; output region stays `31`
        - illegal IO-region axons stay forbidden
        - tick semantics stay compute-then-deliver with tick N visible at compute N+1
        - snapshots remain at tick boundaries
        - reproduction protections, artifact `store_uri` behavior, and Workbench headless dispatcher behavior stay intact
        - do not hand-edit generated outputs under `bin/`, `obj/`, generated proto output, or rendered docs unless their source changed

        Validation expectations for this issue:
        {validation_block}

        Additional validation rules:
        - If you touch protocol-sensitive shared/proto files, include the proto compatibility test.
        - If you change docs sources, include `bash tools/docs/render-nbnv2-docs.sh --check`.
        - Do not report `ready` unless the final release full-suite test passed in this clone.

        Required local commit subject:
        - `{issue.issue_id}: {issue.title}`

        Issue packet:
        Title:
        {issue.title}

        Description:
        {issue.description}

        Design:
        {issue.design}

        Acceptance Criteria:
        {issue.acceptance_criteria}

        Output contract:
        - Return JSON only, matching the provided schema.
        - Use `outcome="ready"` only when the issue is fully implemented, validated, and committed locally.
        - For `ready`, set `commit_sha` to the local commit SHA and `full_suite_passed` to `true`.
        - For `blocked` or `failed`, set `commit_sha` to `null` and list specific blockers.
        - `tests_run` should list the exact validation commands you ran.
        - `docs_changed` should list repo-relative `docs/*` or `*/Design.md` paths you changed.
        - {root_close_line}
        - Keep `summary` and `notes` concise and concrete.
        {recovery_block}
        """
    ).strip() + "\n"


def read_output_json(path: Path) -> WorkerResult:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return WorkerResult(
        issue_id=payload["issue_id"],
        outcome=payload["outcome"],
        commit_sha=payload["commit_sha"],
        summary=payload["summary"],
        tests_run=list(payload["tests_run"]),
        docs_changed=list(payload["docs_changed"]),
        blockers=list(payload["blockers"]),
        full_suite_passed=bool(payload["full_suite_passed"]),
        root_close_candidate=bool(payload["root_close_candidate"]),
        notes=payload["notes"],
    )


def start_exec_worker(
    slot_name: str,
    issue: IssueInfo,
    base_head: str,
    siblings: list[str],
    *,
    recovery_reason: str | None = None,
    reuse_existing: bool = False,
) -> WorkerRun:
    if reuse_existing:
        slot_dir = SLOTS_ROOT / slot_name
        if not slot_dir.exists():
            raise HarnessError(f"Cannot recover {issue.issue_id}; slot clone {slot_dir} is missing.")
        branch_name = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=slot_dir).stdout.strip()
    else:
        slot_dir, branch_name = prepare_slot_clone(slot_name, issue, base_head)
    prompt = worker_prompt(issue, siblings, recovery_reason=recovery_reason)
    suffix = "retry" if recovery_reason else "run"
    stdout_log_path = LOGS_ROOT / f"{issue.issue_id}.{slot_name}.{suffix}.stdout.jsonl"
    result_path = LOGS_ROOT / f"{issue.issue_id}.{slot_name}.{suffix}.result.json"
    prompt_path = LOGS_ROOT / f"{issue.issue_id}.{slot_name}.{suffix}.prompt.txt"
    prompt_path.write_text(prompt, encoding="utf-8")

    args = [
        "codex",
        "exec",
        "--json",
        "--color",
        "never",
        "--dangerously-bypass-approvals-and-sandbox",
        "-C",
        str(slot_dir),
        "--output-schema",
        str(SCHEMA_PATH),
        "--output-last-message",
        str(result_path),
        "-",
    ]
    process = subprocess.Popen(
        args,
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdin is not None
    process.stdin.write(prompt)
    process.stdin.close()

    worker = WorkerRun(
        issue=issue,
        slot_name=slot_name,
        slot_dir=slot_dir,
        branch_name=branch_name,
        base_head=base_head,
        stdout_log_path=stdout_log_path,
        result_path=result_path,
        prompt_path=prompt_path,
        process=process,
        reader_thread=threading.Thread(target=lambda: None),
    )

    def reader_loop() -> None:
        assert process.stdout is not None
        with stdout_log_path.open("w", encoding="utf-8") as handle:
            for line in process.stdout:
                handle.write(line)
                handle.flush()
                worker.last_message = line.strip()
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") == "thread.started":
                    with worker.lock:
                        worker.thread_id = event.get("thread_id")
                        worker.progress_message = f"thread {worker.thread_id} started"
        worker.exit_code = process.wait()

    worker.reader_thread = threading.Thread(target=reader_loop, daemon=True, name=f"{slot_name}-{issue.issue_id}")
    worker.reader_thread.start()
    return worker


def locate_session_file(thread_id: str) -> Path | None:
    matches = sorted(SESSION_ROOT.rglob(f"rollout-*-{thread_id}.jsonl"))
    return matches[-1] if matches else None


def update_worker_progress(worker: WorkerRun) -> None:
    with worker.lock:
        if worker.thread_id and worker.session_file is None:
            worker.session_file = locate_session_file(worker.thread_id)
        session_file = worker.session_file
        offset = worker.session_offset

    if session_file is None or not session_file.exists():
        return

    with session_file.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        for line in handle:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != "event_msg":
                continue
            payload = event.get("payload", {})
            if payload.get("type") != "agent_message":
                continue
            message = payload.get("message", "").strip()
            if not message:
                continue
            percent = None
            match = PROGRESS_RE.search(message)
            if match:
                percent = int(match.group(1))
            with worker.lock:
                worker.progress_message = message.replace("\n", " ")
                if percent is not None:
                    worker.progress_percent = percent
        with worker.lock:
            worker.session_offset = handle.tell()


def render_progress(workers: list[WorkerRun], *, force: bool = False) -> str:
    lines = []
    for worker in workers:
        with worker.lock:
            status = worker.progress_message
            if worker.progress_percent is not None:
                status = f"{worker.progress_percent}% | {status}"
            if worker.exit_code is not None:
                status = f"exit={worker.exit_code} | {status}"
            lines.append(f"{worker.slot_name:<6} {worker.issue.issue_id:<11} {status[:140]}")
    snapshot = "\n".join(lines)
    if force or snapshot:
        print(f"[{utc_now()}]")
        print(snapshot)
        print()
    return snapshot


def wait_for_workers(workers: list[WorkerRun]) -> None:
    last_snapshot = ""
    last_render = 0.0
    while True:
        for worker in workers:
            update_worker_progress(worker)
        snapshot = "\n".join(
            f"{worker.slot_name:<6} {worker.issue.issue_id:<11} "
            f"{(str(worker.progress_percent) + '% | ') if worker.progress_percent is not None else ''}"
            f"{worker.progress_message[:140]}"
            for worker in workers
        )
        now = time.time()
        if snapshot != last_snapshot or now - last_render >= 15:
            render_progress(workers)
            last_snapshot = snapshot
            last_render = now
        if all(worker.process.poll() is not None and not worker.reader_thread.is_alive() for worker in workers):
            render_progress(workers, force=True)
            return
        time.sleep(2)


def validate_worker_result(worker: WorkerRun) -> None:
    worker.reader_thread.join(timeout=5)
    if worker.exit_code != 0:
        raise HarnessError(f"{worker.issue.issue_id} exited with code {worker.exit_code}.")
    if not worker.result_path.exists():
        raise HarnessError(f"{worker.issue.issue_id} did not write a structured result file.")

    result = read_output_json(worker.result_path)
    if result.issue_id != worker.issue.issue_id:
        raise HarnessError(
            f"{worker.issue.issue_id} returned result for {result.issue_id}."
        )
    if result.root_close_candidate and worker.issue.issue_id != "NBNv2-ogh.19":
        raise HarnessError(f"{worker.issue.issue_id} cannot request root epic closure.")
    if result.outcome != "ready":
        raise HarnessError(
            f"{worker.issue.issue_id} did not complete cleanly: {result.outcome} {result.blockers}"
        )
    if not result.commit_sha:
        raise HarnessError(f"{worker.issue.issue_id} reported ready without a commit.")
    if not result.full_suite_passed:
        raise HarnessError(f"{worker.issue.issue_id} reported ready without a full-suite pass.")

    count = run_command(
        ["git", "rev-list", "--count", f"{worker.base_head}..HEAD"],
        cwd=worker.slot_dir,
    ).stdout.strip()
    if count != "1":
        raise HarnessError(
            f"{worker.issue.issue_id} must leave exactly one local commit; found {count}."
        )
    resolved = run_command(["git", "rev-parse", result.commit_sha], cwd=worker.slot_dir).stdout.strip()
    if resolved != result.commit_sha:
        raise HarnessError(
            f"{worker.issue.issue_id} reported {result.commit_sha}, clone resolved {resolved}."
        )
    status = run_command(["git", "status", "--porcelain"], cwd=worker.slot_dir).stdout.strip()
    if status:
        raise HarnessError(f"{worker.issue.issue_id} left a dirty slot clone:\n{status}")
    worker.result = result


def attempt_recovery(worker: WorkerRun, siblings: list[str]) -> WorkerRun:
    if worker.recovery_attempted:
        raise HarnessError(f"{worker.issue.issue_id} already used its recovery attempt.")
    worker.recovery_attempted = True
    reason = worker.last_message or "missing structured result"
    return start_exec_worker(
        worker.slot_name,
        worker.issue,
        worker.base_head,
        siblings,
        recovery_reason=reason,
        reuse_existing=True,
    )


def current_commit_subject(slot_dir: Path, commit_sha: str) -> str:
    return run_command(["git", "log", "-1", "--format=%s", commit_sha], cwd=slot_dir).stdout.strip()


def run_windows_build_test(issue_id: str) -> None:
    if os.environ.get("NBN_SKIP_WINDOWS_BUILD_TEST", "").strip().lower() in {"1", "true", "yes"}:
        print(f"[windows] {issue_id}: skipped via NBN_SKIP_WINDOWS_BUILD_TEST")
        return

    host = os.environ.get("NBN_WINDOWS_BUILD_TEST_HOST", "").strip()
    user = os.environ.get("NBN_WINDOWS_BUILD_TEST_USER", "").strip()
    repo_path = os.environ.get("NBN_WINDOWS_BUILD_TEST_REPO_PATH", "").strip()
    if not host or not user:
        raise HarnessError(
            "Windows remote validation is part of the issue completion flow.\n"
            "Set NBN_WINDOWS_BUILD_TEST_HOST and NBN_WINDOWS_BUILD_TEST_USER,\n"
            "or set NBN_SKIP_WINDOWS_BUILD_TEST=1 to bypass it intentionally."
        )

    command = [
        "bash",
        "tools/dev/pull-windows-build-test.sh",
        "--host",
        host,
        "--user",
        user,
    ]
    if repo_path:
        command.extend(["--repo-path", repo_path])
    run_command(command, capture_output=False)


def integrate_worker(worker: WorkerRun, state: State) -> None:
    assert worker.result is not None

    run_command(["git", "pull", "--rebase"])
    run_command(["git", "fetch", str(worker.slot_dir), worker.branch_name])
    run_command(["git", "cherry-pick", "--no-commit", "FETCH_HEAD"])

    if worker.result.summary:
        comment = (
            f"Harness integrated worker commit {worker.result.commit_sha}. "
            f"Summary: {worker.result.summary}"
        )
        run_command(["bd", "comments", "add", worker.issue.issue_id, comment])

    run_command(
        ["bd", "close", worker.issue.issue_id, "--reason", "Completed by the NBNv2-ogh epic harness."]
    )

    if worker.result.root_close_candidate:
        statuses = list_child_statuses()
        pending = sorted(
            issue_id
            for issue_id, status in statuses.items()
            if issue_id != worker.issue.issue_id and status != "closed"
        )
        if pending:
            raise HarnessError(
                f"{worker.issue.issue_id} requested root closure before all children were closed: {pending}"
            )
        run_command(
            ["bd", "close", ROOT_ISSUE, "--reason", "All child issues completed by the NBNv2-ogh harness."]
        )

    run_command(["git", "add", "-A"])
    subject = current_commit_subject(worker.slot_dir, worker.result.commit_sha)
    if not subject.startswith(f"{worker.issue.issue_id}:"):
        subject = f"{worker.issue.issue_id}: {worker.issue.title}"
    run_command(["git", "commit", "-m", subject])
    run_command(["git", "push"])
    run_windows_build_test(worker.issue.issue_id)
    run_command(["git", "status", "--short", "--branch"], capture_output=False)

    state.integrated.append(
        {
            "issue_id": worker.issue.issue_id,
            "commit_sha": worker.result.commit_sha,
            "integrated_at": utc_now(),
            "docs_changed": worker.result.docs_changed,
        }
    )
    if worker.result.docs_changed:
        state.has_docs_changes = True
    save_state(state)
    shutil.rmtree(worker.slot_dir, ignore_errors=True)


def run_final_validation(state: State) -> None:
    run_command(
        ["dotnet", "build", "NBNv2.sln", "-c", "Release", "--disable-build-servers"],
        capture_output=False,
    )
    run_command(
        [
            "dotnet",
            "test",
            "tests/Nbn.Tests/Nbn.Tests.csproj",
            "-c",
            "Release",
            "--disable-build-servers",
            "--filter",
            "FullyQualifiedName~Nbn.Tests.Proto.ProtoCompatibilityTests",
        ],
        capture_output=False,
    )
    run_command(
        ["dotnet", "test", "NBNv2.sln", "-c", "Release", "--disable-build-servers"],
        capture_output=False,
    )
    if state.has_docs_changes:
        run_command(["bash", "tools/docs/render-nbnv2-docs.sh", "--check"], capture_output=False)

    run_command(["git", "pull", "--rebase"])
    run_command(["bd", "sync"], capture_output=False)
    run_command(["git", "push"])
    run_command(["git", "status", "--short", "--branch"], capture_output=False)


def execute_batch(batch: list[str], issue_map: dict[str, IssueInfo], state: State) -> None:
    statuses = list_child_statuses()
    pending = [issue_id for issue_id in batch if statuses.get(issue_id) != "closed"]
    if not pending:
        print(f"Skipping already-closed batch: {batch}")
        return

    for issue_id in pending:
        missing = [dep for dep in issue_map[issue_id].block_dependencies if statuses.get(dep) != "closed"]
        if missing:
            raise HarnessError(f"{issue_id} still has open prerequisites: {missing}")

    base_head = git_head()
    print(f"Starting batch from {base_head}: {pending}")
    workers = [
        start_exec_worker(f"slot-{index + 1}", issue_map[issue_id], base_head, pending)
        for index, issue_id in enumerate(pending)
    ]
    wait_for_workers(workers)

    for index, worker in enumerate(list(workers)):
        try:
            validate_worker_result(worker)
        except HarnessError:
            replacement = attempt_recovery(worker, pending)
            workers[index] = replacement

    recovery_workers = [worker for worker in workers if worker.result is None]
    if recovery_workers:
        wait_for_workers(recovery_workers)
        for worker in recovery_workers:
            validate_worker_result(worker)

    for worker in workers:
        integrate_worker(worker, state)
        print(f"Integrated {worker.issue.issue_id}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fixed NBNv2-ogh Codex harness.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the fixed schedule and current tracker status without launching workers.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_runtime_layout()
    baseline_commit = git_head()
    state = load_state(baseline_commit)
    issue_map = load_issue_map()
    verify_schedule(issue_map)
    print_plan(issue_map, state.baseline_commit)

    if args.dry_run:
        return 0

    if os.environ.get("NBN_SKIP_WINDOWS_BUILD_TEST", "").strip().lower() not in {"1", "true", "yes"}:
        host = os.environ.get("NBN_WINDOWS_BUILD_TEST_HOST", "").strip()
        user = os.environ.get("NBN_WINDOWS_BUILD_TEST_USER", "").strip()
        if not host or not user:
            raise HarnessError(
                "Set NBN_WINDOWS_BUILD_TEST_HOST and NBN_WINDOWS_BUILD_TEST_USER before running the harness,\n"
                "or set NBN_SKIP_WINDOWS_BUILD_TEST=1 to bypass the required Windows remote validation."
            )

    ensure_clean_main_repo()
    created_root_working = ensure_root_working()
    try:
        print(f"Harness baseline commit: {state.baseline_commit}")
        for batch in SCHEDULE:
            execute_batch(batch, issue_map, state)

        root_status = fetch_issue(ROOT_ISSUE).status
        if root_status != "closed":
            raise HarnessError(f"{ROOT_ISSUE} is still {root_status} after the fixed schedule.")

        run_final_validation(state)
        print(f"Harness complete. Revert target if needed: {state.baseline_commit}")
        return 0
    finally:
        remove_root_working(created_root_working)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarnessError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
