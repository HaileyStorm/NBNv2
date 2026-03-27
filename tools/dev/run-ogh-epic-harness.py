#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EPIC_ID = "NBNv2-ogh"
MAX_PARALLEL = 3
PROGRESS_RE = re.compile(r"(?i)(?:about\s+)?(\d{1,3})%\s+complete")


@dataclass(frozen=True)
class IssueSpec:
    issue_id: str
    title: str
    deps: tuple[str, ...]
    owned_paths: tuple[str, ...]
    close_epic: bool = False


@dataclass
class IssueState:
    issue_id: str
    title: str
    wave: int
    status: str = "pending"
    progress: int | None = None
    attempt: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    thread_id: str | None = None
    workspace: str | None = None
    log_path: str | None = None
    last_message: str | None = None
    verification: list[str] = field(default_factory=list)
    exit_code: int | None = None


ISSUES: dict[str, IssueSpec] = {
    "NBNv2-ogh.1": IssueSpec(
        issue_id="NBNv2-ogh.1",
        title="Establish repo-wide maintainability baseline and enforcement",
        deps=(),
        owned_paths=(
            "Directory.Build.props",
            "Directory.Build.targets",
            ".editorconfig",
            "NBNv2.sln",
            "src/**/*.csproj",
            "tools/**/*.csproj",
            "tests/**/*.csproj",
            "docs/sections/overview/02-technology-stack-and-solution-structure.md",
        ),
    ),
    "NBNv2-ogh.2": IssueSpec(
        issue_id="NBNv2-ogh.2",
        title="Clean up shared contracts, formats, validation, and proto surface",
        deps=("NBNv2-ogh.1",),
        owned_paths=(
            "src/Nbn.Shared",
            "tests/Nbn.Tests/Format",
            "tests/Nbn.Tests/Shared",
            "docs/sections/runtime/04-identifiers-addressing-and-ids.md",
            "docs/sections/formats/17-file-formats-nbn-and-nbs.md",
            "docs/sections/protocols/19-protocol-schemas-proto.md",
        ),
    ),
    "NBNv2-ogh.3": IssueSpec(
        issue_id="NBNv2-ogh.3",
        title="Clean up artifact store, resolver, and reachable publisher surfaces",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2"),
        owned_paths=(
            "src/Nbn.Runtime.Artifacts",
            "tests/Nbn.Tests/RuntimeArtifacts",
            "tests/Nbn.Tests.CrossProcessArtifactWorker",
            "tests/Nbn.Tests/TestSupport/CrossProcessArtifactStoreHarness.cs",
            "docs/sections/formats/16-artifact-storage-and-deduplication.md",
            "docs/sections/formats/17-file-formats-nbn-and-nbs.md",
        ),
    ),
    "NBNv2-ogh.4": IssueSpec(
        issue_id="NBNv2-ogh.4",
        title="Clean up Brain, BrainHost, and Observability actor surfaces",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2"),
        owned_paths=(
            "src/Nbn.Runtime.Brain",
            "src/Nbn.Runtime.BrainHost",
            "src/Nbn.Runtime.Observability",
            "tests/Nbn.Tests/Brain",
            "tests/Nbn.Tests/Observability",
            "docs/sections/runtime/12-brain-lifecycle-failure-recovery-and-snapshots.md",
            "docs/sections/runtime/15-observability-debug-visualization-metrics-tracing.md",
        ),
    ),
    "NBNv2-ogh.5": IssueSpec(
        issue_id="NBNv2-ogh.5",
        title="Clean up SettingsMonitor, service discovery, and reporter surfaces",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2"),
        owned_paths=(
            "src/Nbn.Runtime.SettingsMonitor",
            "src/Nbn.Shared/ServiceEndpointDiscoveryClient.cs",
            "src/Nbn.Shared/SettingsMonitorReporter.cs",
            "tests/Nbn.Tests/SettingsMonitor",
            "tests/Nbn.Tests/Shared/SettingsMonitorReporterTests.cs",
            "docs/sections/runtime/03-distributed-architecture-and-service-topology.md",
            "docs/sections/runtime/18-database-schemas-sqlite.md",
        ),
    ),
    "NBNv2-ogh.6": IssueSpec(
        issue_id="NBNv2-ogh.6",
        title="Refactor HiveMind for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5"),
        owned_paths=(
            "src/Nbn.Runtime.HiveMind",
            "tests/Nbn.Tests/HiveMind",
            "docs/sections/runtime/05-simulation-model-and-tick-semantics.md",
            "docs/sections/runtime/06-global-tick-engine-backpressure-and-scheduling.md",
            "docs/sections/runtime/09-sharding-and-placement.md",
            "docs/sections/runtime/12-brain-lifecycle-failure-recovery-and-snapshots.md",
        ),
    ),
    "NBNv2-ogh.7": IssueSpec(
        issue_id="NBNv2-ogh.7",
        title="Refactor RegionHost and RegionShard for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3"),
        owned_paths=(
            "src/Nbn.Runtime.RegionHost",
            "tests/Nbn.Tests/RegionHost",
            "docs/sections/runtime/10-regionshard-compute-backends.md",
            "docs/sections/runtime/12-brain-lifecycle-failure-recovery-and-snapshots.md",
            "docs/sections/formats/16-artifact-storage-and-deduplication.md",
        ),
    ),
    "NBNv2-ogh.8": IssueSpec(
        issue_id="NBNv2-ogh.8",
        title="Refactor IO Gateway and coordinators for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"),
        owned_paths=(
            "src/Nbn.Runtime.IO",
            "tests/Nbn.Tests/Integration",
            "docs/sections/runtime/13-io-architecture-and-external-world-interface.md",
            "docs/sections/formats/16-artifact-storage-and-deduplication.md",
            "docs/sections/protocols/19-protocol-schemas-proto.md",
        ),
    ),
    "NBNv2-ogh.9": IssueSpec(
        issue_id="NBNv2-ogh.9",
        title="Refactor WorkerNode for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5", "NBNv2-ogh.7"),
        owned_paths=(
            "src/Nbn.Runtime.WorkerNode",
            "tests/Nbn.Tests/WorkerNode",
            "docs/sections/runtime/09-sharding-and-placement.md",
            "docs/sections/runtime/10-regionshard-compute-backends.md",
        ),
    ),
    "NBNv2-ogh.10": IssueSpec(
        issue_id="NBNv2-ogh.10",
        title="Refactor Reproduction runtime for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"),
        owned_paths=(
            "src/Nbn.Runtime.Reproduction",
            "tests/Nbn.Tests/Reproduction",
            "docs/sections/runtime/14-reproduction-and-evolution.md",
            "docs/sections/formats/16-artifact-storage-and-deduplication.md",
            "docs/sections/protocols/19-protocol-schemas-proto.md",
        ),
    ),
    "NBNv2-ogh.11": IssueSpec(
        issue_id="NBNv2-ogh.11",
        title="Refactor Speciation runtime for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5", "NBNv2-ogh.10"),
        owned_paths=(
            "src/Nbn.Runtime.Speciation",
            "tests/Nbn.Tests/Speciation",
            "docs/sections/runtime/14-reproduction-and-evolution.md",
            "docs/sections/runtime/18-database-schemas-sqlite.md",
            "docs/sections/protocols/19-protocol-schemas-proto.md",
        ),
    ),
    "NBNv2-ogh.12": IssueSpec(
        issue_id="NBNv2-ogh.12",
        title="Refactor EvolutionSim for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5", "NBNv2-ogh.10", "NBNv2-ogh.11"),
        owned_paths=(
            "tools/Nbn.Tools.EvolutionSim",
            "tests/Nbn.Tests/Tools",
            "docs/sections/runtime/14-reproduction-and-evolution.md",
        ),
    ),
    "NBNv2-ogh.13": IssueSpec(
        issue_id="NBNv2-ogh.13",
        title="Refactor PerfProbe for maintainability without behavior drift",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.5", "NBNv2-ogh.6", "NBNv2-ogh.9"),
        owned_paths=(
            "tools/Nbn.Tools.PerfProbe",
            "tests/Nbn.Tests/Tools",
        ),
    ),
    "NBNv2-ogh.14": IssueSpec(
        issue_id="NBNv2-ogh.14",
        title="Clean up Workbench services, client, launch, and orchestration surfaces",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.5"),
        owned_paths=(
            "tools/Nbn.Tools.Workbench/Services",
            "tools/Nbn.Tools.Workbench/ViewModels/ShellViewModel.cs",
            "tools/Nbn.Tools.Workbench/ViewModels/OrchestratorPanelViewModel*.cs",
            "tools/Nbn.Tools.Workbench/Design.md",
            "tests/Nbn.Tests/Workbench",
            "docs/sections/overview/02-technology-stack-and-solution-structure.md",
            "docs/sections/runtime/13-io-architecture-and-external-world-interface.md",
            "docs/sections/runtime/15-observability-debug-visualization-metrics-tracing.md",
        ),
    ),
    "NBNv2-ogh.15": IssueSpec(
        issue_id="NBNv2-ogh.15",
        title="Clean up Workbench Designer and artifact workflow surfaces",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.14"),
        owned_paths=(
            "tools/Nbn.Tools.Workbench/ViewModels/Designer*",
            "tools/Nbn.Tools.Workbench/Views/Panels/DesignerPanel.axaml",
            "tools/Nbn.Tools.Workbench/Design.md",
            "tests/Nbn.Tests/Workbench/Designer*Tests.cs",
            "docs/sections/formats/16-artifact-storage-and-deduplication.md",
            "docs/sections/formats/17-file-formats-nbn-and-nbs.md",
        ),
    ),
    "NBNv2-ogh.16": IssueSpec(
        issue_id="NBNv2-ogh.16",
        title="Clean up Workbench visualization surface",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.2", "NBNv2-ogh.3", "NBNv2-ogh.14"),
        owned_paths=(
            "tools/Nbn.Tools.Workbench/ViewModels/Viz*",
            "tools/Nbn.Tools.Workbench/Views/Panels/Viz*",
            "tools/Nbn.Tools.Workbench/Design.md",
            "tests/Nbn.Tests/Workbench/Viz*Tests.cs",
            "docs/sections/runtime/15-observability-debug-visualization-metrics-tracing.md",
        ),
    ),
    "NBNv2-ogh.17": IssueSpec(
        issue_id="NBNv2-ogh.17",
        title="Clean up Workbench IO, Reproduction, Speciation, Debug, and Shell panels",
        deps=("NBNv2-ogh.1", "NBNv2-ogh.12", "NBNv2-ogh.14"),
        owned_paths=(
            "tools/Nbn.Tools.Workbench/ViewModels/ShellViewModel.cs",
            "tools/Nbn.Tools.Workbench/ViewModels/IoPanelViewModel*.cs",
            "tools/Nbn.Tools.Workbench/ViewModels/ReproPanelViewModel*.cs",
            "tools/Nbn.Tools.Workbench/ViewModels/SpeciationPanelViewModel*.cs",
            "tools/Nbn.Tools.Workbench/ViewModels/DebugPanelViewModel.cs",
            "tools/Nbn.Tools.Workbench/Views/Panels",
            "tools/Nbn.Tools.Workbench/Design.md",
            "tools/Nbn.Tools.EvolutionSim/Design.md",
            "tests/Nbn.Tests/Workbench/*Panel*Tests.cs",
            "tests/Nbn.Tests/Workbench/ShellViewModelTests.cs",
        ),
    ),
    "NBNv2-ogh.18": IssueSpec(
        issue_id="NBNv2-ogh.18",
        title="Refactor test infrastructure and suite-wide readability/dedup surfaces",
        deps=(
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
        ),
        owned_paths=(
            "tests/Nbn.Tests/TestSupport",
            "tests/Nbn.Tests/HiveMind/HiveMindPlacementOrchestrationTests.cs",
            "tests/Nbn.Tests/Integration/IoGatewayArtifactReferenceTests.cs",
            "tests/Nbn.Tests/Speciation/SpeciationManagerActorTests.cs",
            "tests/Nbn.Tests/Reproduction/ReproductionManagerActorTests.cs",
            "tests/Nbn.Tests/Workbench/SpeciationPanelViewModelTests.cs",
            "tests/Nbn.Tests/Design.md",
        ),
    ),
    "NBNv2-ogh.19": IssueSpec(
        issue_id="NBNv2-ogh.19",
        title="Complete final Design.md and canonical docs freshness sweep",
        deps=(
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
        ),
        owned_paths=(
            "src/*/Design.md",
            "tools/*/Design.md",
            "tests/Nbn.Tests/Design.md",
            "docs/INDEX.md",
            "docs/sections/*",
            "tools/docs/render_nbnv2_docs.py",
            "tools/docs/render-nbnv2-docs.sh",
            "tools/docs/render-nbnv2-docs.ps1",
        ),
        close_epic=True,
    ),
}


WAVES: tuple[tuple[str, ...], ...] = (
    ("NBNv2-ogh.1",),
    ("NBNv2-ogh.2",),
    ("NBNv2-ogh.3", "NBNv2-ogh.4", "NBNv2-ogh.5"),
    ("NBNv2-ogh.10", "NBNv2-ogh.7", "NBNv2-ogh.14"),
    ("NBNv2-ogh.11", "NBNv2-ogh.6", "NBNv2-ogh.15"),
    ("NBNv2-ogh.12", "NBNv2-ogh.9", "NBNv2-ogh.16"),
    ("NBNv2-ogh.13", "NBNv2-ogh.8", "NBNv2-ogh.17"),
    ("NBNv2-ogh.18",),
    ("NBNv2-ogh.19",),
)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class HarnessError(RuntimeError):
    pass


class EpicHarness:
    def __init__(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.run_id = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        self.run_root = self.repo_root / ".artifacts-temp" / "ogh-harness" / self.run_id
        self.workspaces_root = self.run_root / "workspaces"
        self.logs_root = self.run_root / "logs"
        self.state_path = self.run_root / "state.json"
        self.source_working = self.repo_root / ".working"
        self.origin_url = ""
        self.branch = ""
        self.baseline_commit = ""
        self.current_wave = 0
        self.states: dict[str, IssueState] = {}

        for wave_index, wave in enumerate(WAVES, start=1):
            for issue_id in wave:
                spec = ISSUES[issue_id]
                self.states[issue_id] = IssueState(issue_id=issue_id, title=spec.title, wave=wave_index)

    def event(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    def prepare(self) -> None:
        if shutil.which("codex") is None:
            raise HarnessError("codex CLI was not found on PATH.")
        if shutil.which("bd") is None:
            raise HarnessError("bd CLI was not found on PATH.")

        self.origin_url = self.git(["config", "--get", "remote.origin.url"], cwd=self.repo_root).strip()
        self.branch = self.git(["branch", "--show-current"], cwd=self.repo_root).strip()
        self.baseline_commit = self.git(["rev-parse", "HEAD"], cwd=self.repo_root).strip()

        if not self.origin_url:
            raise HarnessError("remote.origin.url is not configured; the harness requires a pushable remote.")
        if not self.branch:
            raise HarnessError("Current branch is empty; check out the branch that should carry the epic.")

        tracked_dirty = self.git(["status", "--porcelain", "--untracked-files=no"], cwd=self.repo_root).strip()
        if tracked_dirty:
            raise HarnessError("Source repo has tracked modifications; commit or stash them before running the harness.")

        if self.source_working.exists():
            raise HarnessError(f"{self.source_working} already exists; clear the active workspace sentinel first.")

        self.verify_schedule()
        self.run_root.mkdir(parents=True, exist_ok=True)
        self.logs_root.mkdir(parents=True, exist_ok=True)
        self.workspaces_root.mkdir(parents=True, exist_ok=True)
        self.source_working.write_text(
            f"NBNv2-ogh harness run {self.run_id}\nBaseline commit: {self.baseline_commit}\n",
            encoding="utf-8",
        )
        self.write_state()

    def cleanup(self) -> None:
        if self.source_working.exists():
            self.source_working.unlink()

    def verify_schedule(self) -> None:
        if any(len(wave) > MAX_PARALLEL for wave in WAVES):
            raise HarnessError("A wave exceeds the three-worker concurrency cap.")

        expected_issue_ids = set(ISSUES)
        seen_issue_ids = {issue_id for wave in WAVES for issue_id in wave}
        if seen_issue_ids != expected_issue_ids:
            missing = sorted(expected_issue_ids - seen_issue_ids)
            extra = sorted(seen_issue_ids - expected_issue_ids)
            raise HarnessError(f"Wave schedule does not match issue set. Missing={missing} extra={extra}")

        wave_index_by_issue = {
            issue_id: wave_index for wave_index, wave in enumerate(WAVES, start=1) for issue_id in wave
        }
        for issue_id, spec in ISSUES.items():
            for dep in spec.deps:
                if wave_index_by_issue[dep] >= wave_index_by_issue[issue_id]:
                    raise HarnessError(f"{issue_id} is scheduled before dependency {dep} is complete.")

        raw_children = self.bd_json(["list", "--parent", EPIC_ID, "--all", "--json"], cwd=self.repo_root)
        live_ids = {item["id"] for item in raw_children}
        if live_ids != expected_issue_ids:
            missing = sorted(expected_issue_ids - live_ids)
            extra = sorted(live_ids - expected_issue_ids)
            raise HarnessError(f"Live Beads tree does not match harness issue set. Missing={missing} extra={extra}")

        live_specs = {issue_id: self.bd_json(["show", issue_id, "--json"], cwd=self.repo_root)[0] for issue_id in live_ids}
        for issue_id, spec in ISSUES.items():
            live_deps = sorted(dep["id"] for dep in live_specs[issue_id]["dependencies"] if dep["id"] in expected_issue_ids)
            if live_deps != sorted(spec.deps):
                raise HarnessError(f"Dependency mismatch for {issue_id}: expected {sorted(spec.deps)} got {live_deps}")

    async def run(self) -> int:
        self.event(
            f"Run {self.run_id} starting on branch {self.branch} from baseline {self.baseline_commit}. Logs: {self.run_root}"
        )
        self.event("Fixed wave schedule: " + " | ".join(",".join(wave) for wave in WAVES))

        for wave_index, wave in enumerate(WAVES, start=1):
            self.current_wave = wave_index
            self.event(f"Starting wave {wave_index}/{len(WAVES)}: {', '.join(wave)}")
            self.refresh_source_repo()

            tasks = []
            for issue_id in wave:
                if self.is_closed_in_repo(issue_id, self.repo_root):
                    state = self.states[issue_id]
                    state.status = "already_closed"
                    state.progress = 100
                    state.finished_at = now_utc()
                    state.last_message = "Issue already closed before this run."
                    self.write_state()
                    continue
                tasks.append(asyncio.create_task(self.run_issue(issue_id)))

            if tasks:
                results = await asyncio.gather(*tasks)
                if not all(results):
                    self.event(f"Wave {wave_index} failed. Preserving run data at {self.run_root}")
                    return 1

            self.refresh_source_repo()
            self.verify_wave_closed(wave)
            self.event(f"Wave {wave_index}/{len(WAVES)} completed.")

        if not self.is_closed_in_repo(EPIC_ID, self.repo_root):
            raise HarnessError(f"{EPIC_ID} is still open in the source repo after the final wave.")

        self.event(f"Epic harness completed successfully. Final source HEAD: {self.git(['rev-parse', 'HEAD'], self.repo_root).strip()}")
        return 0

    async def run_issue(self, issue_id: str) -> bool:
        spec = ISSUES[issue_id]
        state = self.states[issue_id]
        workspace = self.workspaces_root / issue_id
        log_path = self.logs_root / f"{issue_id}.jsonl"
        last_message_path = self.logs_root / f"{issue_id}.last.txt"

        state.workspace = str(workspace)
        state.log_path = str(log_path)
        self.write_state()

        if workspace.exists():
            shutil.rmtree(workspace)
        self.clone_workspace(workspace)
        self.install_workspace_guard(workspace)

        success = await self.execute_codex_attempt(
            issue_id=issue_id,
            attempt=1,
            prompt=self.build_worker_prompt(spec, workspace),
            workspace=workspace,
            log_path=log_path,
            last_message_path=last_message_path,
        )
        if not success:
            success = await self.execute_codex_attempt(
                issue_id=issue_id,
                attempt=2,
                prompt=self.build_recovery_prompt(spec, workspace, log_path, last_message_path),
                workspace=workspace,
                log_path=self.logs_root / f"{issue_id}.recovery.jsonl",
                last_message_path=self.logs_root / f"{issue_id}.recovery.last.txt",
                recovery=True,
            )

        if success:
            shutil.rmtree(workspace, ignore_errors=True)
        else:
            self.event(f"{issue_id} failed; workspace preserved at {workspace}")

        return success

    async def execute_codex_attempt(
        self,
        issue_id: str,
        attempt: int,
        prompt: str,
        workspace: Path,
        log_path: Path,
        last_message_path: Path,
        recovery: bool = False,
    ) -> bool:
        state = self.states[issue_id]
        state.status = "recovering" if recovery else "running"
        state.attempt = attempt
        state.started_at = state.started_at or now_utc()
        state.progress = 0
        state.last_message = None
        state.verification = []
        self.write_state()

        cmd = [
            "codex",
            "exec",
            "--json",
            "--color",
            "never",
            "--dangerously-bypass-approvals-and-sandbox",
            "-C",
            str(workspace),
            "--output-last-message",
            str(last_message_path),
            "-",
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=workspace,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert process.stdin is not None
        process.stdin.write(prompt.encode("utf-8"))
        await process.stdin.drain()
        process.stdin.close()

        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as log_file:
            assert process.stdout is not None
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace")
                log_file.write(decoded)
                self.parse_worker_output(issue_id, decoded)

        state.exit_code = await process.wait()
        if last_message_path.exists():
            last_text = last_message_path.read_text(encoding="utf-8").strip()
            if last_text:
                state.last_message = last_text

        verification = self.verify_workspace(issue_id, workspace)
        state.verification = verification
        state.finished_at = now_utc()
        state.progress = 100 if verification and all(note.startswith("ok:") for note in verification) else state.progress
        state.status = "completed" if verification and all(note.startswith("ok:") for note in verification) else "failed"
        self.write_state()

        if state.status == "completed":
            self.event(f"{issue_id} completed on attempt {attempt}.")
            return True

        self.event(f"{issue_id} did not satisfy verification on attempt {attempt}.")
        for note in verification:
            self.event(f"  {issue_id}: {note}")
        return False

    def parse_worker_output(self, issue_id: str, line: str) -> None:
        state = self.states[issue_id]
        prior_progress = state.progress
        prior_thread = state.thread_id
        text_fragments: list[str] = []
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict):
            if payload.get("type") == "thread.started":
                state.thread_id = payload.get("thread_id")
            text_fragments.extend(self.collect_strings(payload))
        else:
            text_fragments.append(line.strip())

        for text in text_fragments:
            if not text:
                continue
            match = PROGRESS_RE.search(text)
            if match:
                try:
                    state.progress = max(0, min(100, int(match.group(1))))
                except ValueError:
                    pass
            if len(text) > 400:
                text = text[:397] + "..."
            state.last_message = text

        if state.thread_id and state.thread_id != prior_thread:
            self.event(f"{issue_id} attached to Codex thread {state.thread_id}")
        if state.progress is not None and state.progress != prior_progress:
            self.event(f"{issue_id} progress {state.progress}%")

        self.write_state()

    def collect_strings(self, value: Any) -> list[str]:
        strings: list[str] = []
        if isinstance(value, str):
            strings.append(value)
        elif isinstance(value, dict):
            for nested in value.values():
                strings.extend(self.collect_strings(nested))
        elif isinstance(value, list):
            for nested in value:
                strings.extend(self.collect_strings(nested))
        return strings

    def verify_workspace(self, issue_id: str, workspace: Path) -> list[str]:
        notes: list[str] = []

        issue_status = self.issue_status(issue_id, workspace)
        if issue_status == "closed":
            notes.append("ok: issue is closed")
        else:
            notes.append(f"error: issue status is {issue_status!r}, expected 'closed'")

        cleanliness = self.git(["status", "--porcelain"], cwd=workspace).strip()
        if cleanliness:
            notes.append("error: workspace is not clean after the worker finished")
        else:
            notes.append("ok: workspace is clean")

        self.git(["fetch", "origin"], cwd=workspace)
        ahead_behind = self.git(["rev-list", "--left-right", "--count", f"HEAD...origin/{self.branch}"], cwd=workspace).strip()
        if ahead_behind == "0\t0" or ahead_behind == "0 0":
            notes.append("ok: workspace HEAD matches origin")
        else:
            notes.append(f"error: workspace and origin diverged ({ahead_behind})")

        if issue_id == "NBNv2-ogh.19":
            epic_status = self.issue_status(EPIC_ID, workspace)
            if epic_status == "closed":
                notes.append("ok: root epic is closed")
            else:
                notes.append(f"error: root epic status is {epic_status!r}, expected 'closed'")

        return notes

    def clone_workspace(self, workspace: Path) -> None:
        workspace.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            "git",
            "clone",
            "--branch",
            self.branch,
            "--single-branch",
            "--reference-if-able",
            str(self.repo_root),
            self.origin_url,
            str(workspace),
        ]
        subprocess.run(cmd, cwd=self.repo_root, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        subprocess.run(["git", "config", "core.hooksPath", ".githooks"], cwd=workspace, check=True)

    def install_workspace_guard(self, workspace: Path) -> None:
        working_path = workspace / ".working"
        working_path.write_text(
            f"NBNv2-ogh harness workspace\nRun: {self.run_id}\nIssue workspace: {workspace.name}\n",
            encoding="utf-8",
        )
        exclude_path = workspace / ".git" / "info" / "exclude"
        existing = exclude_path.read_text(encoding="utf-8") if exclude_path.exists() else ""
        if ".working" not in existing.splitlines():
            exclude_path.write_text(existing + ("" if not existing or existing.endswith("\n") else "\n") + ".working\n", encoding="utf-8")

    def build_worker_prompt(self, spec: IssueSpec, workspace: Path) -> str:
        owned = "\n".join(f"- {path}" for path in spec.owned_paths)
        requirements = [
            f"1. Run `bd show {spec.issue_id}` first and treat it as authoritative for acceptance criteria, docs, tests, and dependencies.",
            "2. Follow the repo's AGENTS guidance, including doc-first scouting, Beads lifecycle, build/test expectations, and the landing flow.",
            "3. Emit short progress updates in agent messages with exact wording like `About 35% complete.` whenever you cross a major milestone; the harness scrapes these for live progress.",
            f"4. Complete only {spec.issue_id} end-to-end in this clone. Do the required code/docs work, run the required validation, commit, `git pull --rebase`, `bd sync`, `git push`, confirm `git status` is up to date with origin, and close the issue with `bd close {spec.issue_id} --reason ...`.",
            "5. Use the Windows remote validation flow described in `tools/dev/windows-log-pull.md` once local validation and push succeed; resolve and retest if that remote step exposes problems.",
        ]
        if spec.close_epic:
            requirements.append(
                f"6. After closing {spec.issue_id}, confirm every child of {EPIC_ID} is closed and then close {EPIC_ID} with `bd close {EPIC_ID} --reason ...`."
            )
            requirements.append("7. Do not pick or plan other Beads issues. The harness owns issue ordering and dependency scheduling.")
            requirements.append("8. Final message: concise summary, validation run, commit SHA(s), and whether the Windows remote build/test step passed.")
        else:
            requirements.append("6. Do not pick or plan other Beads issues. The harness owns issue ordering and dependency scheduling.")
            requirements.append("7. Final message: concise summary, validation run, commit SHA(s), and whether the Windows remote build/test step passed.")

        return f"""You are Codex running inside an isolated clone created by the NBNv2-ogh epic harness.

Harness context:
- Root epic: {EPIC_ID}
- Assigned issue: {spec.issue_id} - {spec.title}
- Fixed wave: {self.states[spec.issue_id].wave}/{len(WAVES)}
- Baseline commit before the epic run: {self.baseline_commit}
- Target branch: {self.branch}
- Exclusive workspace clone: {workspace}
- The harness already created a repo-root .working sentinel for this clone.
- Stay inside this issue's expected ownership slice:
{owned}

Execution requirements:
{chr(10).join(requirements)}

If you hit rebase or tracker merge conflicts from earlier wave commits, resolve them and continue; the harness chose this schedule because the code slices are intended to be compatible.
"""

    def build_recovery_prompt(self, spec: IssueSpec, workspace: Path, log_path: Path, last_message_path: Path) -> str:
        last_message = ""
        if last_message_path.exists():
            last_message = last_message_path.read_text(encoding="utf-8").strip()

        verification = "\n".join(f"- {note}" for note in self.states[spec.issue_id].verification) or "- no verification notes captured"
        return f"""A prior Codex worker for {spec.issue_id} exited without satisfying the harness verification.

Context:
- Assigned issue: {spec.issue_id} - {spec.title}
- Workspace: {workspace}
- Prior log: {log_path}
- Prior final message: {last_message or "(missing)"} 
- Current verification failures:
{verification}

Task:
1. Inspect the workspace and the prior run log.
2. Finish only the missing work needed to fully land {spec.issue_id}. If the implementation is already done, do the remaining Beads/git closure steps only.
3. Emit progress updates using the same exact percent format: `About 35% complete.`
4. End only when the issue is closed, the workspace is clean, the branch matches origin, and{" the root epic is also closed," if spec.close_epic else ""} the final message concisely states what you fixed.
"""

    def issue_status(self, issue_id: str, cwd: Path) -> str:
        payload = self.bd_json(["show", issue_id, "--json"], cwd=cwd)
        if not payload:
            raise HarnessError(f"bd show returned no data for {issue_id}")
        return str(payload[0]["status"])

    def is_closed_in_repo(self, issue_id: str, cwd: Path) -> bool:
        return self.issue_status(issue_id, cwd) == "closed"

    def verify_wave_closed(self, wave: tuple[str, ...]) -> None:
        for issue_id in wave:
            if not self.is_closed_in_repo(issue_id, self.repo_root):
                raise HarnessError(f"{issue_id} is not closed in the source repo after its wave completed.")

    def refresh_source_repo(self) -> None:
        self.git(["fetch", "origin"], cwd=self.repo_root)
        self.git(["pull", "--rebase"], cwd=self.repo_root)
        self.write_state()

    def bd_json(self, args: list[str], cwd: Path) -> Any:
        output = subprocess.run(["bd", *args], cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return json.loads(output.stdout)

    def git(self, args: list[str], cwd: Path) -> str:
        completed = subprocess.run(["git", *args], cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return completed.stdout

    def write_state(self) -> None:
        summary = {
            "run_id": self.run_id,
            "epic_id": EPIC_ID,
            "baseline_commit": self.baseline_commit,
            "branch": self.branch,
            "origin_url": self.origin_url,
            "current_wave": self.current_wave,
            "waves": [list(wave) for wave in WAVES],
            "states": {issue_id: asdict(state) for issue_id, state in self.states.items()},
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


async def main() -> int:
    harness = EpicHarness()
    try:
        harness.prepare()
        return await harness.run()
    finally:
        harness.cleanup()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(130)
