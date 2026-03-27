from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from xml.sax.saxutils import escape


BASE_DIR = Path(__file__).resolve().parent
DOCS_DIR = BASE_DIR.parent
ROOT_DIR = DOCS_DIR.parent
SVG_DIR = BASE_DIR / "svg"
PNG_DIR = BASE_DIR / "png"
NODE_RENDERER = DOCS_DIR / "branding" / "render_png.mjs"

WIDTH = 1600
HEIGHT = 900
FONT_STACK = "Segoe UI, Helvetica Neue, Arial, sans-serif"

INK = "#131B2C"
SLATE = "#556071"
GOLD = "#E0A31A"
TEAL = "#1B8393"
ORANGE = "#F97316"
GREEN = "#2B8857"
RED = "#B94B34"
BG = "#FFFDF8"
PAPER = "#FFF8EB"
PAPER_GOLD = "#FFF3CF"
PAPER_TEAL = "#EAF7F9"
PAPER_ORANGE = "#FFF1E7"
PAPER_SLATE = "#F3F5F8"
BORDER = "#D9D1BB"
MUTED = "#7A8392"


def fmt(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def attrs(**kwargs: object) -> str:
    parts: list[str] = []
    for key, value in kwargs.items():
        if value is None:
            continue
        parts.append(f'{key.replace("_", "-")}="{escape(str(value))}"')
    return " ".join(parts)


def rect(x: float, y: float, width: float, height: float, **kwargs: object) -> str:
    return (
        f'<rect {attrs(x=fmt(x), y=fmt(y), width=fmt(width), height=fmt(height), **kwargs)} />'
    )


def line(x1: float, y1: float, x2: float, y2: float, **kwargs: object) -> str:
    return f'<line {attrs(x1=fmt(x1), y1=fmt(y1), x2=fmt(x2), y2=fmt(y2), **kwargs)} />'


def polyline(points: Iterable[tuple[float, float]], **kwargs: object) -> str:
    point_string = " ".join(f"{fmt(x)},{fmt(y)}" for x, y in points)
    return f'<polyline {attrs(points=point_string, **kwargs)} />'


def path(d: str, **kwargs: object) -> str:
    return f'<path {attrs(d=d, **kwargs)} />'


def circle(cx: float, cy: float, r: float, **kwargs: object) -> str:
    return f'<circle {attrs(cx=fmt(cx), cy=fmt(cy), r=fmt(r), **kwargs)} />'


def text(
    x: float,
    y: float,
    value: str,
    *,
    size: int = 22,
    weight: int = 500,
    fill: str = INK,
    anchor: str = "start",
    baseline: str = "alphabetic",
    letter_spacing: str | None = None,
) -> str:
    attributes = attrs(
        x=fmt(x),
        y=fmt(y),
        fill=fill,
        font_size=size,
        font_weight=weight,
        font_family=FONT_STACK,
        text_anchor=anchor,
        dominant_baseline=baseline,
        letter_spacing=letter_spacing,
    )
    return f"<text {attributes}>{escape(value)}</text>"


def multiline_text(
    x: float,
    y: float,
    lines: list[str],
    *,
    size: int = 19,
    weight: int = 500,
    fill: str = SLATE,
    anchor: str = "start",
    line_gap: float = 1.35,
) -> str:
    tspans: list[str] = []
    for index, line_value in enumerate(lines):
        dy = "0" if index == 0 else f"{line_gap}em"
        tspans.append(
            f'<tspan x="{fmt(x)}" dy="{dy}">{escape(line_value)}</tspan>'
        )
    attributes = attrs(
        x=fmt(x),
        y=fmt(y),
        fill=fill,
        font_size=size,
        font_weight=weight,
        font_family=FONT_STACK,
        text_anchor=anchor,
    )
    return f"<text {attributes}>" + "".join(tspans) + "</text>"


def group(elements: list[str], **kwargs: object) -> str:
    if kwargs:
        return f'<g {attrs(**kwargs)}>\n' + "\n".join(elements) + "\n</g>"
    return "<g>\n" + "\n".join(elements) + "\n</g>"


def pill(x: float, y: float, width: float, height: float, label: str, *, fill: str, text_fill: str = INK) -> str:
    return group(
        [
            rect(x, y, width, height, rx=height / 2, fill=fill),
            text(x + width / 2, y + height / 2 + 1, label, size=18, weight=700, fill=text_fill, anchor="middle", baseline="middle"),
        ]
    )


def number_badge(cx: float, cy: float, number: int, *, fill: str) -> str:
    return group(
        [
            circle(cx, cy, 18, fill=fill),
            text(cx, cy + 1, str(number), size=18, weight=800, fill="white", anchor="middle", baseline="middle"),
        ]
    )


def card(
    x: float,
    y: float,
    width: float,
    height: float,
    title_text: str,
    body_lines: list[str],
    *,
    accent: str,
    fill: str = PAPER,
    title_size: int = 24,
    body_size: int = 18,
) -> str:
    padding = 20
    accent_height = 9
    return group(
        [
            rect(x, y, width, height, rx=26, fill=fill, stroke=BORDER, stroke_width=2, filter="url(#shadow)"),
            rect(x, y, width, accent_height, rx=26, fill=accent),
            text(x + padding, y + 44, title_text, size=title_size, weight=750, fill=INK),
            multiline_text(x + padding, y + 78, body_lines, size=body_size, weight=500, fill=SLATE),
        ]
    )


def section_frame(
    x: float,
    y: float,
    width: float,
    height: float,
    title_text: str,
    subtitle: str,
    *,
    accent: str,
    fill: str = "white",
) -> str:
    return group(
        [
            rect(x, y, width, height, rx=34, fill=fill, stroke=BORDER, stroke_width=2),
            rect(x, y, width, 12, rx=34, fill=accent),
            text(x + 24, y + 44, title_text, size=28, weight=800, fill=INK),
            text(x + 24, y + 76, subtitle, size=18, weight=500, fill=MUTED),
        ]
    )


def arrow(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    *,
    stroke: str = INK,
    width: float = 4,
    dash: str | None = None,
    label_text: str | None = None,
    label_x: float | None = None,
    label_y: float | None = None,
) -> str:
    marker_id = {
        INK: "arrow-ink",
        TEAL: "arrow-teal",
        GOLD: "arrow-gold",
        ORANGE: "arrow-orange",
        GREEN: "arrow-green",
        RED: "arrow-red",
    }.get(stroke, "arrow-ink")
    elements = [
        line(
            x1,
            y1,
            x2,
            y2,
            stroke=stroke,
            stroke_width=fmt(width),
            stroke_dasharray=dash,
            stroke_linecap="round",
            marker_end=f"url(#{marker_id})",
        )
    ]
    if label_text is not None:
        elements.append(
            text(
                label_x if label_x is not None else (x1 + x2) / 2,
                label_y if label_y is not None else (y1 + y2) / 2 - 10,
                label_text,
                size=17,
                weight=700,
                fill=stroke,
                anchor="middle",
            )
        )
    return group(elements)


def elbow_arrow(
    points: list[tuple[float, float]],
    *,
    stroke: str = INK,
    width: float = 4,
    dash: str | None = None,
    label_text: str | None = None,
    label_x: float | None = None,
    label_y: float | None = None,
) -> str:
    marker_id = {
        INK: "arrow-ink",
        TEAL: "arrow-teal",
        GOLD: "arrow-gold",
        ORANGE: "arrow-orange",
        GREEN: "arrow-green",
        RED: "arrow-red",
    }.get(stroke, "arrow-ink")
    elements = [
        polyline(
            points,
            fill="none",
            stroke=stroke,
            stroke_width=fmt(width),
            stroke_dasharray=dash,
            stroke_linecap="round",
            stroke_linejoin="round",
            marker_end=f"url(#{marker_id})",
        )
    ]
    if label_text is not None:
        elements.append(
            text(
                label_x if label_x is not None else points[-1][0],
                label_y if label_y is not None else points[-1][1] - 14,
                label_text,
                size=17,
                weight=700,
                fill=stroke,
                anchor="middle",
            )
        )
    return group(elements)


def backdrop() -> str:
    return group(
        [
            rect(0, 0, WIDTH, HEIGHT, fill="url(#bg-gradient)"),
            circle(86, 90, 180, fill="#FFF3CF", opacity="0.8"),
            circle(1510, 120, 180, fill="#EAF7F9", opacity="0.85"),
            circle(1460, 830, 220, fill="#FFF1E7", opacity="0.85"),
            circle(130, 790, 180, fill="#F3F5F8", opacity="0.9"),
        ]
    )


def title_block(title_text: str, subtitle: str) -> str:
    return group(
        [
            text(70, 70, title_text, size=42, weight=850, fill=INK),
            text(70, 108, subtitle, size=21, weight=500, fill=SLATE),
        ]
    )


def callout_band(x: float, y: float, width: float, height: float, headline: str, body: str, *, accent: str) -> str:
    return group(
        [
            rect(x, y, width, height, rx=28, fill="white", stroke=BORDER, stroke_width=2),
            rect(x, y, 14, height, rx=28, fill=accent),
            text(x + 32, y + 40, headline, size=24, weight=780, fill=INK),
            multiline_text(x + 32, y + 72, body.split("\n"), size=18, weight=500, fill=SLATE),
        ]
    )


def worker_box(x: float, y: float, width: float, height: float, name: str, chips: list[tuple[str, str]]) -> str:
    chip_elements: list[str] = [
        rect(x, y, width, height, rx=28, fill="white", stroke=BORDER, stroke_width=2)
    ]
    chip_elements.append(text(x + 20, y + 38, name, size=24, weight=800, fill=INK))
    chip_x = x + 20
    chip_y = y + 56
    for label, fill in chips:
        chip_width = max(88, 16 + len(label) * 10)
        chip_elements.append(pill(chip_x, chip_y, chip_width, 28, label, fill=fill, text_fill=INK))
        chip_x += chip_width + 10
    return group(chip_elements)


def docs_link_note() -> str:
    return text(1528, 875, "SVG source + PNG render live together under docs/diagrams/", size=16, weight=500, fill=MUTED, anchor="end")


def defs() -> str:
    return """
<defs>
  <linearGradient id="bg-gradient" x1="0" y1="0" x2="1600" y2="900" gradientUnits="userSpaceOnUse">
    <stop offset="0%" stop-color="#FFFDF8" />
    <stop offset="100%" stop-color="#FFF8EE" />
  </linearGradient>
  <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
    <feDropShadow dx="0" dy="12" stdDeviation="14" flood-color="#131B2C" flood-opacity="0.10" />
  </filter>
  <marker id="arrow-ink" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#131B2C" />
  </marker>
  <marker id="arrow-teal" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#1B8393" />
  </marker>
  <marker id="arrow-gold" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#E0A31A" />
  </marker>
  <marker id="arrow-orange" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#F97316" />
  </marker>
  <marker id="arrow-green" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#2B8857" />
  </marker>
  <marker id="arrow-red" markerWidth="16" markerHeight="16" refX="12" refY="8" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 16 8 L 0 16 z" fill="#B94B34" />
  </marker>
</defs>
""".strip()


def svg_doc(elements: list[str]) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" fill="none">\n'
        f"{defs()}\n"
        + "\n".join(elements)
        + "\n</svg>\n"
    )


def runtime_service_topology() -> str:
    elements = [
        backdrop(),
        title_block(
            "NBN runtime service topology",
            "Control-plane roots stay discoverable while per-brain actors and shards move across workers.",
        ),
        section_frame(40, 140, 300, 700, "External surfaces", "Operator and client entrypoints", accent=GOLD),
        section_frame(360, 140, 520, 700, "Control plane", "Discovery, lifecycle, routing, and artifacts", accent=TEAL),
        section_frame(900, 140, 660, 700, "Per-brain runtime", "Actors can be co-located or worker-hosted", accent=ORANGE),
        card(70, 190, 240, 130, "External World", ["Proto.Actor clients", "input writes", "output subscriptions"], accent=GOLD, fill=PAPER_GOLD),
        card(70, 360, 240, 150, "Workbench", ["service discovery", "spawn / control", "observability peers"], accent=GOLD, fill=PAPER_GOLD),
        callout_band(
            70,
            560,
            240,
            180,
            "Abstraction boundary",
            "Clients do not need shard PIDs or worker placement.\nIO Gateway remains the stable public surface.",
            accent=GOLD,
        ),
        card(390, 190, 220, 150, "SettingsMonitor", ["registry + leases", "service endpoints", "settings + capability rows"], accent=TEAL, fill=PAPER_TEAL),
        card(630, 190, 220, 150, "HiveMind", ["global tick pacing", "brain lifecycle", "placement / recovery"], accent=TEAL, fill=PAPER_TEAL),
        card(390, 390, 220, 150, "IO Gateway", ["well-known gateway", "brain control routing", "per-brain coordinators"], accent=TEAL, fill=PAPER_TEAL),
        card(630, 390, 220, 120, "Reproduction", ["artifact-based child synthesis", "compatibility assessment"], accent=TEAL, fill=PAPER_TEAL),
        card(500, 575, 240, 110, "Observability hubs", ["debug + visualization streams"], accent=TEAL, fill=PAPER_TEAL),
        card(500, 705, 240, 105, "Artifact Store", ["CAS manifests", "partial fetch + cache"], accent=TEAL, fill=PAPER_TEAL),
        card(935, 175, 170, 100, "BrainRoot", ["control + metadata"], accent=ORANGE, fill=PAPER_ORANGE),
        card(1120, 175, 210, 100, "BrainSignalRouter", ["tick delivery + routing"], accent=ORANGE, fill=PAPER_ORANGE),
        rect(930, 320, 400, 455, rx=28, fill="none", stroke="#C8D3D6", stroke_width=2, stroke_dasharray="10 10"),
        text(950, 352, "Low-latency worker segment", size=18, weight=700, fill=TEAL),
        worker_box(940, 380, 180, 380, "Worker A", [("CPU", PAPER_GOLD), ("RTT 1x", PAPER_TEAL)]),
        worker_box(1140, 380, 180, 380, "Worker B", [("GPU", PAPER_ORANGE), ("RTT 1x", PAPER_TEAL)]),
        worker_box(1340, 380, 180, 380, "Worker C", [("remote", PAPER_SLATE), ("RTT 4x", PAPER_SLATE)]),
        card(960, 450, 140, 95, "InputCoordinator", ["brain-local input mode"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        card(960, 565, 140, 145, "RegionShard", ["region 0..1023", "compute backend", "selective region load"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        card(1160, 450, 140, 145, "RegionShard", ["region 1024..2047", "heavy mutual traffic"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        card(1160, 620, 140, 90, "RegionShard", ["region 2048..3071"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        card(1360, 450, 140, 95, "OutputCoordinator", ["vector + single events"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        card(1360, 565, 140, 145, "RegionShard", ["region 3072..3499", "tail shard if needed"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=16),
        arrow(310, 255, 390, 455, stroke=GOLD, label_text="brain control + IO", label_x=345, label_y=340),
        arrow(310, 430, 390, 265, stroke=GOLD, label_text="discovery", label_x=332, label_y=332),
        arrow(310, 432, 500, 630, stroke=GOLD, label_text="subscribe", label_x=390, label_y=560),
        arrow(610, 265, 630, 265, stroke=INK, label_text="registry + settings", label_x=620, label_y=250),
        arrow(500, 340, 500, 390, stroke=INK, label_text="forward / spawn", label_x=560, label_y=374),
        arrow(610, 455, 630, 455, stroke=INK, label_text="spawn / kill / pause", label_x=620, label_y=440),
        arrow(740, 510, 620, 705, stroke=INK, label_text="store child artifacts", label_x=735, label_y=620),
        arrow(740, 735, 945, 650, stroke=TEAL, label_text="selective .nbn/.nbs fetch", label_x=880, label_y=700),
        arrow(740, 735, 1360, 648, stroke=TEAL, label_text="cache-backed load", label_x=1120, label_y=700),
        arrow(740, 630, 310, 630, stroke=TEAL, label_text="peer subscriptions", label_x=515, label_y=615),
        arrow(740, 630, 1225, 275, stroke=TEAL, label_text="debug + viz streams", label_x=975, label_y=420),
        arrow(850, 265, 1020, 225, stroke=ORANGE, label_text="place + register", label_x=960, label_y=205),
        arrow(1110, 225, 1120, 225, stroke=ORANGE),
        elbow_arrow([(500, 540), (500, 602), (1030, 602), (1030, 545)], stroke=ORANGE, label_text="route directly to hosted coordinators", label_x=770, label_y=588),
        elbow_arrow([(1330, 225), (1450, 225), (1450, 450)], stroke=ORANGE, label_text="output events", label_x=1450, label_y=350),
        elbow_arrow([(1330, 225), (1210, 225), (1210, 450)], stroke=ORANGE, label_text="delivery fanout", label_x=1208, label_y=350),
        arrow(1320, 650, 1360, 650, stroke=ORANGE),
        docs_link_note(),
    ]
    return svg_doc(elements)


def tick_compute_deliver_pipeline() -> str:
    columns = [
        (60, 165, "Tick N compute", GOLD, PAPER_GOLD, ["1. merge I into B", "2. decay / homeostasis", "3. activate + reset", "4. fire axons if threshold passed"]),
        (430, 165, "Tick N deliver", TEAL, PAPER_TEAL, ["SignalRouter gathers fires", "deliver to target shards / IO", "accumulate into next-tick I", "publish output events / vectors"]),
        (800, 165, "Tick N+1 compute", GOLD, PAPER_GOLD, ["Merged inbox is now visible", "same gating + activation flow", "persistent B survives idle ticks", "new fires advance the network"]),
        (1170, 165, "Tick N+1 deliver", TEAL, PAPER_TEAL, ["new outgoing contributions", "InputCoordinator injects writes", "target inboxes refill", "cycle repeats deterministically"]),
    ]
    elements = [
        backdrop(),
        title_block(
            "Tick compute / deliver pipeline",
            "Delivery during tick N feeds inbox I, but activation can only see it at compute start of tick N+1.",
        ),
    ]
    for x, y, title_text, accent, fill, lines in columns:
        elements.append(card(x, y, 310, 290, title_text, lines, accent=accent, fill=fill))
    elements.extend(
        [
            arrow(370, 310, 430, 310, stroke=INK, label_text="phase barrier", label_x=400, label_y=290),
            arrow(740, 310, 800, 310, stroke=INK, label_text="next tick", label_x=770, label_y=290),
            arrow(1110, 310, 1170, 310, stroke=INK, label_text="phase barrier", label_x=1140, label_y=290),
            card(180, 540, 520, 215, "Neuron state across ticks", ["B = persistent buffer", "I = inbox accumulator for the next merge", "Signals do not mutate the current tick's activation path."], accent=GOLD, fill="white"),
            rect(240, 614, 160, 86, rx=22, fill=PAPER_GOLD, stroke=BORDER, stroke_width=2),
            rect(460, 614, 160, 86, rx=22, fill=PAPER_TEAL, stroke=BORDER, stroke_width=2),
            text(320, 648, "B", size=40, weight=850, fill=INK, anchor="middle"),
            text(320, 680, "persistent buffer", size=17, weight=600, fill=SLATE, anchor="middle"),
            text(540, 648, "I", size=40, weight=850, fill=INK, anchor="middle"),
            text(540, 680, "next merge inbox", size=17, weight=600, fill=SLATE, anchor="middle"),
            arrow(400, 656, 460, 656, stroke=GOLD, label_text="merge at compute start", label_x=430, label_y=635),
            card(800, 540, 300, 110, "Input path", ["External writes are buffered and injected during delivery."], accent=TEAL, fill="white"),
            card(1170, 540, 300, 110, "Output path", ["Output region 31 emits single + vector events per tick."], accent=TEAL, fill="white"),
            elbow_arrow([(950, 540), (950, 500), (1325, 500), (1325, 540)], stroke=TEAL, label_text="same tick publication", label_x=1120, label_y=482),
            callout_band(
                800,
                670,
                670,
                135,
                "Critical invariant",
                "Do not dispatch compute N+1 until deliver N is finalized. This is what keeps signals from tick N visible only on the next compute phase.",
                accent=RED,
            ),
            docs_link_note(),
        ]
    )
    return svg_doc(elements)


def sharding_and_placement() -> str:
    shard_x = 90
    shard_y = 205
    shard_width = 610
    stride_unit = 610 / 3500
    boundaries = [0, 1024, 2048, 3072, 3500]
    shard_colors = [GOLD, TEAL, ORANGE, GREEN]
    shard_labels = [
        ("Shard A", "0-1023"),
        ("Shard B", "1024-2047"),
        ("Shard C", "2048-3071"),
        ("Tail shard", "3072-3499"),
    ]
    elements = [
        backdrop(),
        title_block(
            "Region sharding and placement locality",
            "Shard cuts follow stride boundaries, then HiveMind maps the slices onto the lowest-latency worker plan that still fits resources.",
        ),
        section_frame(60, 150, 690, 670, "Region section view", "One region, one contiguous neuron span, stride-aligned shard cuts", accent=GOLD),
        section_frame(790, 150, 750, 670, "Placement plan", "Locality first, capacity second, epoch activation before resume", accent=TEAL),
        rect(shard_x, shard_y, shard_width, 88, rx=24, fill="white", stroke=BORDER, stroke_width=2),
    ]
    for index in range(4):
        left = shard_x + boundaries[index] * stride_unit
        right = shard_x + boundaries[index + 1] * stride_unit
        elements.append(
            rect(left, shard_y, right - left, 88, rx=24 if index in (0, 3) else 0, fill=shard_colors[index], opacity="0.92")
        )
        elements.append(text((left + right) / 2, shard_y + 38, shard_labels[index][0], size=21, weight=800, fill="white", anchor="middle"))
        elements.append(text((left + right) / 2, shard_y + 64, shard_labels[index][1], size=16, weight=600, fill="white", anchor="middle"))
    for boundary in boundaries:
        x = shard_x + boundary * stride_unit
        elements.append(line(x, shard_y - 26, x, shard_y + 108, stroke="#B7BFCB", stroke_width=2, stroke_dasharray="6 8"))
        label_anchor = "start" if boundary == 0 else "middle"
        label_x = x if boundary != 3500 else x - 8
        elements.append(text(label_x, shard_y - 36, str(boundary), size=16, weight=700, fill=SLATE, anchor=label_anchor))
    elements.extend(
        [
            multiline_text(90, 350, ["Checkpoint stride = 1024 neurons", "All shard starts align to stride.", "Only the final tail shard may be shorter."], size=19, weight=500, fill=SLATE),
            card(90, 470, 280, 170, "Placement inputs", ["worker CPU / RAM / storage", "GPU + VRAM fit when backend enabled", "peer RTT samples between workers"], accent=GOLD, fill=PAPER_GOLD),
            card(400, 470, 280, 170, "Runtime rules", ["keep heavy-traffic shards close", "refresh routing tables", "resume only after epoch is active"], accent=GOLD, fill=PAPER_GOLD),
            worker_box(840, 240, 190, 230, "Worker A", [("same host", PAPER_GOLD), ("RTT 1x", PAPER_TEAL)]),
            worker_box(1060, 240, 190, 230, "Worker B", [("low latency", PAPER_TEAL), ("GPU fit", PAPER_ORANGE)]),
            worker_box(1280, 240, 190, 230, "Worker C", [("fallback", PAPER_SLATE), ("RTT 4x", PAPER_SLATE)]),
            card(860, 320, 150, 120, "Shard A", ["0-1023", "InputCoordinator"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=16),
            card(1080, 320, 150, 120, "Shard B", ["1024-2047", "heavy traffic peer"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=16),
            card(1080, 460, 150, 120, "Shard C", ["2048-3071", "GPU-capable node"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=16),
            card(1300, 460, 150, 120, "Tail shard", ["3072-3499", "OutputCoordinator"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=16),
            line(1035, 360, 1078, 360, stroke=TEAL, stroke_width=8, stroke_dasharray="16 12"),
            text(1056, 345, "heavy mutual traffic", size=16, weight=700, fill=TEAL, anchor="middle"),
            arrow(700, 250, 835, 250, stroke=GOLD, label_text="build shard plan", label_x=770, label_y=230),
            arrow(700, 560, 840, 560, stroke=GOLD, label_text="score by locality + fit", label_x=770, label_y=540),
            callout_band(
                840,
                645,
                630,
                125,
                "Scheduling preference",
                "Prefer one machine first, then the lowest-latency worker segment. Split across slower links only when the tighter locality cannot satisfy the shard plan.",
                accent=TEAL,
            ),
            docs_link_note(),
        ]
    )
    return svg_doc(elements)


def snapshot_and_recovery_lifecycle() -> str:
    steps = [
        ("Base artifacts", ["brain definition .nbn", "optional snapshot .nbs"], GOLD, PAPER_GOLD),
        ("Spawn", ["load region directories", "place shards + coordinators"], TEAL, PAPER_TEAL),
        ("Active brain", ["compute / deliver ticks", "emit outputs + telemetry"], ORANGE, PAPER_ORANGE),
        ("Boundary snapshot", ["persist B buffers", "enabled masks + overlays"], GREEN, "white"),
        ("Failure detected", ["pause tick dispatch", "mark brain Recovering"], RED, "white"),
        ("Whole-brain restore", ["reload from durable .nbn + .nbs", "re-place all runtime actors"], TEAL, PAPER_TEAL),
        ("Epoch active", ["routing refreshed", "resume Active or Paused"], GREEN, "white"),
    ]
    positions = [50, 250, 470, 690, 910, 1130, 1350]
    elements = [
        backdrop(),
        title_block(
            "Spawn, snapshot, and recovery lifecycle",
            "Snapshots are taken at tick boundaries, and recovery always rebuilds the full brain from durable artifacts.",
        ),
    ]
    for index, ((title_text, lines, accent, fill), x) in enumerate(zip(steps, positions), start=1):
        elements.append(card(x, 180, 190, 180, title_text, lines, accent=accent, fill=fill, title_size=22, body_size=17))
        elements.append(number_badge(x + 24, 204, index, fill=accent))
        if index < len(steps):
            next_x = positions[index]
            elements.append(arrow(x + 190, 270, next_x, 270, stroke=accent if accent != GREEN else INK))
    elements.extend(
        [
            arrow(565, 360, 785, 535, stroke=GREEN, label_text="tick-boundary write", label_x=675, label_y=430),
            card(650, 520, 270, 150, "Latest durable snapshot", ["stores persistent buffers", "optional enabled bits", "axon overlay codes only where changed"], accent=GREEN, fill="white"),
            arrow(785, 670, 1225, 360, stroke=GREEN, label_text="restore source of truth", label_x=1015, label_y=560),
            callout_band(
                70,
                710,
                1460,
                125,
                "Recovery invariant",
                "A lost shard does not get patched back from surviving live peers. HiveMind unloads the old runtime and reconstructs the entire brain from the last stored .nbn + .nbs pair before a new placement epoch can resume ticks.",
                accent=RED,
            ),
            docs_link_note(),
        ]
    )
    return svg_doc(elements)


def reproduction_flow() -> str:
    gates = [
        "Format + quantization compatibility",
        "Input/output region invariants",
        "Region presence similarity",
        "Per-region neuron span tolerance",
        "Function distribution similarity",
        "Connectivity distribution similarity",
        "Spot-check overlap (strength ignored)",
    ]
    elements = [
        backdrop(),
        title_block(
            "Reproduction compatibility and child synthesis",
            "The runtime can stop at assessment-only reports or continue through locus-aligned mutation into a child artifact and optional spawn.",
        ),
        card(60, 170, 260, 150, "Parent A", ["BrainId or artifact refs", "base-only or live strengths"], accent=GOLD, fill=PAPER_GOLD),
        card(60, 350, 260, 150, "Parent B", ["BrainId or artifact refs", "same addressing options"], accent=GOLD, fill=PAPER_GOLD),
        card(60, 540, 260, 190, "ReproduceConfig", ["run_count + thresholds", "protect_io_region_neuron_counts=true by default", "manual IO add/remove only when protection is disabled"], accent=GOLD, fill=PAPER_GOLD),
        section_frame(370, 145, 450, 615, "Compatibility gate cascade", "Abort as soon as one gate fails", accent=TEAL),
        section_frame(860, 165, 300, 265, "Locus-aligned mutation", "(region_id, neuron_id) stays canonical", accent=ORANGE),
        section_frame(860, 500, 300, 180, "Assessment-only path", "No child synthesis, no spawn attempt", accent=TEAL),
        card(1210, 190, 270, 120, "Child artifact", ["new .nbn definition", "summary + mutation report"], accent=GREEN, fill="white"),
        card(1210, 350, 270, 150, "Optional spawn", ["SpawnChildDefaultOn / Never / Always", "IO / HiveMind surface child BrainId on success"], accent=GREEN, fill="white"),
        card(1210, 540, 270, 120, "Similarity report", ["compatible=false + abort_reason when a gate or runtime step fails"], accent=TEAL, fill="white"),
    ]
    gate_y = 220
    for index, gate in enumerate(gates, start=1):
        row_y = gate_y + (index - 1) * 68
        elements.append(rect(405, row_y, 380, 48, rx=18, fill="white", stroke=BORDER, stroke_width=2))
        elements.append(number_badge(433, row_y + 24, index, fill=TEAL))
        elements.append(text(465, row_y + 29, gate, size=18, weight=650, fill=INK))
    elements.extend(
        [
            multiline_text(890, 235, ["align by (region_id, neuron_id)", "mutate functions / params / axons", "IO axon invariants always apply", "duplicate targets still forbidden"], size=18, weight=500, fill=SLATE),
            multiline_text(890, 555, ["Use the same compatibility rules", "Return scores without child bytes", "Speciation bootstrap relies on this path"], size=18, weight=500, fill=SLATE),
            arrow(320, 245, 370, 245, stroke=GOLD, label_text="load parents", label_x=345, label_y=225),
            arrow(320, 425, 370, 425, stroke=GOLD),
            arrow(320, 635, 370, 635, stroke=GOLD, label_text="config + manual IO ops", label_x=345, label_y=615),
            arrow(820, 320, 860, 320, stroke=ORANGE, label_text="compatible", label_x=840, label_y=300),
            arrow(820, 590, 1210, 600, stroke=TEAL, label_text="assessment result", label_x=1015, label_y=575),
            elbow_arrow([(1010, 430), (1010, 460), (1345, 460), (1345, 500)], stroke=GREEN, label_text="spawn policy", label_x=1180, label_y=442),
            arrow(1160, 250, 1210, 250, stroke=GREEN, label_text="synthesize child", label_x=1185, label_y=230),
            arrow(1345, 310, 1345, 350, stroke=GREEN),
            callout_band(
                370,
                785,
                1110,
                82,
                "Protected IO regions",
                "Neuron count changes in regions 0 and 31 are rejected unless the caller explicitly disables protection and supplies manual add/remove operations. Axon invariants still apply either way.",
                accent=RED,
            ),
            docs_link_note(),
        ]
    )
    return svg_doc(elements)


def artifact_store_partial_fetch() -> str:
    elements = [
        backdrop(),
        title_block(
            "Artifact store, resolver, and partial fetch path",
            "Runtime callers resolve an exact store_uri, load manifests, and then choose either full-artifact or region-index-guided partial reads.",
        ),
        section_frame(50, 150, 280, 620, "Runtime callers", "These services all honor the resolver path", accent=GOLD),
        section_frame(360, 150, 300, 250, "Resolver", "No silent local fallback for non-file stores", accent=TEAL),
        section_frame(700, 120, 350, 280, "Manifest", "artifact_id, media_type, chunk list, optional region index", accent=TEAL),
        section_frame(700, 435, 350, 285, "CAS storage", "SQLite metadata + chunk payloads on disk", accent=GOLD),
        section_frame(1090, 150, 460, 240, "Partial fetch", "Use region index + HTTP Range when supported", accent=ORANGE),
        section_frame(1090, 435, 460, 285, "Full artifact + cache", "Canonical bytes stay available for whole-artifact users", accent=GREEN),
        card(80, 210, 220, 88, "HiveMind", ["spawn / recovery loads"], accent=GOLD, fill=PAPER_GOLD, title_size=22, body_size=16),
        card(80, 320, 220, 88, "RegionHost", ["region section materialization"], accent=GOLD, fill=PAPER_GOLD, title_size=22, body_size=16),
        card(80, 430, 220, 88, "WorkerNode", ["runtime-local cache reuse"], accent=GOLD, fill=PAPER_GOLD, title_size=22, body_size=16),
        card(80, 540, 220, 88, "Reproduction", ["artifact-based parent loading"], accent=GOLD, fill=PAPER_GOLD, title_size=22, body_size=16),
        card(390, 210, 240, 70, "Exact store_uri map", ["NBN_ARTIFACT_STORE_URI_MAP"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=15),
        card(390, 295, 240, 70, "Adapter path", ["in-process registrations"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=15),
        card(730, 190, 290, 76, "Manifest row", ["artifact sha256 + media type"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=15),
        card(730, 285, 290, 76, "Region index", ["offset / length per region section"], accent=TEAL, fill=PAPER_TEAL, title_size=21, body_size=15),
        card(730, 490, 290, 78, "SQLite metadata", ["artifacts + artifact_chunks"], accent=GOLD, fill=PAPER_GOLD, title_size=21, body_size=15),
        card(730, 590, 290, 88, "Chunk payloads", ["chunks/aa/<hash>", "hash uses uncompressed bytes"], accent=GOLD, fill=PAPER_GOLD, title_size=21, body_size=15),
        card(1120, 205, 400, 74, "Indexed partial read", ["GET manifest, then fetch only the needed byte ranges or region section"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=15),
        card(1120, 300, 400, 64, "Fallback", ["405 / 501 range responses fall back to full reads"], accent=ORANGE, fill=PAPER_ORANGE, title_size=21, body_size=15),
        card(1120, 490, 400, 84, "Node-local cache", ["reused after the first successful remote read / write-through"], accent=GREEN, fill="white", title_size=21, body_size=15),
        card(1120, 595, 400, 84, "Canonical bytes", ["whole-artifact callers still reconstruct exact original bytes"], accent=GREEN, fill="white", title_size=21, body_size=15),
        arrow(300, 255, 360, 255, stroke=GOLD, label_text="store_uri", label_x=330, label_y=235),
        arrow(300, 365, 360, 255, stroke=GOLD),
        arrow(300, 475, 360, 255, stroke=GOLD),
        arrow(300, 585, 360, 255, stroke=GOLD),
        arrow(660, 255, 700, 255, stroke=TEAL, label_text="resolve backend", label_x=680, label_y=235),
        arrow(875, 400, 875, 435, stroke=INK, label_text="persist / reuse chunks", label_x=965, label_y=420),
        arrow(1050, 255, 1090, 255, stroke=ORANGE, label_text="selective path", label_x=1070, label_y=235),
        arrow(1050, 590, 1090, 590, stroke=GREEN, label_text="full read path", label_x=1070, label_y=570),
        arrow(1320, 574, 1320, 595, stroke=GREEN),
        callout_band(
            50,
            795,
            1500,
            72,
            "Operational note",
            "The local cache is a convenience layer, not an authority layer. Upstream manifest/content responses still define truth for a remote artifact store.",
            accent=TEAL,
        ),
        docs_link_note(),
    ]
    return svg_doc(elements)


@dataclass(frozen=True)
class Diagram:
    slug: str
    render: Callable[[], str]


DIAGRAMS = [
    Diagram("runtime-service-topology", runtime_service_topology),
    Diagram("tick-compute-deliver-pipeline", tick_compute_deliver_pipeline),
    Diagram("sharding-and-placement", sharding_and_placement),
    Diagram("snapshot-and-recovery-lifecycle", snapshot_and_recovery_lifecycle),
    Diagram("reproduction-flow", reproduction_flow),
    Diagram("artifact-store-partial-fetch", artifact_store_partial_fetch),
]


def render_png(svg_path: Path, png_path: Path) -> None:
    subprocess.run(
        ["node", str(NODE_RENDERER), str(svg_path), str(png_path), str(WIDTH)],
        check=True,
        cwd=ROOT_DIR,
    )


def main() -> None:
    SVG_DIR.mkdir(parents=True, exist_ok=True)
    PNG_DIR.mkdir(parents=True, exist_ok=True)

    for diagram in DIAGRAMS:
        svg_path = SVG_DIR / f"{diagram.slug}.svg"
        png_path = PNG_DIR / f"{diagram.slug}.png"
        svg_path.write_text(diagram.render(), encoding="utf-8", newline="\n")
        render_png(svg_path, png_path)
        print(f"rendered {svg_path.relative_to(ROOT_DIR)}")
        print(f"rendered {png_path.relative_to(ROOT_DIR)}")


if __name__ == "__main__":
    main()
