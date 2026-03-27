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
FONT_STACK = "DejaVu Sans, Arial, Helvetica, sans-serif"

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
    title_size: int = 22,
    body_size: int = 15,
) -> str:
    padding = 18
    accent_height = 8
    return group(
        [
            rect(x, y, width, height, rx=26, fill=fill, stroke=BORDER, stroke_width=2, filter="url(#shadow)"),
            rect(x, y, width, accent_height, rx=26, fill=accent),
            text(x + padding, y + 40, title_text, size=title_size, weight=750, fill=INK),
            multiline_text(x + padding, y + 70, body_lines, size=body_size, weight=500, fill=SLATE, line_gap=1.28),
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
    elements = [
        rect(x, y, width, height, rx=34, fill=fill, stroke=BORDER, stroke_width=2),
        rect(x, y, width, 12, rx=34, fill=accent),
        text(x + 24, y + 42, title_text, size=25, weight=800, fill=INK),
    ]
    if subtitle:
        elements.append(
            multiline_text(x + 24, y + 68, subtitle.split("\n"), size=15, weight=500, fill=MUTED, line_gap=1.18)
        )
    return group(elements)


def arrow(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    *,
    stroke: str = INK,
    width: float = 3,
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
                size=14,
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
    width: float = 3,
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
                size=14,
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
            circle(90, 88, 120, fill="#FFF3CF", opacity="0.75"),
            circle(1515, 100, 110, fill="#EAF7F9", opacity="0.75"),
            circle(1495, 820, 120, fill="#FFF1E7", opacity="0.7"),
        ]
    )


def title_block(title_text: str, subtitle: str) -> str:
    return group(
        [
            text(70, 66, title_text, size=34, weight=850, fill=INK),
            multiline_text(70, 96, subtitle.split("\n"), size=17, weight=500, fill=SLATE, line_gap=1.18),
        ]
    )


def callout_band(x: float, y: float, width: float, height: float, headline: str, body: str, *, accent: str) -> str:
    return group(
        [
            rect(x, y, width, height, rx=28, fill="white", stroke=BORDER, stroke_width=2),
            rect(x, y, 14, height, rx=28, fill=accent),
            text(x + 32, y + 36, headline, size=21, weight=780, fill=INK),
            multiline_text(x + 32, y + 64, body.split("\n"), size=15, weight=500, fill=SLATE, line_gap=1.22),
        ]
    )


def worker_box(x: float, y: float, width: float, height: float, name: str, chips: list[tuple[str, str]]) -> str:
    chip_elements: list[str] = [
        rect(x, y, width, height, rx=28, fill="white", stroke=BORDER, stroke_width=2)
    ]
    chip_elements.append(text(x + 20, y + 36, name, size=22, weight=800, fill=INK))
    chip_x = x + 20
    chip_y = y + 50
    for label, fill in chips:
        chip_width = max(88, 16 + len(label) * 10)
        chip_elements.append(pill(chip_x, chip_y, chip_width, 28, label, fill=fill, text_fill=INK))
        chip_x += chip_width + 10
    return group(chip_elements)


def docs_link_note() -> str:
    return ""


def defs() -> str:
    return """
<defs>
  <linearGradient id="bg-gradient" x1="0" y1="0" x2="1600" y2="900" gradientUnits="userSpaceOnUse">
    <stop offset="0%" stop-color="#FFFDF8" />
    <stop offset="100%" stop-color="#FFF8EE" />
  </linearGradient>
  <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
    <feDropShadow dx="0" dy="8" stdDeviation="10" flood-color="#131B2C" flood-opacity="0.08" />
  </filter>
  <marker id="arrow-ink" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#131B2C" />
  </marker>
  <marker id="arrow-teal" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#1B8393" />
  </marker>
  <marker id="arrow-gold" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#E0A31A" />
  </marker>
  <marker id="arrow-orange" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#F97316" />
  </marker>
  <marker id="arrow-green" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#2B8857" />
  </marker>
  <marker id="arrow-red" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="strokeWidth">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#B94B34" />
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
            "Stable service roots on the left.\nMovable per-brain actors on the right.",
        ),
        section_frame(50, 150, 240, 620, "Clients", "", accent=GOLD),
        section_frame(340, 150, 480, 620, "Core services", "", accent=TEAL),
        section_frame(870, 150, 680, 620, "Per-brain runtime", "", accent=ORANGE),
        card(80, 360, 180, 110, "External World", ["clients", "input + output"], accent=GOLD, fill=PAPER_GOLD),
        card(80, 540, 180, 110, "Workbench", ["discovery", "control", "visualization"], accent=GOLD, fill=PAPER_GOLD),
        card(380, 220, 180, 110, "SettingsMonitor", ["registry", "settings", "capabilities"], accent=TEAL, fill=PAPER_TEAL),
        card(600, 220, 180, 110, "HiveMind", ["global tick", "brain lifecycle", "recovery"], accent=TEAL, fill=PAPER_TEAL),
        card(490, 390, 180, 110, "IO Gateway", ["public IO surface", "spawns coordinators"], accent=TEAL, fill=PAPER_TEAL),
        card(380, 560, 180, 110, "Observability", ["debug", "visualization"], accent=TEAL, fill=PAPER_TEAL),
        card(600, 560, 180, 110, "Artifact Store", [".nbn / .nbs CAS", "partial fetch"], accent=TEAL, fill=PAPER_TEAL),
        card(920, 230, 170, 90, "BrainRoot", ["control"], accent=ORANGE, fill=PAPER_ORANGE, title_size=20, body_size=15),
        card(1160, 230, 220, 90, "BrainSignalRouter", ["deliver + route"], accent=ORANGE, fill=PAPER_ORANGE, title_size=20, body_size=15),
        worker_box(910, 390, 180, 250, "Worker A", [("RTT 1x", PAPER_TEAL)]),
        worker_box(1130, 390, 180, 250, "Worker B", [("RTT 1x", PAPER_TEAL)]),
        worker_box(1350, 390, 180, 250, "Worker C", [("RTT 4x", PAPER_SLATE)]),
        rect(932, 470, 136, 44, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        rect(932, 528, 136, 60, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        rect(1152, 470, 136, 60, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        rect(1152, 544, 136, 60, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        rect(1372, 470, 136, 60, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        rect(1372, 544, 136, 44, rx=16, fill=PAPER_ORANGE, stroke=BORDER, stroke_width=2),
        text(1000, 496, "InputCoordinator", size=17, weight=700, fill=INK, anchor="middle", baseline="middle"),
        multiline_text(1000, 546, ["RegionShard A", "0-1023"], size=16, weight=700, fill=INK, anchor="middle"),
        multiline_text(1220, 496, ["RegionShard B", "1024-2047"], size=16, weight=700, fill=INK, anchor="middle"),
        multiline_text(1220, 570, ["RegionShard C", "2048-3071"], size=16, weight=700, fill=INK, anchor="middle"),
        multiline_text(1440, 496, ["RegionShard D", "3072-3499"], size=16, weight=700, fill=INK, anchor="middle"),
        text(1440, 570, "OutputCoordinator", size=17, weight=700, fill=INK, anchor="middle", baseline="middle"),
        arrow(260, 415, 490, 445, stroke=GOLD, label_text="IO + control", label_x=380, label_y=405),
        arrow(260, 595, 380, 615, stroke=GOLD, label_text="subscribe", label_x=320, label_y=580),
        arrow(560, 275, 600, 275, stroke=INK, label_text="registry", label_x=580, label_y=255),
        arrow(580, 390, 690, 330, stroke=INK, label_text="lifecycle", label_x=655, label_y=372),
        arrow(780, 275, 920, 275, stroke=ORANGE, label_text="place", label_x=850, label_y=255),
        arrow(1090, 275, 1160, 275, stroke=ORANGE),
        elbow_arrow([(1270, 320), (1270, 360), (1000, 360), (1000, 390)], stroke=ORANGE),
        elbow_arrow([(1270, 320), (1270, 360), (1220, 360), (1220, 390)], stroke=ORANGE),
        elbow_arrow([(1270, 320), (1270, 360), (1440, 360), (1440, 390)], stroke=ORANGE),
        arrow(780, 615, 910, 615, stroke=TEAL, label_text="load / recover artifacts", label_x=845, label_y=595),
        callout_band(
            80,
            800,
            1460,
            70,
            "Abstraction boundary",
            "Clients and Workbench talk to stable service roots.\nThey do not need shard placement or actor PIDs.",
            accent=GOLD,
        ),
    ]
    return svg_doc(elements)


def tick_compute_deliver_pipeline() -> str:
    columns = [
        (60, "Tick N compute", GOLD, PAPER_GOLD, ["merge I into B", "gate + activate", "reset buffer", "fire axons"]),
        (430, "Tick N deliver", TEAL, PAPER_TEAL, ["route fired signals", "fill target I", "publish outputs", "buffer inputs"]),
        (800, "Tick N+1 compute", GOLD, PAPER_GOLD, ["merged inbox visible", "same compute rules", "B persists", "new fires advance"]),
        (1170, "Tick N+1 deliver", TEAL, PAPER_TEAL, ["deliver new fires", "inject inputs", "fill inboxes", "repeat"]),
    ]
    elements = [
        backdrop(),
        title_block(
            "Tick compute / deliver pipeline",
            "Delivery during tick N fills inbox I.\nCompute sees it at tick N+1.",
        ),
    ]
    for x, title_text, accent, fill, lines in columns:
        elements.append(card(x, 210, 310, 220, title_text, lines, accent=accent, fill=fill))
    elements.extend(
        [
            arrow(370, 320, 430, 320, stroke=INK, label_text="phase barrier", label_x=400, label_y=300),
            arrow(740, 320, 800, 320, stroke=INK, label_text="next tick", label_x=770, label_y=300),
            arrow(1110, 320, 1170, 320, stroke=INK, label_text="phase barrier", label_x=1140, label_y=300),
            card(70, 520, 700, 170, "Neuron state between phases", ["B = persistent buffer", "I = next-merge inbox", "Tick N delivery changes I only."], accent=GOLD, fill="white"),
            rect(150, 598, 180, 70, rx=18, fill=PAPER_GOLD, stroke=BORDER, stroke_width=2),
            rect(500, 598, 180, 70, rx=18, fill=PAPER_TEAL, stroke=BORDER, stroke_width=2),
            text(240, 626, "B", size=36, weight=850, fill=INK, anchor="middle", baseline="middle"),
            text(240, 648, "persistent buffer", size=16, weight=700, fill=SLATE, anchor="middle", baseline="middle"),
            text(590, 626, "I", size=36, weight=850, fill=INK, anchor="middle", baseline="middle"),
            text(590, 648, "next-merge inbox", size=16, weight=700, fill=SLATE, anchor="middle", baseline="middle"),
            arrow(330, 633, 500, 633, stroke=GOLD, label_text="merge next tick", label_x=415, label_y=612),
            card(860, 520, 280, 120, "Input path", ["InputCoordinator injects buffered writes."], accent=TEAL, fill="white"),
            card(1220, 520, 280, 120, "Output path", ["Output region 31 publishes events."], accent=TEAL, fill="white"),
            callout_band(
                860,
                690,
                640,
                110,
                "Critical invariant",
                "Do not dispatch compute N+1 until deliver N is finalized.\nThat is what keeps tick N signals out of tick N compute.",
                accent=RED,
            ),
        ]
    )
    return svg_doc(elements)


def sharding_and_placement() -> str:
    shard_x = 115
    shard_y = 240
    shard_width = 590
    stride_unit = shard_width / 3500
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
            "Shard cuts follow stride boundaries.\nPlacement then scores locality and fit.",
        ),
        section_frame(60, 160, 720, 610, "Shard plan", "One region section cut into\nstride-aligned slices", accent=GOLD),
        section_frame(820, 160, 720, 610, "Placement plan", "Locality first.\nFit second.", accent=TEAL),
        rect(shard_x, shard_y, shard_width, 80, rx=22, fill="white", stroke=BORDER, stroke_width=2),
    ]
    for index in range(4):
        left = shard_x + boundaries[index] * stride_unit
        right = shard_x + boundaries[index + 1] * stride_unit
        elements.append(
            rect(left, shard_y, right - left, 80, rx=22 if index in (0, 3) else 0, fill=shard_colors[index], opacity="0.95")
        )
        elements.append(text((left + right) / 2, shard_y + 34, shard_labels[index][0], size=19, weight=800, fill="white", anchor="middle"))
        elements.append(text((left + right) / 2, shard_y + 58, shard_labels[index][1], size=15, weight=700, fill="white", anchor="middle"))
    for boundary in boundaries:
        x = shard_x + boundary * stride_unit
        elements.append(line(x, shard_y - 20, x, shard_y + 102, stroke="#B7BFCB", stroke_width=2, stroke_dasharray="6 8"))
        label_anchor = "start" if boundary == 0 else "middle"
        label_x = x if boundary != 3500 else x - 8
        elements.append(text(label_x, shard_y - 28, str(boundary), size=16, weight=700, fill=SLATE, anchor=label_anchor))
    elements.extend(
        [
            multiline_text(115, 360, ["Stride = 1024 neurons", "All starts align to stride.", "Only the tail can be short."], size=17, weight=500, fill=SLATE),
            card(115, 460, 180, 110, "Stride rule", ["start % stride == 0"], accent=GOLD, fill=PAPER_GOLD, title_size=20, body_size=15),
            card(310, 460, 180, 110, "Tail rule", ["final shard may be short"], accent=GOLD, fill=PAPER_GOLD, title_size=20, body_size=15),
            card(505, 460, 180, 110, "Epoch rule", ["refresh routes before resume"], accent=GOLD, fill=PAPER_GOLD, title_size=20, body_size=15),
            rect(860, 230, 420, 410, rx=28, fill="none", stroke="#C7D8DB", stroke_width=2, stroke_dasharray="10 10"),
            text(880, 258, "lowest-latency segment", size=15, weight=700, fill=TEAL),
            worker_box(860, 290, 180, 300, "Worker A", [("RTT 1x", PAPER_TEAL)]),
            worker_box(1080, 290, 180, 300, "Worker B", [("RTT 1x", PAPER_TEAL)]),
            worker_box(1320, 290, 180, 300, "Worker C", [("RTT 4x", PAPER_SLATE)]),
            pill(900, 390, 100, 34, "Shard A", fill=GOLD, text_fill="white"),
            pill(900, 438, 100, 34, "Input IO", fill=ORANGE, text_fill="white"),
            pill(1120, 390, 100, 34, "Shard B", fill=TEAL, text_fill="white"),
            pill(1120, 438, 100, 34, "Shard C", fill=ORANGE, text_fill="white"),
            pill(1360, 390, 100, 34, "Tail shard", fill=GREEN, text_fill="white"),
            pill(1360, 438, 120, 34, "Output IO", fill=ORANGE, text_fill="white"),
            arrow(705, 280, 860, 280, stroke=GOLD, label_text="score locality + fit", label_x=783, label_y=258),
            callout_band(
                115,
                640,
                1385,
                95,
                "Scheduling preference",
                "Prefer one machine first, then the lowest-latency segment.\nSplit across slower links only when needed.",
                accent=TEAL,
            ),
        ]
    )
    return svg_doc(elements)


def snapshot_and_recovery_lifecycle() -> str:
    steps = [
        ("Base artifacts", [".nbn definition", "latest .nbs snapshot"], GOLD, PAPER_GOLD),
        ("Spawn epoch", ["load directories", "place shards + IO"], TEAL, PAPER_TEAL),
        ("Active ticks", ["compute / deliver", "emit outputs"], ORANGE, PAPER_ORANGE),
        ("Boundary snapshot", ["persist B", "enabled bits", "axon overlays"], GREEN, "white"),
        ("Failure pause", ["stop dispatch", "mark Recovering"], RED, "white"),
        ("Restore + resume", ["reload whole brain", "activate new epoch", "resume ticks"], TEAL, PAPER_TEAL),
    ]
    positions = [40, 290, 540, 790, 1040, 1290]
    elements = [
        backdrop(),
        title_block(
            "Spawn, snapshot, and recovery lifecycle",
            "Snapshots happen at tick boundaries.\nRecovery rebuilds the whole brain from durable artifacts.",
        ),
    ]
    for index, ((title_text, lines, accent, fill), x) in enumerate(zip(steps, positions), start=1):
        elements.append(card(x, 220, 210, 170, title_text, lines, accent=accent, fill=fill, title_size=22, body_size=17))
        elements.append(number_badge(x + 24, 204, index, fill=accent))
        if index < len(steps):
            next_x = positions[index]
            elements.append(arrow(x + 210, 305, next_x, 305, stroke=INK))
    elements.extend(
        [
            card(620, 560, 360, 140, "Durable artifact pair", ["written only at boundaries", "used as full restore source"], accent=GREEN, fill="white"),
            arrow(895, 390, 800, 560, stroke=GREEN, label_text="write snapshot", label_x=900, label_y=500),
            arrow(980, 630, 1395, 390, stroke=GREEN, label_text="restore from artifacts", label_x=1180, label_y=540),
            callout_band(
                70,
                760,
                1460,
                95,
                "Recovery invariant",
                "A lost shard is not patched back from surviving live peers.\nHiveMind reconstructs the whole brain from the last .nbn + .nbs pair.",
                accent=RED,
            ),
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
            "Assessment can stop after scoring.\nCompatible requests can continue to child synthesis.",
        ),
        card(60, 220, 230, 110, "Parent A", ["BrainId or artifact refs", "base or live strength"], accent=GOLD, fill=PAPER_GOLD),
        card(60, 380, 230, 110, "Parent B", ["same addressing modes", "same rules"], accent=GOLD, fill=PAPER_GOLD),
        card(60, 540, 230, 150, "ReproduceConfig", ["run_count + thresholds", "IO counts protected by default", "manual IO edits only when protection is off"], accent=GOLD, fill=PAPER_GOLD),
        section_frame(350, 190, 470, 520, "Compatibility gates", "Abort on first failure", accent=TEAL),
        section_frame(880, 190, 600, 520, "Outputs", "Assessment or synthesis", accent=ORANGE),
        card(930, 290, 220, 130, "Assessment only", ["same gate cascade", "no child bytes", "used by speciation"], accent=TEAL, fill="white"),
        card(1200, 280, 240, 175, "Child synthesis", ["align by locus", "mutate functions / params / axons", "respect IO invariants", "optional spawn"], accent=ORANGE, fill="white"),
        card(1010, 545, 300, 100, "Similarity + mutation report", ["assessment returns here", "synthesis returns here"], accent=GREEN, fill="white"),
    ]
    gate_y = 220
    for index, gate in enumerate(gates, start=1):
        row_y = gate_y + (index - 1) * 56
        elements.append(rect(405, row_y, 380, 48, rx=18, fill="white", stroke=BORDER, stroke_width=2))
        elements.append(number_badge(433, row_y + 24, index, fill=TEAL))
        elements.append(text(465, row_y + 29, gate, size=18, weight=650, fill=INK))
    elements.extend(
        [
            arrow(290, 275, 350, 275, stroke=GOLD, label_text="load", label_x=320, label_y=255),
            arrow(290, 435, 350, 435, stroke=GOLD),
            arrow(290, 615, 350, 615, stroke=GOLD, label_text="policy", label_x=320, label_y=595),
            arrow(820, 330, 930, 350, stroke=TEAL),
            arrow(820, 360, 1200, 365, stroke=ORANGE),
            arrow(1040, 420, 1090, 545, stroke=TEAL),
            arrow(1320, 455, 1240, 545, stroke=ORANGE),
            callout_band(
                60,
                780,
                1480,
                75,
                "Protected IO regions",
                "Neuron count changes in regions 0 and 31 require protection to be disabled\nand explicit manual add/remove operations. Axon invariants still apply.",
                accent=RED,
            ),
        ]
    )
    return svg_doc(elements)


def artifact_store_partial_fetch() -> str:
    elements = [
        backdrop(),
        title_block(
            "Artifact store, resolver, and partial fetch path",
            "Callers resolve a store_uri, load a manifest,\nthen choose partial or full reads.",
        ),
        section_frame(60, 180, 250, 560, "Callers", "All of these start from\nstore_uri", accent=GOLD),
        section_frame(370, 240, 250, 180, "Resolver", "Exact store_uri or\nbuilt-in HTTP(S)", accent=TEAL),
        section_frame(690, 180, 310, 220, "Manifest", "artifact_id, chunk list,\noptional region index", accent=TEAL),
        section_frame(690, 460, 310, 220, "CAS storage", "SQLite metadata\n+ chunk payloads", accent=GOLD),
        section_frame(1070, 180, 420, 220, "Partial fetch", "Manifest-driven region reads", accent=ORANGE),
        section_frame(1070, 460, 420, 220, "Full read + cache", "Whole artifact remains available", accent=GREEN),
        card(90, 280, 190, 320, "Runtime callers", ["HiveMind", "RegionHost", "WorkerNode", "Reproduction"], accent=GOLD, fill=PAPER_GOLD),
        card(400, 285, 190, 90, "Resolver path", ["env map + adapters + HTTP(S)"], accent=TEAL, fill=PAPER_TEAL),
        card(720, 250, 250, 70, "Manifest row", ["sha256 + media type + chunks"], accent=TEAL, fill=PAPER_TEAL),
        card(720, 335, 250, 70, "Region index", ["optional offset / length"], accent=TEAL, fill=PAPER_TEAL),
        card(720, 525, 250, 70, "SQLite metadata", ["artifacts + artifact_chunks"], accent=GOLD, fill=PAPER_GOLD),
        card(720, 610, 250, 70, "Chunk files", ["chunks/aa/<hash>"], accent=GOLD, fill=PAPER_GOLD),
        card(1105, 250, 350, 80, "Indexed partial read", ["fetch only the needed bytes"], accent=ORANGE, fill=PAPER_ORANGE),
        card(1105, 345, 350, 70, "Range fallback", ["405 / 501 -> full artifact read"], accent=ORANGE, fill=PAPER_ORANGE),
        card(1105, 525, 350, 80, "Node-local cache", ["reuse after first successful fetch"], accent=GREEN, fill="white"),
        card(1105, 620, 350, 70, "Canonical bytes", ["whole-artifact callers can rebuild exact bytes"], accent=GREEN, fill="white"),
        arrow(310, 335, 370, 330, stroke=GOLD, label_text="store_uri", label_x=340, label_y=315),
        arrow(620, 330, 690, 290, stroke=TEAL, label_text="resolve", label_x=655, label_y=305),
        arrow(845, 400, 845, 460, stroke=INK, label_text="persist chunks", label_x=935, label_y=428),
        arrow(1000, 290, 1070, 290, stroke=ORANGE, label_text="partial read", label_x=1035, label_y=270),
        arrow(1000, 570, 1070, 570, stroke=GREEN, label_text="full read", label_x=1035, label_y=550),
        callout_band(
            60,
            785,
            1430,
            70,
            "Operational note",
            "The local cache is a convenience layer, not an authority layer.\nUpstream manifest/content responses still define truth.",
            accent=TEAL,
        ),
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
