from __future__ import annotations

import math
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from PIL import Image, ImageDraw, ImageFont


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
SVG_DIR = BASE_DIR / "svg"
PNG_DIR = BASE_DIR / "png"
NODE_RENDERER = ROOT_DIR / "render_png.mjs"
ICON_SIZE = 1024
LOGO_WIDTH = 1500
LOGO_HEIGHT = 560


def fmt(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def pt(x: float, y: float) -> str:
    return f"{fmt(x)},{fmt(y)}"


def attr_name(name: str) -> str:
    return name.replace("_", "-")


def attrs(**kwargs: object) -> str:
    parts: list[str] = []
    for key, value in kwargs.items():
        if value is None:
            continue
        parts.append(f'{attr_name(key)}="{value}"')
    return " ".join(parts)


def circle(x: float, y: float, r: float, **kwargs: object) -> str:
    return f'<circle {attrs(cx=fmt(x), cy=fmt(y), r=fmt(r), **kwargs)} />'


def rect(x: float, y: float, width: float, height: float, **kwargs: object) -> str:
    return (
        f'<rect {attrs(x=fmt(x), y=fmt(y), width=fmt(width), height=fmt(height), **kwargs)} />'
    )


def line(x1: float, y1: float, x2: float, y2: float, **kwargs: object) -> str:
    return f'<line {attrs(x1=fmt(x1), y1=fmt(y1), x2=fmt(x2), y2=fmt(y2), **kwargs)} />'


def path(d: str, **kwargs: object) -> str:
    return f'<path {attrs(d=d, **kwargs)} />'


def polygon(points: list[tuple[float, float]], **kwargs: object) -> str:
    point_string = " ".join(pt(x, y) for x, y in points)
    return f'<polygon {attrs(points=point_string, **kwargs)} />'


def group(elements: list[str], **kwargs: object) -> str:
    if kwargs:
        return f'<g {attrs(**kwargs)}>\n' + "\n".join(elements) + "\n</g>"
    return "<g>\n" + "\n".join(elements) + "\n</g>"


def svg_doc(width: int, height: int, elements: list[str]) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" fill="none">\n'
        + "\n".join(elements)
        + "\n</svg>\n"
    )


def arc_path(cx: float, cy: float, radius: float, start_deg: float, end_deg: float) -> str:
    start = math.radians(start_deg)
    end = math.radians(end_deg)
    start_pt = (cx + radius * math.cos(start), cy + radius * math.sin(start))
    end_pt = (cx + radius * math.cos(end), cy + radius * math.sin(end))
    sweep = (end_deg - start_deg) % 360
    large_arc = 1 if sweep > 180 else 0
    return (
        f"M {pt(*start_pt)} "
        f"A {fmt(radius)} {fmt(radius)} 0 {large_arc} 1 {pt(*end_pt)}"
    )


def diamond(cx: float, cy: float, radius: float) -> list[tuple[float, float]]:
    return [
        (cx, cy - radius),
        (cx + radius, cy),
        (cx, cy + radius),
        (cx - radius, cy),
    ]


def regular_polygon(
    cx: float,
    cy: float,
    radius: float,
    sides: int,
    rotation_deg: float = 0,
) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for index in range(sides):
        angle = math.radians(rotation_deg + index * (360 / sides))
        points.append((cx + radius * math.cos(angle), cy + radius * math.sin(angle)))
    return points


def open_ring_segment(
    cx: float,
    cy: float,
    radius: float,
    start_deg: float,
    end_deg: float,
    stroke: str,
    width: float,
    cap: str = "butt",
    opacity: float | None = None,
) -> str:
    return path(
        arc_path(cx, cy, radius, start_deg, end_deg),
        stroke=stroke,
        stroke_width=fmt(width),
        stroke_linecap=cap,
        opacity=None if opacity is None else fmt(opacity),
    )


def parallelogram_along_line(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    width: float,
) -> list[tuple[float, float]]:
    dx = x2 - x1
    dy = y2 - y1
    length = math.hypot(dx, dy)
    if length == 0:
        return [(x1, y1)] * 4
    ux = dx / length
    uy = dy / length
    px = -uy * width / 2
    py = ux * width / 2
    return [
        (x1 + px, y1 + py),
        (x2 + px, y2 + py),
        (x2 - px, y2 - py),
        (x1 - px, y1 - py),
    ]


@dataclass(frozen=True)
class Concept:
    slug: str
    title: str
    family: str
    summary: str
    ink: str
    accent: str
    accent2: str
    wordmark_style: str
    icon_builder: Callable[["Concept"], str]
    logo_overlay_builder: Callable[["Concept"], str]
    symbol_scale: float = 0.29
    symbol_x: int = 86
    symbol_y: int = 88
    wordmark_x: int = 446
    wordmark_y: int = 188


def build_region_wordmark(x: float, y: float, height: float, color: str) -> str:
    scale = height / 140
    stroke = 22 * scale
    width = 108 * scale
    gap = 36 * scale
    b_width = 126 * scale
    bottom = y + height
    n2_x = x + width + gap + b_width + gap
    b_x = x + width + gap
    elements = [
        path(
            f"M {pt(x, bottom)} L {pt(x, y)} L {pt(x + width, bottom)} L {pt(x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
            f"M {pt(b_x, y)} L {pt(b_x + b_width * 0.62, y)} "
            f"Q {pt(b_x + b_width, y)} {pt(b_x + b_width, y + height * 0.25)} "
            f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width * 0.62, y + height * 0.5)} "
            f"L {pt(b_x, y + height * 0.5)} "
            f"M {pt(b_x, y + height * 0.5)} L {pt(b_x + b_width * 0.62, y + height * 0.5)} "
            f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width, y + height * 0.75)} "
            f"Q {pt(b_x + b_width, bottom)} {pt(b_x + b_width * 0.62, bottom)} "
            f"L {pt(b_x, bottom)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(n2_x, bottom)} L {pt(n2_x, y)} L {pt(n2_x + width, bottom)} L {pt(n2_x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
    ]
    return group(elements)


def build_shard_wordmark(x: float, y: float, height: float, color: str) -> str:
    scale = height / 140
    stroke = 24 * scale
    width = 112 * scale
    gap = 34 * scale
    b_width = 122 * scale
    bottom = y + height
    n2_x = x + width + gap + b_width + gap
    b_x = x + width + gap
    elements = [
        path(
            f"M {pt(x, bottom)} L {pt(x, y)} L {pt(x + width, bottom)} L {pt(x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="square",
            stroke_linejoin="miter",
        ),
        path(
            f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
            f"M {pt(b_x + stroke * 0.15, y)} L {pt(b_x + b_width * 0.64, y)} "
            f"L {pt(b_x + b_width, y + height * 0.22)} L {pt(b_x + b_width, y + height * 0.42)} "
            f"L {pt(b_x + b_width * 0.66, y + height * 0.5)} "
            f"M {pt(b_x, y + height * 0.5)} L {pt(b_x + b_width * 0.66, y + height * 0.5)} "
            f"L {pt(b_x + b_width, y + height * 0.58)} L {pt(b_x + b_width, y + height * 0.78)} "
            f"L {pt(b_x + b_width * 0.64, bottom)} L {pt(b_x + stroke * 0.15, bottom)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="square",
            stroke_linejoin="miter",
        ),
        path(
            f"M {pt(n2_x, bottom)} L {pt(n2_x, y)} L {pt(n2_x + width, bottom)} L {pt(n2_x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="square",
            stroke_linejoin="miter",
        ),
    ]
    return group(elements)


def build_orbit_wordmark(x: float, y: float, height: float, color: str) -> str:
    scale = height / 140
    stroke = 22 * scale
    width = 110 * scale
    gap = 34 * scale
    b_width = 128 * scale
    bottom = y + height
    n2_x = x + width + gap + b_width + gap
    b_x = x + width + gap
    elements = [
        path(
            f"M {pt(x, bottom)} L {pt(x, y)} L {pt(x + width, bottom)} L {pt(x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
            f"M {pt(b_x + stroke * 0.5, y)} L {pt(b_x + b_width * 0.62, y)} "
            f"Q {pt(b_x + b_width, y)} {pt(b_x + b_width, y + height * 0.25)} "
            f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width * 0.62, y + height * 0.5)} "
            f"L {pt(b_x + stroke * 0.5, y + height * 0.5)} "
            f"M {pt(b_x + stroke * 0.5, y + height * 0.5)} L {pt(b_x + b_width * 0.62, y + height * 0.5)} "
            f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width, y + height * 0.75)} "
            f"Q {pt(b_x + b_width, bottom)} {pt(b_x + b_width * 0.62, bottom)} "
            f"L {pt(b_x + stroke * 0.5, bottom)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(n2_x, bottom)} L {pt(n2_x, y)} L {pt(n2_x + width, bottom)} L {pt(n2_x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
    ]
    return group(elements)


def build_wordmark(concept: Concept) -> str:
    height = 232
    if concept.wordmark_style == "region":
        return build_region_wordmark(concept.wordmark_x, concept.wordmark_y, height, concept.ink)
    if concept.wordmark_style == "shard":
        return build_shard_wordmark(concept.wordmark_x, concept.wordmark_y, height, concept.ink)
    return build_orbit_wordmark(concept.wordmark_x, concept.wordmark_y, height, concept.ink)


def render_png(svg_path: Path, png_path: Path, width: int) -> None:
    subprocess.run(
        ["node", str(NODE_RENDERER), str(svg_path), str(png_path), str(width)],
        check=True,
        cwd=ROOT_DIR,
    )


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/trebucbd.ttf" if bold else "C:/Windows/Fonts/trebuc.ttf",
        "C:/Windows/Fonts/seguisb.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size=size)
    return ImageFont.load_default()


def build_boundary_lock_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(512, 512, 278, 34, 332, concept.ink, 88, cap="butt"),
        rect(216, 412, 74, 212, rx="18", fill=concept.accent),
        rect(754, 354, 74, 258, rx="18", fill=concept.accent),
        path(
            "M 286,604 L 404,484 L 536,484 L 650,548 L 734,548",
            stroke=concept.ink,
            stroke_width="52",
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        line(514, 428, 514, 594, stroke=concept.accent2, stroke_width="22", stroke_linecap="round"),
        circle(286, 602, 20, fill=concept.accent2),
        circle(734, 548, 20, fill=concept.accent2),
    ]
    return group(elements)


def build_tick_index_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(512, 512, 284, -14, 54, concept.accent, 84, cap="butt"),
        open_ring_segment(512, 512, 284, 88, 160, concept.ink, 84, cap="butt"),
        open_ring_segment(512, 512, 284, 190, 252, concept.accent, 84, cap="butt"),
        open_ring_segment(512, 512, 284, 280, 336, concept.ink, 84, cap="butt"),
        line(344, 664, 650, 358, stroke=concept.ink, stroke_width="44", stroke_linecap="round"),
        rect(650, 314, 38, 78, rx="8", fill=concept.accent2, transform="rotate(45 669 353)"),
        rect(244, 472, 34, 80, rx="8", fill=concept.accent, transform="rotate(-90 261 512)"),
        rect(476, 772, 34, 86, rx="8", fill=concept.accent2, transform="rotate(0 493 815)"),
        circle(512, 512, 88, stroke=concept.ink, stroke_width="28"),
    ]
    return group(elements)


def build_sharded_halo_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(488, 498, 292, -16, 78, concept.ink, 96, cap="butt"),
        open_ring_segment(530, 520, 262, 116, 214, concept.accent, 96, cap="butt"),
        open_ring_segment(492, 534, 282, 232, 336, concept.ink, 96, cap="butt"),
        circle(456, 498, 92, stroke=concept.ink, stroke_width="28"),
        rect(438, 480, 36, 36, rx="8", fill=concept.accent2, transform="rotate(45 456 498)"),
        line(548, 280, 612, 210, stroke=concept.accent2, stroke_width="18", stroke_linecap="round"),
    ]
    return group(elements)


def build_region_seal_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(512, 512, 292, 18, 344, concept.ink, 88, cap="butt"),
        rect(478, 184, 68, 118, rx="12", fill="white"),
        rect(768, 478, 118, 68, rx="12", fill="white"),
        rect(478, 722, 68, 118, rx="12", fill="white"),
        rect(140, 478, 118, 68, rx="12", fill="white"),
        path(
            "M 274,512 C 386,512 400,388 510,388 C 632,388 638,640 754,640",
            stroke=concept.accent,
            stroke_width="48",
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        circle(274, 512, 18, fill=concept.accent2),
        circle(754, 640, 18, fill=concept.accent2),
        circle(512, 512, 74, stroke=concept.ink, stroke_width="22"),
    ]
    return group(elements)


def build_fracture_grid_icon(concept: Concept) -> str:
    outer = diamond(512, 512, 334)
    elements = [
        polygon(outer, stroke=concept.ink, stroke_width="36"),
        polygon(parallelogram_along_line(384, 256, 660, 532, 58), fill=concept.accent),
        polygon(parallelogram_along_line(660, 532, 512, 680, 58), fill=concept.accent),
        line(258, 520, 388, 390, stroke=concept.ink, stroke_width="30", stroke_linecap="square"),
        line(388, 390, 510, 512, stroke=concept.ink, stroke_width="30", stroke_linecap="square"),
        line(388, 646, 512, 522, stroke=concept.accent2, stroke_width="22", stroke_linecap="square"),
        polygon(diamond(512, 522, 28), fill=concept.ink),
    ]
    return group(elements)


def build_shard_nexus_icon(concept: Concept) -> str:
    elements = [
        polygon(diamond(512, 512, 336), stroke=concept.ink, stroke_width="34"),
        polygon(parallelogram_along_line(336, 640, 336, 368, 52), fill=concept.ink),
        polygon(parallelogram_along_line(336, 368, 690, 722, 52), fill=concept.accent),
        polygon(parallelogram_along_line(690, 722, 690, 332, 52), fill=concept.ink),
        polygon(parallelogram_along_line(236, 512, 336, 412, 18), fill=concept.accent2),
        polygon(parallelogram_along_line(690, 332, 786, 236, 18), fill=concept.accent2),
        rect(338, 368, 26, 26, fill="white", transform="rotate(45 351 381)"),
        rect(676, 710, 26, 26, fill="white", transform="rotate(45 689 723)"),
    ]
    return group(elements)


def build_phase_shift_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(474, 530, 316, 196, 354, concept.accent, 70, cap="round"),
        open_ring_segment(560, 472, 246, 24, 214, concept.ink, 82, cap="round"),
        open_ring_segment(508, 520, 152, 198, 348, concept.ink, 40, cap="round"),
        line(500, 520, 768, 288, stroke=concept.ink, stroke_width="22", stroke_linecap="round"),
        circle(500, 520, 28, fill=concept.accent2),
        circle(770, 286, 26, fill=concept.accent, stroke="white", stroke_width="12"),
    ]
    return group(elements)


def build_eclipse_relay_icon(concept: Concept) -> str:
    elements = [
        open_ring_segment(512, 512, 302, -26, 136, concept.ink, 74, cap="round"),
        open_ring_segment(512, 512, 302, 168, 328, concept.accent, 74, cap="round"),
        open_ring_segment(458, 554, 170, 208, 16, concept.ink, 34, cap="round"),
        circle(458, 554, 88, stroke=concept.ink, stroke_width="24"),
        polygon(parallelogram_along_line(420, 594, 658, 356, 24), fill=concept.accent2),
        circle(656, 354, 24, fill=concept.accent2),
    ]
    return group(elements)


def overlay_none(_: Concept) -> str:
    return ""


def overlay_boundary_lock(concept: Concept) -> str:
    return ""


def overlay_tick_index(concept: Concept) -> str:
    return group(
        [
            line(360, 396, 440, 316, stroke=concept.accent, stroke_width="22", stroke_linecap="round"),
            rect(432, 304, 26, 54, rx="6", fill=concept.accent2, transform="rotate(45 445 331)"),
        ]
    )


def overlay_sharded_halo(concept: Concept) -> str:
    return group(
        [
            open_ring_segment(448, 444, 116, 230, 344, concept.accent, 18, cap="butt"),
            open_ring_segment(448, 444, 144, 12, 94, concept.ink, 18, cap="butt"),
        ]
    )


def overlay_region_seal(concept: Concept) -> str:
    return group(
        [
            line(402, 470, 486, 470, stroke=concept.accent, stroke_width="18", stroke_linecap="round"),
            line(486, 470, 552, 412, stroke=concept.accent2, stroke_width="18", stroke_linecap="round"),
        ]
    )


def overlay_fracture_grid(concept: Concept) -> str:
    return ""


def overlay_shard_nexus(concept: Concept) -> str:
    return group(
        [
            polygon(diamond(402, 452, 34), fill=concept.accent, stroke=concept.ink, stroke_width="12"),
            line(434, 420, 518, 504, stroke=concept.ink, stroke_width="18", stroke_linecap="square"),
        ]
    )


def overlay_phase_shift(concept: Concept) -> str:
    return group(
        [
            open_ring_segment(430, 430, 124, 208, 346, concept.accent, 18, cap="round"),
            circle(546, 380, 13, fill=concept.accent2),
        ]
    )


def overlay_eclipse_relay(concept: Concept) -> str:
    return group(
        [
            open_ring_segment(472, 446, 122, 212, 18, concept.accent, 18, cap="round"),
            line(470, 446, 560, 356, stroke=concept.accent2, stroke_width="14", stroke_linecap="round"),
        ]
    )


CONCEPTS = [
    Concept(
        slug="boundary-lock",
        title="Boundary Lock",
        family="Region Loop",
        summary="A hard regional boundary interrupted by explicit gates and a single route through the system.",
        ink="#111827",
        accent="#E56B2F",
        accent2="#F7B267",
        wordmark_style="region",
        icon_builder=build_boundary_lock_icon,
        logo_overlay_builder=overlay_boundary_lock,
        symbol_scale=0.295,
        symbol_x=84,
        symbol_y=86,
        wordmark_x=392,
        wordmark_y=192,
    ),
    Concept(
        slug="tick-index",
        title="Tick Indexed Ring",
        family="Region Loop",
        summary="Time-sliced boundary geometry with uneven notches and a deterministic route across the field.",
        ink="#111827",
        accent="#D49A1D",
        accent2="#FF6B35",
        wordmark_style="region",
        icon_builder=build_tick_index_icon,
        logo_overlay_builder=overlay_tick_index,
        symbol_scale=0.295,
        symbol_x=82,
        symbol_y=88,
        wordmark_x=392,
        wordmark_y=192,
    ),
    Concept(
        slug="sharded-halo",
        title="Sharded Halo",
        family="Region Loop",
        summary="One system held together by unequal regional plates, with tension created by an offset core.",
        ink="#101827",
        accent="#0F8A7B",
        accent2="#FF6B35",
        wordmark_style="region",
        icon_builder=build_sharded_halo_icon,
        logo_overlay_builder=overlay_sharded_halo,
        symbol_scale=0.292,
        symbol_x=84,
        symbol_y=86,
        wordmark_x=398,
        wordmark_y=192,
    ),
    Concept(
        slug="region-seal",
        title="Region Seal",
        family="Region Loop",
        summary="A stamped deterministic emblem: one sealed boundary with carved slots and a controlled path.",
        ink="#111827",
        accent="#0E7490",
        accent2="#F97316",
        wordmark_style="region",
        icon_builder=build_region_seal_icon,
        logo_overlay_builder=overlay_region_seal,
        symbol_scale=0.292,
        symbol_x=84,
        symbol_y=88,
        wordmark_x=392,
        wordmark_y=192,
    ),
    Concept(
        slug="fracture-grid",
        title="Fracture Grid",
        family="Shard Lattice",
        summary="A severe lattice reduced to a single structural seam and a few load-bearing joints.",
        ink="#102038",
        accent="#F06A3D",
        accent2="#4FB3BF",
        wordmark_style="shard",
        icon_builder=build_fracture_grid_icon,
        logo_overlay_builder=overlay_fracture_grid,
        symbol_scale=0.285,
        symbol_x=82,
        symbol_y=84,
        wordmark_x=366,
        wordmark_y=190,
    ),
    Concept(
        slug="shard-nexus",
        title="Shard Nexus",
        family="Shard Lattice",
        summary="A monolithic shard with an internal N-like structural path instead of an illustrative network diagram.",
        ink="#102038",
        accent="#2AA8A1",
        accent2="#F59E0B",
        wordmark_style="shard",
        icon_builder=build_shard_nexus_icon,
        logo_overlay_builder=overlay_shard_nexus,
        symbol_scale=0.286,
        symbol_x=82,
        symbol_y=84,
        wordmark_x=374,
        wordmark_y=190,
    ),
    Concept(
        slug="phase-shift",
        title="Phase Shift",
        family="Tick Orbit",
        summary="Off-center timing geometry with delayed rings and a live orbit that never resolves into symmetry.",
        ink="#10213A",
        accent="#11B5E4",
        accent2="#FF595E",
        wordmark_style="orbit",
        icon_builder=build_phase_shift_icon,
        logo_overlay_builder=overlay_phase_shift,
        symbol_scale=0.286,
        symbol_x=82,
        symbol_y=86,
        wordmark_x=390,
        wordmark_y=192,
    ),
    Concept(
        slug="eclipse-relay",
        title="Eclipse Relay",
        family="Tick Orbit",
        summary="A two-phase orbital relay where one path eclipses the other and carries the signal onward.",
        ink="#10213A",
        accent="#12A4D9",
        accent2="#FF5A5F",
        wordmark_style="orbit",
        icon_builder=build_eclipse_relay_icon,
        logo_overlay_builder=overlay_eclipse_relay,
        symbol_scale=0.286,
        symbol_x=82,
        symbol_y=86,
        wordmark_x=392,
        wordmark_y=192,
    ),
]


def render_icon_svg(concept: Concept) -> str:
    return svg_doc(ICON_SIZE, ICON_SIZE, [concept.icon_builder(concept)])


def render_logo_svg(concept: Concept) -> str:
    elements = [
        rect(0, 0, LOGO_WIDTH, LOGO_HEIGHT, fill="white"),
        group(
            [concept.icon_builder(concept)],
            transform=f"translate({concept.symbol_x} {concept.symbol_y}) scale({fmt(concept.symbol_scale)})",
        ),
        build_wordmark(concept),
    ]
    overlay = concept.logo_overlay_builder(concept)
    if overlay:
        elements.append(overlay)
    return svg_doc(LOGO_WIDTH, LOGO_HEIGHT, elements)


def save_svg(path_value: Path, content: str) -> None:
    path_value.write_text(content, encoding="utf-8")


def add_shadow(base: Image.Image, x: int, y: int, width: int, height: int, radius: int) -> None:
    draw = ImageDraw.Draw(base)
    for offset, alpha in ((18, 20), (10, 28), (5, 36)):
        draw.rounded_rectangle(
            (x, y + offset, x + width, y + height + offset),
            radius=radius,
            fill=(15, 25, 38, alpha),
        )


def board_text(draw: ImageDraw.ImageDraw, x: int, y: int, text: str, font: ImageFont.ImageFont, fill: str, width: int) -> None:
    wrapped = textwrap.fill(text, width=width)
    draw.text((x, y), wrapped, fill=fill, font=font, spacing=6)


def build_board() -> None:
    board = Image.new("RGBA", (2860, 2120), "#F3EEE6")
    draw = ImageDraw.Draw(board)
    title_font = load_font(66, bold=True)
    heading_font = load_font(30, bold=True)
    body_font = load_font(22)
    micro_font = load_font(18, bold=True)

    draw.text((120, 64), "NBN logo exploration round 2", fill="#15202C", font=title_font)
    draw.text(
        (122, 142),
        "Second-generation concepts with harder geometry, fewer gestures, and tighter symbol-wordmark integration.",
        fill="#4B5864",
        font=body_font,
    )

    card_width = 620
    card_height = 880
    start_x = 120
    start_y = 230
    gap_x = 40
    gap_y = 44

    for index, concept in enumerate(CONCEPTS):
        row = index // 4
        col = index % 4
        x = start_x + col * (card_width + gap_x)
        y = start_y + row * (card_height + gap_y)
        add_shadow(board, x, y, card_width, card_height, 36)
        draw.rounded_rectangle((x, y, x + card_width, y + card_height), radius=36, fill="white")
        draw.rounded_rectangle((x, y, x + card_width, y + 18), radius=36, fill=concept.ink)
        draw.rectangle((x, y + 18, x + card_width, y + 36), fill=concept.ink)

        draw.text((x + 34, y + 56), concept.title, fill="#12202C", font=heading_font)
        draw.text((x + 34, y + 98), concept.family, fill=concept.accent, font=body_font)
        board_text(draw, x + 34, y + 138, concept.summary, body_font, "#5A6672", 42)

        icon_path = PNG_DIR / f"nbn-{concept.slug}-icon.png"
        logo_path = PNG_DIR / f"nbn-{concept.slug}-logo.png"
        icon_image = Image.open(icon_path).convert("RGBA")
        logo_image = Image.open(logo_path).convert("RGBA")

        icon_large = icon_image.resize((220, 220), Image.LANCZOS)
        board.alpha_composite(icon_large, (x + 40, y + 258))

        logo_scale = min(540 / logo_image.width, 180 / logo_image.height)
        logo_size = (int(logo_image.width * logo_scale), int(logo_image.height * logo_scale))
        logo_resized = logo_image.resize(logo_size, Image.LANCZOS)
        board.alpha_composite(logo_resized, (x + 40, y + 508))

        draw.text((x + 42, y + 724), "64px", fill="#8693A0", font=micro_font)
        draw.text((x + 160, y + 724), "32px", fill="#8693A0", font=micro_font)
        draw.text((x + 258, y + 724), "16px", fill="#8693A0", font=micro_font)

        for offset_x, size in ((38, 64), (154, 32), (250, 16)):
            micro = icon_image.resize((size, size), Image.LANCZOS)
            board.alpha_composite(micro, (x + offset_x, y + 760))

    board.save(PNG_DIR / "nbn-round2-board.png")


def build_shortlist_sheet() -> None:
    board = Image.new("RGBA", (2200, 1120), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    shortlist = ["boundary-lock", "fracture-grid", "phase-shift", "tick-index"]

    draw.text((96, 56), "Shortlist test sheet", fill="#14202C", font=title_font)
    draw.text((98, 124), "Large logo plus icon-size behavior for the most promising branches.", fill="#55626E", font=body_font)

    for index, slug in enumerate(shortlist):
        concept = next(item for item in CONCEPTS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 26, y + 26), concept.title, fill="#12202C", font=label_font)
        draw.text((x + 26, y + 66), concept.family, fill=concept.accent, font=body_font)

        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(410 / logo.width, 140 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        logo_resized = logo.resize(logo_size, Image.LANCZOS)
        board.alpha_composite(logo_resized, (x + 24, y + 120))

        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px, py in (("96", 96, x + 30, y + 340), ("48", 48, x + 166, y + 364), ("24", 24, x + 274, y + 388), ("16", 16, x + 352, y + 396)):
            draw.text((px, y + 300), f"{label}px", fill="#7A8794", font=body_font)
            resized = icon.resize((size, size), Image.LANCZOS)
            board.alpha_composite(resized, (px, py))

    board.save(PNG_DIR / "nbn-round2-shortlist.png")


def main() -> None:
    SVG_DIR.mkdir(parents=True, exist_ok=True)
    PNG_DIR.mkdir(parents=True, exist_ok=True)

    for concept in CONCEPTS:
        icon_svg = render_icon_svg(concept)
        logo_svg = render_logo_svg(concept)
        icon_svg_path = SVG_DIR / f"nbn-{concept.slug}-icon.svg"
        logo_svg_path = SVG_DIR / f"nbn-{concept.slug}-logo.svg"
        save_svg(icon_svg_path, icon_svg)
        save_svg(logo_svg_path, logo_svg)
        render_png(icon_svg_path, PNG_DIR / f"nbn-{concept.slug}-icon.png", ICON_SIZE)
        render_png(logo_svg_path, PNG_DIR / f"nbn-{concept.slug}-logo.png", LOGO_WIDTH)

    build_board()
    build_shortlist_sheet()


if __name__ == "__main__":
    main()
