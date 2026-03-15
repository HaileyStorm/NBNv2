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
LOGO_WIDTH = 1460
LOGO_HEIGHT = 520


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


def open_ring(cx: float, cy: float, radius: float, start_deg: float, end_deg: float, stroke: str, width: float, cap: str = "butt") -> str:
    return path(
        arc_path(cx, cy, radius, start_deg, end_deg),
        stroke=stroke,
        stroke_width=fmt(width),
        stroke_linecap=cap,
    )


def diamond(cx: float, cy: float, radius: float) -> list[tuple[float, float]]:
    return [
        (cx, cy - radius),
        (cx + radius, cy),
        (cx, cy + radius),
        (cx - radius, cy),
    ]


def parallelogram_along_line(x1: float, y1: float, x2: float, y2: float, width: float) -> list[tuple[float, float]]:
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
    band: str
    summary: str
    icon_builder: Callable[["Concept"], str]
    ink: str = "#131B2C"
    orange: str = "#F97316"
    teal: str = "#1B8393"
    gold: str = "#E0A31A"
    symbol_scale: float = 0.245
    symbol_x: int = 62
    symbol_y: int = 46
    wordmark_x: int = 314
    wordmark_y: int = 122


def build_wordmark(x: float, y: float, height: float, color: str) -> str:
    scale = height / 140
    stroke = 22 * scale
    width = 110 * scale
    gap = 34 * scale
    b_width = 124 * scale
    bottom = y + height
    b_x = x + width + gap
    n2_x = b_x + b_width + gap
    return group(
        [
            path(
                f"M {pt(x, bottom)} L {pt(x, y)} L {pt(x + width, bottom)} L {pt(x + width, y)}",
                stroke=color,
                stroke_width=fmt(stroke),
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            path(
                f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
                f"M {pt(b_x + stroke * 0.4, y)} L {pt(b_x + b_width * 0.62, y)} "
                f"Q {pt(b_x + b_width, y)} {pt(b_x + b_width, y + height * 0.25)} "
                f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width * 0.62, y + height * 0.5)} "
                f"L {pt(b_x + stroke * 0.4, y + height * 0.5)} "
                f"M {pt(b_x + stroke * 0.4, y + height * 0.5)} L {pt(b_x + b_width * 0.62, y + height * 0.5)} "
                f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width, y + height * 0.75)} "
                f"Q {pt(b_x + b_width, bottom)} {pt(b_x + b_width * 0.62, bottom)} "
                f"L {pt(b_x + stroke * 0.4, bottom)}",
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
    )


def build_seal_tightened(concept: Concept) -> str:
    return group(
        [
            open_ring(512, 512, 286, 206, 334, concept.ink, 82, "round"),
            open_ring(512, 512, 286, 6, 142, concept.ink, 82, "round"),
            path(
                "M 232,516 L 388,428 L 520,428 L 640,492 L 766,492",
                stroke=concept.teal,
                stroke_width="50",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(512, 512, 82, stroke=concept.ink, stroke_width="22"),
            circle(232, 516, 18, fill=concept.orange),
            circle(766, 486, 18, fill=concept.orange),
        ]
    )


def build_loop_modernized(concept: Concept) -> str:
    return group(
        [
            open_ring(512, 512, 286, 208, 248, concept.gold, 70, "round"),
            open_ring(512, 512, 286, 266, 316, concept.ink, 70, "round"),
            open_ring(512, 512, 286, 334, 24, concept.gold, 70, "round"),
            open_ring(512, 512, 286, 40, 92, concept.ink, 70, "round"),
            open_ring(512, 512, 286, 112, 154, concept.gold, 70, "round"),
            open_ring(512, 512, 286, 170, 198, concept.ink, 70, "round"),
            path(
                "M 248,528 C 350,448 430,392 512,432 C 586,466 608,590 710,590",
                stroke=concept.teal,
                stroke_width="46",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(512, 512, 78, stroke=concept.ink, stroke_width="20"),
            circle(248, 528, 16, fill=concept.orange),
            circle(710, 590, 16, fill=concept.orange),
        ]
    )


def build_lattice_simplified(concept: Concept) -> str:
    nodes = [(512, 232), (364, 380), (660, 380), (512, 528), (364, 676)]
    elements = [
        polygon(diamond(512, 512, 332), stroke=concept.ink, stroke_width="34"),
        line(512, 232, 364, 380, stroke=concept.teal, stroke_width="24", stroke_linecap="round"),
        line(364, 380, 512, 528, stroke=concept.teal, stroke_width="24", stroke_linecap="round"),
        line(512, 528, 364, 676, stroke=concept.teal, stroke_width="24", stroke_linecap="round"),
        polygon(parallelogram_along_line(426, 296, 660, 528, 56), fill=concept.orange),
    ]
    for index, (x, y) in enumerate(nodes):
        fill = concept.orange if index in (0, 3, 4) else "white"
        elements.append(polygon(diamond(x, y, 24), fill=fill, stroke=concept.ink, stroke_width="12"))
    return group(elements)


def build_loop_in_seal(concept: Concept) -> str:
    return group(
        [
            open_ring(512, 512, 290, 198, 334, concept.ink, 78, "round"),
            open_ring(512, 512, 290, 10, 166, concept.ink, 78, "round"),
            open_ring(512, 512, 290, 172, 188, concept.gold, 42, "round"),
            open_ring(512, 512, 290, 344, 358, concept.gold, 42, "round"),
            path(
                "M 222,524 C 330,426 414,396 498,438 C 566,470 590,590 794,550",
                stroke=concept.teal,
                stroke_width="48",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(512, 512, 76, stroke=concept.ink, stroke_width="20"),
            circle(222, 524, 18, fill=concept.orange),
            circle(794, 550, 18, fill=concept.orange),
        ]
    )


def build_shard_seal(concept: Concept) -> str:
    nodes = [(512, 368), (412, 468), (612, 468), (512, 568)]
    elements = [
        open_ring(512, 512, 290, 206, 338, concept.ink, 82, "round"),
        open_ring(512, 512, 290, 18, 148, concept.ink, 82, "round"),
        polygon(diamond(512, 468, 150), stroke=concept.teal, stroke_width="20"),
        line(512, 368, 412, 468, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        line(512, 368, 612, 468, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        line(412, 468, 512, 568, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        polygon(parallelogram_along_line(456, 412, 612, 568, 44), fill=concept.orange),
    ]
    for index, (x, y) in enumerate(nodes):
        fill = concept.orange if index in (0, 3) else "white"
        elements.append(polygon(diamond(x, y, 18), fill=fill, stroke=concept.ink, stroke_width="10"))
    return group(elements)


def build_ringed_lattice(concept: Concept) -> str:
    nodes = [(398, 396), (626, 396), (512, 512), (398, 628), (626, 628)]
    elements = [
        open_ring(512, 512, 304, 216, 316, concept.ink, 56, "round"),
        open_ring(512, 512, 304, 334, 82, concept.gold, 56, "round"),
        open_ring(512, 512, 304, 104, 196, concept.ink, 56, "round"),
        polygon(diamond(512, 512, 222), stroke=concept.ink, stroke_width="20"),
        line(398, 396, 512, 512, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        line(512, 512, 626, 628, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        line(626, 396, 512, 512, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        polygon(parallelogram_along_line(404, 618, 622, 400, 44), fill=concept.orange),
    ]
    for index, (x, y) in enumerate(nodes):
        fill = concept.orange if index in (1, 3) else "white"
        elements.append(polygon(diamond(x, y, 18), fill=fill, stroke=concept.ink, stroke_width="10"))
    return group(elements)


def build_segmented_diamond(concept: Concept) -> str:
    elements = [
        open_ring(512, 512, 0, 0, 0, concept.ink, 0),
        polygon([(512, 188), (636, 312), (596, 352), (512, 268), (428, 352), (388, 312)], fill=concept.ink),
        polygon([(836, 512), (712, 636), (672, 596), (756, 512), (672, 428), (712, 388)], fill=concept.gold),
        polygon([(512, 836), (388, 712), (428, 672), (512, 756), (596, 672), (636, 712)], fill=concept.ink),
        polygon([(188, 512), (312, 388), (352, 428), (268, 512), (352, 596), (312, 636)], fill=concept.gold),
        path(
            "M 278,512 C 380,428 470,432 512,512 C 560,602 650,596 750,512",
            stroke=concept.teal,
            stroke_width="44",
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        circle(512, 512, 72, stroke=concept.ink, stroke_width="18"),
    ]
    return group(elements)


def build_orbit_grid(concept: Concept) -> str:
    nodes = [(394, 402), (628, 402), (394, 628), (628, 628)]
    elements = [
        open_ring(512, 512, 286, 214, 274, concept.gold, 38, "round"),
        open_ring(512, 512, 286, 312, 20, concept.ink, 38, "round"),
        open_ring(512, 512, 286, 58, 112, concept.gold, 38, "round"),
        polygon(diamond(512, 512, 230), stroke=concept.ink, stroke_width="16"),
        line(394, 402, 628, 628, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        line(628, 402, 394, 628, stroke=concept.teal, stroke_width="18", stroke_linecap="round"),
        path(
            "M 310,572 C 414,458 474,438 532,464 C 592,490 640,590 736,586",
            stroke=concept.orange,
            stroke_width="34",
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
    ]
    for x, y in nodes:
        elements.append(circle(x, y, 18, fill="white", stroke=concept.ink, stroke_width="10"))
    return group(elements)


def build_seal_nexus(concept: Concept) -> str:
    nodes = [(362, 434), (512, 354), (662, 434), (512, 602)]
    elements = [
        open_ring(512, 512, 290, 200, 332, concept.ink, 82, "round"),
        open_ring(512, 512, 290, 14, 146, concept.ink, 82, "round"),
        open_ring(512, 512, 290, 160, 180, concept.gold, 42, "round"),
        open_ring(512, 512, 290, 346, 2, concept.gold, 42, "round"),
        line(362, 434, 512, 354, stroke=concept.teal, stroke_width="20", stroke_linecap="round"),
        line(512, 354, 662, 434, stroke=concept.teal, stroke_width="20", stroke_linecap="round"),
        line(512, 354, 512, 602, stroke=concept.teal, stroke_width="20", stroke_linecap="round"),
        polygon(parallelogram_along_line(358, 590, 662, 434, 46), fill=concept.orange),
    ]
    for index, (x, y) in enumerate(nodes):
        fill = concept.orange if index in (0, 2) else "white"
        elements.append(polygon(diamond(x, y, 18), fill=fill, stroke=concept.ink, stroke_width="10"))
    return group(elements)


CONCEPTS = [
    Concept(
        slug="seal-tightened",
        title="Seal Tightened",
        band="Conservative",
        summary="A cleaned-up seal: one strong boundary, one looped route, no extra garnish.",
        icon_builder=build_seal_tightened,
        symbol_scale=0.25,
    ),
    Concept(
        slug="loop-modernized",
        title="Loop Modernized",
        band="Conservative",
        summary="The old Region Loop reduced to cleaner ring segments, a single route, and a calmer center.",
        icon_builder=build_loop_modernized,
        symbol_scale=0.25,
    ),
    Concept(
        slug="lattice-simplified",
        title="Lattice Simplified",
        band="Conservative",
        summary="The old Shard Lattice collapsed to a sturdier frame and one clear diagonal route.",
        icon_builder=build_lattice_simplified,
        symbol_scale=0.245,
    ),
    Concept(
        slug="loop-in-seal",
        title="Loop In Seal",
        band="Moderate",
        summary="Region Seal outside, Region Loop inside: bounded system with a living internal route.",
        icon_builder=build_loop_in_seal,
        symbol_scale=0.248,
    ),
    Concept(
        slug="shard-seal",
        title="Shard Seal",
        band="Moderate",
        summary="A seal silhouette wrapped around a minimal shard lattice and one highlighted path.",
        icon_builder=build_shard_seal,
        symbol_scale=0.246,
    ),
    Concept(
        slug="ringed-lattice",
        title="Ringed Lattice",
        band="Moderate",
        summary="An open regional ring enclosing a sparse lattice rather than a dense technical diagram.",
        icon_builder=build_ringed_lattice,
        symbol_scale=0.246,
    ),
    Concept(
        slug="segmented-diamond",
        title="Segmented Diamond",
        band="Hybrid",
        summary="Shard silhouette outside, loop logic inside, with seal-style segmentation around the perimeter.",
        icon_builder=build_segmented_diamond,
        symbol_scale=0.25,
    ),
    Concept(
        slug="orbit-grid",
        title="Orbit Grid",
        band="Hybrid",
        summary="A sparse grid with a routed loop and partial boundary markers instead of one dominant enclosure.",
        icon_builder=build_orbit_grid,
        symbol_scale=0.25,
    ),
    Concept(
        slug="seal-nexus",
        title="Seal Nexus",
        band="Hybrid",
        summary="Open seal silhouette plus simplified shard structure plus one deliberate signal route.",
        icon_builder=build_seal_nexus,
        symbol_scale=0.246,
    ),
]


def render_icon_svg(concept: Concept) -> str:
    return svg_doc(ICON_SIZE, ICON_SIZE, [concept.icon_builder(concept)])


def render_logo_svg(concept: Concept) -> str:
    return svg_doc(
        LOGO_WIDTH,
        LOGO_HEIGHT,
        [
            rect(0, 0, LOGO_WIDTH, LOGO_HEIGHT, fill="white"),
            group(
                [concept.icon_builder(concept)],
                transform=f"translate({concept.symbol_x} {concept.symbol_y}) scale({fmt(concept.symbol_scale)})",
            ),
            build_wordmark(concept.wordmark_x, concept.wordmark_y, 210, concept.ink),
        ],
    )


def save_svg(path_value: Path, content: str) -> None:
    path_value.write_text(content, encoding="utf-8")


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


def add_shadow(base: Image.Image, x: int, y: int, width: int, height: int, radius: int) -> None:
    draw = ImageDraw.Draw(base)
    for offset, alpha in ((18, 20), (10, 28), (5, 38)):
        draw.rounded_rectangle(
            (x, y + offset, x + width, y + height + offset),
            radius=radius,
            fill=(14, 23, 36, alpha),
        )


def build_board() -> None:
    board = Image.new("RGBA", (2580, 2410), "#F4EFE7")
    draw = ImageDraw.Draw(board)
    title_font = load_font(64, bold=True)
    heading_font = load_font(30, bold=True)
    body_font = load_font(22)
    micro_font = load_font(18, bold=True)

    draw.text((120, 58), "NBN logo exploration round 3", fill="#17212D", font=title_font)
    draw.text(
        (122, 130),
        "Nine options built from Region Seal, old Region Loop, and old Shard Lattice: conservative through hybrid.",
        fill="#55626E",
        font=body_font,
    )

    card_w = 760
    card_h = 680
    start_x = 120
    start_y = 220
    gap_x = 30
    gap_y = 34

    for index, concept in enumerate(CONCEPTS):
        row = index // 3
        col = index % 3
        x = start_x + col * (card_w + gap_x)
        y = start_y + row * (card_h + gap_y)
        add_shadow(board, x, y, card_w, card_h, 34)
        draw.rounded_rectangle((x, y, x + card_w, y + card_h), radius=34, fill="white")
        draw.rounded_rectangle((x, y, x + card_w, y + 18), radius=34, fill="#17212D")
        draw.rectangle((x, y + 18, x + card_w, y + 34), fill="#17212D")
        draw.text((x + 28, y + 54), concept.title, fill="#17212D", font=heading_font)
        draw.text((x + 28, y + 92), concept.band, fill=concept.orange if concept.band != "Moderate" else concept.teal, font=body_font)
        wrapped = textwrap.fill(concept.summary, width=50)
        draw.text((x + 28, y + 128), wrapped, fill="#5C6975", font=body_font, spacing=5)

        icon = Image.open(PNG_DIR / f"nbn-{concept.slug}-icon.png").convert("RGBA")
        icon_large = icon.resize((182, 182), Image.LANCZOS)
        board.alpha_composite(icon_large, (x + 38, y + 232))

        logo = Image.open(PNG_DIR / f"nbn-{concept.slug}-logo.png").convert("RGBA")
        logo_scale = min(690 / logo.width, 154 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        logo_resized = logo.resize(logo_size, Image.LANCZOS)
        board.alpha_composite(logo_resized, (x + 22, y + 430))

        draw.text((x + 40, y + 586), "64px", fill="#8A96A2", font=micro_font)
        draw.text((x + 154, y + 586), "24px", fill="#8A96A2", font=micro_font)
        draw.text((x + 252, y + 586), "16px", fill="#8A96A2", font=micro_font)
        for px, size in ((36, 64), (146, 24), (242, 16)):
            micro = icon.resize((size, size), Image.LANCZOS)
            board.alpha_composite(micro, (x + px, y + 610))

    board.save(PNG_DIR / "nbn-round3-board.png")


def build_shortlist() -> None:
    shortlist = ["seal-tightened", "loop-in-seal", "shard-seal", "ringed-lattice"]
    board = Image.new("RGBA", (2200, 1100), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    draw.text((96, 56), "Round 3 shortlist", fill="#17212D", font=title_font)
    draw.text((98, 124), "The four strongest seal / loop / lattice directions after the initial synthesis.", fill="#56636F", font=body_font)

    for index, slug in enumerate(shortlist):
        concept = next(item for item in CONCEPTS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 26, y + 24), concept.title, fill="#17212D", font=label_font)
        draw.text((x + 26, y + 64), concept.band, fill=concept.orange if concept.band != "Moderate" else concept.teal, font=body_font)

        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(414 / logo.width, 120 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 24, y + 112))

        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px in (("96", 96, 30), ("48", 48, 174), ("24", 24, 294), ("16", 16, 376)):
            draw.text((x + px, y + 286), f"{label}px", fill="#85929E", font=body_font)
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 330))

    board.save(PNG_DIR / "nbn-round3-shortlist.png")


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
    build_shortlist()


if __name__ == "__main__":
    main()
