from __future__ import annotations

import math
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from PIL import Image, ImageDraw, ImageFont


BASE_DIR = Path(__file__).resolve().parent
SVG_DIR = BASE_DIR / "svg"
PNG_DIR = BASE_DIR / "png"
NODE_RENDERER = BASE_DIR / "render_png.mjs"
ICON_SIZE = 1024
LOGO_WIDTH = 1800
LOGO_HEIGHT = 640


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
    return f'<g {attrs(**kwargs)}>\n' + "\n".join(elements) + "\n</g>"


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


def hexagon(cx: float, cy: float, rx: float, ry: float) -> list[tuple[float, float]]:
    return [
        (cx, cy - ry),
        (cx + rx, cy - ry / 2),
        (cx + rx, cy + ry / 2),
        (cx, cy + ry),
        (cx - rx, cy + ry / 2),
        (cx - rx, cy - ry / 2),
    ]


@dataclass(frozen=True)
class Concept:
    slug: str
    title: str
    summary: str
    primary: str
    secondary: str
    accent: str
    ink: str
    tint: str
    icon_builder: Callable[["Concept"], str]
    accent_builder: Callable[["Concept", float, float], str]


def build_tick_orbit_icon(concept: Concept) -> str:
    outer_points = [
        (512 + 290 * math.cos(math.radians(angle)), 512 + 290 * math.sin(math.radians(angle)))
        for angle in (-38, 40, 120, 210)
    ]
    elements = [
        circle(512, 512, 348, fill=concept.tint),
        path(
            arc_path(512, 512, 290, -35, 222),
            stroke=concept.primary,
            stroke_width="46",
            stroke_linecap="round",
        ),
        path(
            arc_path(512, 512, 204, 142, 394),
            stroke=concept.secondary,
            stroke_width="24",
            stroke_linecap="round",
            opacity="0.95",
        ),
        line(
            326,
            714,
            704,
            322,
            stroke=concept.primary,
            stroke_width="34",
            stroke_linecap="round",
        ),
        polygon(diamond(512, 512, 98), fill="white", stroke=concept.primary, stroke_width="18"),
        circle(512, 512, 22, fill=concept.accent),
    ]
    for index, (x, y) in enumerate(outer_points):
        fill = concept.accent if index == 0 else concept.secondary
        radius = 28 if index == 0 else 22
        elements.append(circle(x, y, radius, fill=fill, stroke="white", stroke_width="12"))
    elements.append(
        line(
            outer_points[0][0] + 32,
            outer_points[0][1] - 40,
            outer_points[0][0] + 92,
            outer_points[0][1] - 108,
            stroke=concept.accent,
            stroke_width="18",
            stroke_linecap="round",
        )
    )
    return group(elements)


def build_region_loop_icon(concept: Concept) -> str:
    elements = [rect(156, 156, 712, 712, rx="240", fill=concept.tint)]
    for index in range(8):
        start = index * 45 - 8
        end = start + 28
        color = concept.primary if index % 2 == 0 else concept.secondary
        if index in (0, 4):
            color = concept.accent
        elements.append(
            path(
                arc_path(512, 512, 260, start, end),
                stroke=color,
                stroke_width="54",
                stroke_linecap="round",
            )
        )
    elements.extend(
        [
            path(
                "M 188,512 C 292,512 314,386 422,386 "
                "C 514,386 540,650 640,650 C 724,650 780,584 838,512",
                stroke=concept.primary,
                stroke_width="34",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(188, 512, 34, fill=concept.secondary, stroke="white", stroke_width="14"),
            circle(838, 512, 34, fill=concept.accent, stroke="white", stroke_width="14"),
            circle(512, 512, 86, fill="white", stroke=concept.primary, stroke_width="22"),
            path(
                arc_path(512, 512, 116, 210, 332),
                stroke=concept.accent,
                stroke_width="18",
                stroke_linecap="round",
            ),
        ]
    )
    return group(elements)


def build_axon_bridge_icon(concept: Concept) -> str:
    elements = [
        circle(512, 512, 350, fill=concept.tint),
        rect(214, 248, 82, 528, rx="41", fill=concept.primary),
        rect(728, 248, 82, 528, rx="41", fill=concept.primary),
        path(
            "M 296,364 C 422,214 612,214 728,364",
            stroke=concept.secondary,
            stroke_width="32",
            stroke_linecap="round",
        ),
        path(
            "M 296,512 C 412,622 612,622 728,512",
            stroke=concept.accent,
            stroke_width="34",
            stroke_linecap="round",
        ),
        path(
            "M 296,660 C 428,816 608,816 728,660",
            stroke=concept.primary,
            stroke_width="24",
            stroke_linecap="round",
            opacity="0.85",
        ),
        circle(512, 512, 90, fill="white", stroke=concept.ink, stroke_width="18"),
        path(
            "M 468,512 L 512,468 L 556,512 L 512,556 Z",
            fill=concept.secondary,
            stroke=concept.primary,
            stroke_width="12",
        ),
        circle(392, 272, 20, fill=concept.secondary),
        circle(624, 752, 20, fill=concept.accent),
    ]
    return group(elements)


def build_shard_lattice_icon(concept: Concept) -> str:
    nodes = [
        (512, 228),
        (396, 344),
        (628, 344),
        (280, 460),
        (512, 460),
        (744, 460),
        (396, 576),
        (628, 576),
        (512, 692),
    ]
    links = [
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (2, 4),
        (2, 5),
        (3, 6),
        (4, 6),
        (4, 7),
        (5, 7),
        (6, 8),
        (7, 8),
    ]
    route = [0, 1, 4, 7, 8]
    elements = [
        polygon(diamond(512, 512, 348), fill=concept.tint),
        polygon(diamond(512, 512, 320), stroke=concept.primary, stroke_width="18"),
    ]
    for start_index, end_index in links:
        x1, y1 = nodes[start_index]
        x2, y2 = nodes[end_index]
        elements.append(
            line(
                x1,
                y1,
                x2,
                y2,
                stroke=concept.secondary,
                stroke_width="16",
                stroke_linecap="round",
                opacity="0.78",
            )
        )
    route_points = " ".join(pt(*nodes[index]) for index in route)
    elements.append(
        f'<polyline points="{route_points}" fill="none" stroke="{concept.accent}" '
        'stroke-width="28" stroke-linecap="round" stroke-linejoin="round" />'
    )
    for index, (x, y) in enumerate(nodes):
        fill = concept.accent if index in route else "white"
        elements.append(
            polygon(
                diamond(x, y, 28 if index in route else 22),
                fill=fill,
                stroke=concept.primary,
                stroke_width="12",
            )
        )
    return group(elements)


def build_snapshot_crystal_icon(concept: Concept) -> str:
    outer = hexagon(512, 512, 262, 324)
    middle = hexagon(512, 492, 220, 270)
    inner = hexagon(512, 468, 162, 208)
    elements = [
        rect(150, 150, 724, 724, rx="210", fill=concept.tint),
        polygon(outer, fill=concept.secondary, opacity="0.16", stroke=concept.primary, stroke_width="18"),
        polygon(middle, fill=concept.accent, opacity="0.12", stroke=concept.primary, stroke_width="16"),
        polygon(inner, fill="white", stroke=concept.primary, stroke_width="16"),
        line(512, 232, 512, 760, stroke=concept.primary, stroke_width="26", stroke_linecap="round"),
        line(350, 378, 674, 558, stroke=concept.secondary, stroke_width="18", stroke_linecap="round"),
        line(674, 378, 350, 558, stroke=concept.accent, stroke_width="18", stroke_linecap="round"),
        circle(512, 232, 22, fill=concept.accent),
        circle(512, 760, 22, fill=concept.secondary),
        polygon(diamond(512, 496, 54), fill=concept.primary),
    ]
    return group(elements)


def build_observatory_halo_icon(concept: Concept) -> str:
    elements = [
        circle(512, 512, 346, fill=concept.tint),
        circle(512, 512, 98, fill="white", stroke=concept.primary, stroke_width="24"),
        path(
            arc_path(512, 512, 172, -28, 164),
            stroke=concept.primary,
            stroke_width="30",
            stroke_linecap="round",
        ),
        path(
            arc_path(512, 512, 250, 28, 248),
            stroke=concept.secondary,
            stroke_width="24",
            stroke_linecap="round",
        ),
        path(
            arc_path(512, 512, 312, 214, 398),
            stroke=concept.accent,
            stroke_width="20",
            stroke_linecap="round",
        ),
        line(512, 512, 740, 318, stroke=concept.primary, stroke_width="18", stroke_linecap="round"),
        circle(740, 318, 28, fill=concept.accent, stroke="white", stroke_width="10"),
        circle(350, 635, 18, fill=concept.secondary),
        circle(612, 744, 18, fill=concept.primary),
        path(
            arc_path(512, 512, 118, 236, 302),
            stroke=concept.accent,
            stroke_width="14",
            stroke_linecap="round",
        ),
    ]
    return group(elements)


def build_wordmark(x: float, y: float, height: float, color: str) -> str:
    scale = height / 120
    stroke = 18 * scale
    first_n_x = x
    b_x = x + 142 * scale
    second_n_x = x + 310 * scale
    width = 92 * scale
    bottom = y + height
    midpoint = y + height / 2
    elements = [
        path(
            f"M {pt(first_n_x, bottom)} L {pt(first_n_x, y)} "
            f"L {pt(first_n_x + width, bottom)} L {pt(first_n_x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
            f"M {pt(b_x, y)} L {pt(b_x + 58 * scale, y)} "
            f"Q {pt(b_x + 92 * scale, y)} {pt(b_x + 92 * scale, y + 30 * scale)} "
            f"Q {pt(b_x + 92 * scale, midpoint)} {pt(b_x + 58 * scale, midpoint)} "
            f"L {pt(b_x, midpoint)} "
            f"M {pt(b_x, midpoint)} L {pt(b_x + 58 * scale, midpoint)} "
            f"Q {pt(b_x + 92 * scale, midpoint)} {pt(b_x + 92 * scale, y + 90 * scale)} "
            f"Q {pt(b_x + 92 * scale, bottom)} {pt(b_x + 58 * scale, bottom)} "
            f"L {pt(b_x, bottom)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        path(
            f"M {pt(second_n_x, bottom)} L {pt(second_n_x, y)} "
            f"L {pt(second_n_x + width, bottom)} L {pt(second_n_x + width, y)}",
            stroke=color,
            stroke_width=fmt(stroke),
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
    ]
    return group(elements)


def accent_tick_orbit(concept: Concept, x: float, y: float) -> str:
    return group(
        [
            path(
                arc_path(x, y, 118, 198, 350),
                stroke=concept.secondary,
                stroke_width="14",
                stroke_linecap="round",
            ),
            circle(x + 116, y - 8, 13, fill=concept.accent),
        ]
    )


def accent_region_loop(concept: Concept, x: float, y: float) -> str:
    pieces = []
    for index in range(6):
        pieces.append(
            rect(
                x + index * 48,
                y - 10,
                34,
                20,
                rx="10",
                fill=concept.secondary if index % 2 else concept.primary,
            )
        )
    pieces.append(circle(x + 310, y, 10, fill=concept.accent))
    return group(pieces)


def accent_axon_bridge(concept: Concept, x: float, y: float) -> str:
    return group(
        [
            path(
                f"M {pt(x, y)} C {pt(x + 90, y - 34)} {pt(x + 194, y + 34)} {pt(x + 300, y)}",
                stroke=concept.secondary,
                stroke_width="14",
                stroke_linecap="round",
            ),
            circle(x - 6, y, 10, fill=concept.primary),
            circle(x + 306, y, 10, fill=concept.accent),
        ]
    )


def accent_shard_lattice(concept: Concept, x: float, y: float) -> str:
    pieces = [
        line(x + 28, y, x + 94, y, stroke=concept.secondary, stroke_width="10", stroke_linecap="round"),
        line(x + 134, y, x + 200, y, stroke=concept.secondary, stroke_width="10", stroke_linecap="round"),
        line(x + 240, y, x + 306, y, stroke=concept.secondary, stroke_width="10", stroke_linecap="round"),
    ]
    for offset, fill in ((0, "white"), (106, concept.accent), (212, "white"), (318, concept.accent)):
        pieces.append(
            polygon(diamond(x + offset, y, 14), fill=fill, stroke=concept.primary, stroke_width="8")
        )
    return group(pieces)


def accent_snapshot_crystal(concept: Concept, x: float, y: float) -> str:
    return group(
        [
            path(
                f"M {pt(x, y + 18)} L {pt(x + 48, y - 18)} L {pt(x + 96, y + 18)} "
                f"L {pt(x + 144, y - 18)} L {pt(x + 192, y + 18)}",
                stroke=concept.primary,
                stroke_width="14",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            path(
                f"M {pt(x + 18, y + 34)} L {pt(x + 174, y + 34)}",
                stroke=concept.accent,
                stroke_width="8",
                stroke_linecap="round",
            ),
        ]
    )


def accent_observatory_halo(concept: Concept, x: float, y: float) -> str:
    return group(
        [
            path(
                arc_path(x + 100, y, 68, 205, 354),
                stroke=concept.secondary,
                stroke_width="12",
                stroke_linecap="round",
            ),
            path(
                arc_path(x + 100, y, 104, 214, 330),
                stroke=concept.primary,
                stroke_width="10",
                stroke_linecap="round",
            ),
            circle(x + 204, y - 8, 10, fill=concept.accent),
        ]
    )


CONCEPTS = [
    Concept(
        slug="tick-orbit",
        title="Tick Orbit",
        summary="Global cadence, stable routing, and a disciplined pulse around a shared core.",
        primary="#103B73",
        secondary="#2BB6C4",
        accent="#FF6B4A",
        ink="#102132",
        tint="#EAF7F8",
        icon_builder=build_tick_orbit_icon,
        accent_builder=accent_tick_orbit,
    ),
    Concept(
        slug="region-loop",
        title="Region Loop",
        summary="A split-brain atlas with explicit ingress, egress, and region-level structure.",
        primary="#7A3E2B",
        secondary="#F2B544",
        accent="#2D6A73",
        ink="#2C1912",
        tint="#FFF4E8",
        icon_builder=build_region_loop_icon,
        accent_builder=accent_region_loop,
    ),
    Concept(
        slug="axon-bridge",
        title="Axon Bridge",
        summary="Directional relay paths between poles, emphasizing IO and signal handoff.",
        primary="#19427A",
        secondary="#22A38F",
        accent="#F25F5C",
        ink="#112133",
        tint="#ECF7F3",
        icon_builder=build_axon_bridge_icon,
        accent_builder=accent_axon_bridge,
    ),
    Concept(
        slug="shard-lattice",
        title="Shard Lattice",
        summary="Distributed compute cells arranged as a structured, deterministic neural mesh.",
        primary="#1D3557",
        secondary="#3AAFA9",
        accent="#E76F51",
        ink="#132238",
        tint="#EEF4F6",
        icon_builder=build_shard_lattice_icon,
        accent_builder=accent_shard_lattice,
    ),
    Concept(
        slug="snapshot-crystal",
        title="Snapshot Crystal",
        summary="Layered artifact geometry for state capture, replay, and deterministic recovery.",
        primary="#1E3A8A",
        secondary="#38BDF8",
        accent="#F4A261",
        ink="#111827",
        tint="#EEF4FF",
        icon_builder=build_snapshot_crystal_icon,
        accent_builder=accent_snapshot_crystal,
    ),
    Concept(
        slug="observatory-halo",
        title="Observatory Halo",
        summary="Signals, metrics, and debugging treated as first-class parts of the identity.",
        primary="#0B3954",
        secondary="#087E8B",
        accent="#FF5A5F",
        ink="#10212E",
        tint="#EEF7F6",
        icon_builder=build_observatory_halo_icon,
        accent_builder=accent_observatory_halo,
    ),
]


def render_icon_svg(concept: Concept) -> str:
    return svg_doc(ICON_SIZE, ICON_SIZE, [concept.icon_builder(concept)])


def render_logo_svg(concept: Concept) -> str:
    icon = concept.icon_builder(concept)
    elements = [
        rect(80, 72, LOGO_WIDTH - 160, LOGO_HEIGHT - 144, rx="72", fill="white"),
        group([icon], transform="translate(96 82) scale(0.34)"),
        build_wordmark(470, 188, 204, concept.ink),
        concept.accent_builder(concept, 566, 476),
        path(
            f"M {pt(468, 512)} L {pt(1184, 512)}",
            stroke=concept.primary,
            stroke_width="6",
            stroke_linecap="round",
            opacity="0.18",
        ),
    ]
    return svg_doc(LOGO_WIDTH, LOGO_HEIGHT, elements)


def save_svg(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def render_png(svg_path: Path, png_path: Path, width: int) -> None:
    subprocess.run(
        ["node", str(NODE_RENDERER), str(svg_path), str(png_path), str(width)],
        check=True,
        cwd=BASE_DIR,
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
    for offset, alpha in ((20, 22), (12, 32), (6, 46)):
        draw.rounded_rectangle(
            (x, y + offset, x + width, y + height + offset),
            radius=radius,
            fill=(16, 28, 40, alpha),
        )


def build_board() -> None:
    board = Image.new("RGBA", (2520, 1820), "#F2EEE7")
    draw = ImageDraw.Draw(board)
    title_font = load_font(64, bold=True)
    heading_font = load_font(34, bold=True)
    body_font = load_font(24)

    draw.text((120, 68), "NBN logo exploration", fill="#14202B", font=title_font)
    draw.text(
        (122, 142),
        "Six finalists selected from broader brainstorming around distributed brains, tick cadence, routing, and observability.",
        fill="#4C5A67",
        font=body_font,
    )

    card_width = 720
    card_height = 720
    start_x = 120
    start_y = 240
    gap_x = 60
    gap_y = 58

    for index, concept in enumerate(CONCEPTS):
        row = index // 3
        col = index % 3
        x = start_x + col * (card_width + gap_x)
        y = start_y + row * (card_height + gap_y)
        add_shadow(board, x, y, card_width, card_height, 40)
        draw.rounded_rectangle((x, y, x + card_width, y + card_height), radius=40, fill="white")
        draw.rounded_rectangle((x, y, x + card_width, y + 16), radius=40, fill=concept.primary)
        draw.rectangle((x, y + 16, x + card_width, y + 40), fill=concept.primary)
        draw.text((x + 42, y + 54), concept.title, fill=concept.ink, font=heading_font)

        wrapped = textwrap.fill(concept.summary, width=50)
        draw.text((x + 42, y + 106), wrapped, fill="#55626E", font=body_font, spacing=6)

        icon_image = Image.open(PNG_DIR / f"nbn-{concept.slug}-icon.png").convert("RGBA")
        icon_resized = icon_image.resize((220, 220), Image.LANCZOS)
        board.alpha_composite(icon_resized, (x + 42, y + 232))

        logo_image = Image.open(PNG_DIR / f"nbn-{concept.slug}-logo.png").convert("RGBA")
        scale = min(580 / logo_image.width, 220 / logo_image.height)
        logo_size = (int(logo_image.width * scale), int(logo_image.height * scale))
        logo_resized = logo_image.resize(logo_size, Image.LANCZOS)
        board.alpha_composite(logo_resized, (x + 88, y + 430))

    board.save(PNG_DIR / "nbn-concept-board.png")


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
        render_png(icon_svg_path, PNG_DIR / f"nbn-{concept.slug}-icon.png", width=ICON_SIZE)
        render_png(logo_svg_path, PNG_DIR / f"nbn-{concept.slug}-logo.png", width=LOGO_WIDTH)

    build_board()


if __name__ == "__main__":
    main()
