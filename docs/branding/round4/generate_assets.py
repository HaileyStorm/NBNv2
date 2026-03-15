from __future__ import annotations

import math
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

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


def open_ring(
    cx: float,
    cy: float,
    radius: float,
    start_deg: float,
    end_deg: float,
    stroke: str,
    width: float,
    cap: str = "round",
) -> str:
    return path(
        arc_path(cx, cy, radius, start_deg, end_deg),
        stroke=stroke,
        stroke_width=fmt(width),
        stroke_linecap=cap,
    )


def transform_points(
    points: list[tuple[float, float]],
    *,
    dx: float = 0,
    dy: float = 0,
    sx: float = 1.0,
    sy: float = 1.0,
    cx: float = 512,
    cy: float = 512,
) -> list[tuple[float, float]]:
    transformed: list[tuple[float, float]] = []
    for x, y in points:
        tx = cx + (x - cx) * sx + dx
        ty = cy + (y - cy) * sy + dy
        transformed.append((tx, ty))
    return transformed


@dataclass(frozen=True)
class Variant:
    slug: str
    title: str
    family: Literal["Loop Modernized", "Segmented Diamond"]
    intensity: Literal["micro", "light", "small"]
    tweak: str
    summary: str
    params: dict[str, float]
    ink: str = "#131B2C"
    orange: str = "#F97316"
    teal: str = "#1B8393"
    gold: str = "#E0A31A"
    symbol_scale: float = 0.246
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


def build_loop_icon(variant: Variant) -> str:
    p = variant.params
    rotate = p.get("rotate", 0)
    expand = p.get("expand", 0)
    route_raise = p.get("route_raise", 0)
    right_shift = p.get("right_shift", 0)
    right_drop = p.get("right_drop", 0)
    eye_dx = p.get("eye_dx", 0)
    eye_dy = p.get("eye_dy", 0)
    eye_delta = p.get("eye_delta", 0)
    node_delta = p.get("node_delta", 0)
    path_width = 46 + p.get("path_delta", 0)
    ring_width = 70 + p.get("ring_delta", 0)
    segment_scale = 1 + p.get("segment_scale", 0)

    base_segments = [
        (208, 248, variant.gold),
        (266, 316, variant.ink),
        (334, 24, variant.gold),
        (40, 92, variant.ink),
        (112, 154, variant.gold),
        (170, 198, variant.ink),
    ]

    elements: list[str] = []
    for start, end, color in base_segments:
        mid = (start + end) / 2
        span = (end - start) % 360
        new_span = span * segment_scale + expand
        new_start = mid - new_span / 2 + rotate
        new_end = mid + new_span / 2 + rotate
        elements.append(open_ring(512, 512, 286, new_start, new_end, color, ring_width))

    left_node = (248, 528 + route_raise / 2)
    right_node = (710 + right_shift, 590 + right_drop + route_raise / 2)
    eye_x = 512 + eye_dx
    eye_y = 512 + eye_dy
    path_d = (
        f"M {pt(left_node[0], left_node[1])} "
        f"C {pt(350, 448 + route_raise)} {pt(430 + eye_dx / 2, 392 + route_raise)} {pt(eye_x, 432 + route_raise / 2)} "
        f"C {pt(586 + right_shift / 3, 466 + route_raise / 2)} {pt(608 + right_shift / 2, 590 + right_drop)} {pt(*right_node)}"
    )
    elements.extend(
        [
            path(
                path_d,
                stroke=variant.teal,
                stroke_width=fmt(path_width),
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(eye_x, eye_y, 78 + eye_delta, stroke=variant.ink, stroke_width="20"),
            circle(left_node[0], left_node[1], 16 + node_delta, fill=variant.orange),
            circle(right_node[0], right_node[1], 16 + node_delta, fill=variant.orange),
        ]
    )
    return group(elements)


def build_segmented_diamond_icon(variant: Variant) -> str:
    p = variant.params
    side_dx = p.get("side_dx", 0)
    top_dy = p.get("top_dy", 0)
    bottom_dy = p.get("bottom_dy", 0)
    overall_scale = 1 + p.get("overall_scale", 0)
    eye_delta = p.get("eye_delta", 0)
    eye_dx = p.get("eye_dx", 0)
    eye_dy = p.get("eye_dy", 0)
    wave_raise = p.get("wave_raise", 0)
    wave_flat = p.get("wave_flat", 0)
    left_shift = p.get("left_shift", 0)
    right_shift = p.get("right_shift", 0)

    top_dark = [(512, 188), (636, 312), (596, 352), (512, 268), (428, 352), (388, 312)]
    right_gold = [(836, 512), (712, 636), (672, 596), (756, 512), (672, 428), (712, 388)]
    bottom_dark = [(512, 836), (388, 712), (428, 672), (512, 756), (596, 672), (636, 712)]
    left_gold = [(188, 512), (312, 388), (352, 428), (268, 512), (352, 596), (312, 636)]

    top_points = transform_points(top_dark, dy=top_dy, sx=overall_scale, sy=overall_scale)
    bottom_points = transform_points(bottom_dark, dy=bottom_dy, sx=overall_scale, sy=overall_scale)
    left_points = transform_points(left_gold, dx=left_shift + side_dx, sx=overall_scale, sy=overall_scale)
    right_points = transform_points(right_gold, dx=right_shift - side_dx, sx=overall_scale, sy=overall_scale)

    eye_x = 512 + eye_dx
    eye_y = 512 + eye_dy
    path_d = (
        f"M {pt(278 + left_shift, 512 + wave_raise)} "
        f"C {pt(380 + left_shift / 2, 428 + wave_raise - wave_flat)} {pt(470, 432 + wave_raise)} {pt(eye_x, 512 + wave_raise / 2)} "
        f"C {pt(560, 602 + wave_flat + wave_raise / 2)} {pt(650 + right_shift / 2, 596 + wave_raise)} {pt(750 + right_shift, 512 + wave_raise)}"
    )
    return group(
        [
            polygon(top_points, fill=variant.ink),
            polygon(right_points, fill=variant.gold),
            polygon(bottom_points, fill=variant.ink),
            polygon(left_points, fill=variant.gold),
            path(
                path_d,
                stroke=variant.teal,
                stroke_width="44",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(eye_x, eye_y, 72 + eye_delta, stroke=variant.ink, stroke_width="18"),
        ]
    )


LOOP_VARIANTS = [
    Variant(
        slug="loop-tight-gap",
        title="Loop Tight Gap",
        family="Loop Modernized",
        intensity="micro",
        tweak="tighter segment gaps",
        summary="Keeps the original layout but closes the outer ring gaps slightly.",
        params={"expand": 6},
    ),
    Variant(
        slug="loop-even-flow",
        title="Loop Even Flow",
        family="Loop Modernized",
        intensity="micro",
        tweak="flatter route, smaller nodes",
        summary="Reduces route drama and node weight without changing the overall composition.",
        params={"path_delta": -4, "node_delta": -2, "route_raise": 4},
    ),
    Variant(
        slug="loop-wide-core",
        title="Loop Wide Core",
        family="Loop Modernized",
        intensity="light",
        tweak="larger eye, gentler path",
        summary="Opens the central core and calms the curve around it.",
        params={"eye_delta": 10, "wave_flat": 18},
    ),
    Variant(
        slug="loop-right-pull",
        title="Loop Right Pull",
        family="Loop Modernized",
        intensity="light",
        tweak="relay endpoint pulled forward",
        summary="Keeps the same route but gives the right side more directional tension.",
        params={"right_shift": 26, "right_drop": -8, "route_raise": -6},
    ),
    Variant(
        slug="loop-offset-core",
        title="Loop Offset Core",
        family="Loop Modernized",
        intensity="small",
        tweak="eye shifted right, route lifted",
        summary="A small but real adjustment that creates more bias inside the same layout.",
        params={"eye_dx": 18, "route_raise": -10, "right_shift": 12},
    ),
]


DIAMOND_VARIANTS = [
    Variant(
        slug="diamond-tight",
        title="Diamond Tight",
        family="Segmented Diamond",
        intensity="micro",
        tweak="segments slightly closer",
        summary="Tightens the diamond segments without changing the basic silhouette.",
        params={"side_dx": -12, "top_dy": 8, "bottom_dy": -8},
    ),
    Variant(
        slug="diamond-open",
        title="Diamond Open",
        family="Segmented Diamond",
        intensity="micro",
        tweak="wider gaps around the core",
        summary="Creates more air around the center while keeping the same segmented frame.",
        params={"side_dx": 16, "top_dy": -10, "bottom_dy": 10},
    ),
    Variant(
        slug="diamond-wide-gate",
        title="Diamond Wide Gate",
        family="Segmented Diamond",
        intensity="light",
        tweak="gold side gates widened",
        summary="Pushes the side segments outward to emphasize the lateral gate feel.",
        params={"left_shift": -20, "right_shift": 20, "overall_scale": 0.02},
    ),
    Variant(
        slug="diamond-flat-wave",
        title="Diamond Flat Wave",
        family="Segmented Diamond",
        intensity="light",
        tweak="flatter internal route",
        summary="Keeps the segmented frame but makes the inner wave calmer and more infrastructural.",
        params={"wave_flat": 26, "wave_raise": -4},
    ),
    Variant(
        slug="diamond-core-shift",
        title="Diamond Core Shift",
        family="Segmented Diamond",
        intensity="small",
        tweak="eye shifted left and down",
        summary="A subtle core displacement that changes the balance while keeping the same structure.",
        params={"eye_dx": -16, "eye_dy": 10, "wave_raise": 8, "left_shift": -8, "right_shift": 8},
    ),
]


VARIANTS = LOOP_VARIANTS + DIAMOND_VARIANTS


def render_icon_svg(variant: Variant) -> str:
    builder = build_loop_icon if variant.family == "Loop Modernized" else build_segmented_diamond_icon
    return svg_doc(ICON_SIZE, ICON_SIZE, [builder(variant)])


def render_logo_svg(variant: Variant) -> str:
    builder = build_loop_icon if variant.family == "Loop Modernized" else build_segmented_diamond_icon
    return svg_doc(
        LOGO_WIDTH,
        LOGO_HEIGHT,
        [
            rect(0, 0, LOGO_WIDTH, LOGO_HEIGHT, fill="white"),
            group([builder(variant)], transform=f"translate({variant.symbol_x} {variant.symbol_y}) scale({fmt(variant.symbol_scale)})"),
            build_wordmark(variant.wordmark_x, variant.wordmark_y, 210, variant.ink),
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
    for offset, alpha in ((18, 20), (10, 28), (5, 36)):
        draw.rounded_rectangle(
            (x, y + offset, x + width, y + height + offset),
            radius=radius,
            fill=(14, 23, 36, alpha),
        )


def build_family_board(family: Literal["Loop Modernized", "Segmented Diamond"], variants: list[Variant], output_name: str) -> None:
    board = Image.new("RGBA", (3060, 980), "#F5F0E8")
    draw = ImageDraw.Draw(board)
    title_font = load_font(62, bold=True)
    heading_font = load_font(26, bold=True)
    body_font = load_font(20)
    micro_font = load_font(18, bold=True)

    draw.text((84, 44), family, fill="#17212D", font=title_font)
    draw.text(
        (86, 112),
        "Five small-variation options ranging from micro spacing changes to slightly stronger layout bias.",
        fill="#56636F",
        font=body_font,
    )

    card_w = 560
    card_h = 760
    start_x = 84
    gap_x = 24
    y = 180

    for index, variant in enumerate(variants):
        x = start_x + index * (card_w + gap_x)
        add_shadow(board, x, y, card_w, card_h, 30)
        draw.rounded_rectangle((x, y, x + card_w, y + card_h), radius=30, fill="white")
        draw.rounded_rectangle((x, y, x + card_w, y + 16), radius=30, fill="#17212D")
        draw.rectangle((x, y + 16, x + card_w, y + 32), fill="#17212D")
        draw.text((x + 24, y + 48), variant.title, fill="#17212D", font=heading_font)
        draw.text((x + 24, y + 82), variant.intensity, fill=variant.orange if variant.family == "Loop Modernized" else variant.teal, font=body_font)
        draw.text((x + 24, y + 110), variant.tweak, fill="#6A7885", font=body_font)
        wrapped = textwrap.fill(variant.summary, width=38)
        draw.text((x + 24, y + 144), wrapped, fill="#5B6874", font=body_font, spacing=5)

        icon = Image.open(PNG_DIR / f"nbn-{variant.slug}-icon.png").convert("RGBA")
        board.alpha_composite(icon.resize((186, 186), Image.LANCZOS), (x + 22, y + 246))

        logo = Image.open(PNG_DIR / f"nbn-{variant.slug}-logo.png").convert("RGBA")
        logo_scale = min(510 / logo.width, 118 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 18, y + 458))

        draw.text((x + 24, y + 598), "64px", fill="#8894A0", font=micro_font)
        draw.text((x + 124, y + 598), "24px", fill="#8894A0", font=micro_font)
        draw.text((x + 208, y + 598), "16px", fill="#8894A0", font=micro_font)
        for px, size in ((20, 64), (116, 24), (198, 16)):
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 628))

    board.save(PNG_DIR / output_name)


def build_shortlist() -> None:
    shortlist = [
        "loop-even-flow",
        "loop-offset-core",
        "diamond-open",
        "diamond-wide-gate",
    ]
    board = Image.new("RGBA", (2200, 1100), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    draw.text((96, 56), "Round 4 shortlist", fill="#17212D", font=title_font)
    draw.text((98, 124), "The most promising small refinements from the two source marks.", fill="#56636F", font=body_font)

    for index, slug in enumerate(shortlist):
        variant = next(item for item in VARIANTS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 24, y + 24), variant.title, fill="#17212D", font=label_font)
        draw.text((x + 24, y + 60), variant.tweak, fill="#6A7885", font=body_font)
        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(420 / logo.width, 114 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 20, y + 110))
        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px in (("96", 96, 30), ("48", 48, 174), ("24", 24, 294), ("16", 16, 376)):
            draw.text((x + px, y + 286), f"{label}px", fill="#86929E", font=body_font)
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 330))

    board.save(PNG_DIR / "nbn-round4-shortlist.png")


def main() -> None:
    SVG_DIR.mkdir(parents=True, exist_ok=True)
    PNG_DIR.mkdir(parents=True, exist_ok=True)

    for variant in VARIANTS:
        icon_svg = render_icon_svg(variant)
        logo_svg = render_logo_svg(variant)
        icon_svg_path = SVG_DIR / f"nbn-{variant.slug}-icon.svg"
        logo_svg_path = SVG_DIR / f"nbn-{variant.slug}-logo.svg"
        save_svg(icon_svg_path, icon_svg)
        save_svg(logo_svg_path, logo_svg)
        render_png(icon_svg_path, PNG_DIR / f"nbn-{variant.slug}-icon.png", ICON_SIZE)
        render_png(logo_svg_path, PNG_DIR / f"nbn-{variant.slug}-logo.png", LOGO_WIDTH)

    build_family_board("Loop Modernized", LOOP_VARIANTS, "nbn-round4-loop-board.png")
    build_family_board("Segmented Diamond", DIAMOND_VARIANTS, "nbn-round4-diamond-board.png")
    build_shortlist()


if __name__ == "__main__":
    main()
