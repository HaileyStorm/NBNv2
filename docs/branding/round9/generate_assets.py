from __future__ import annotations

import subprocess
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
    import math

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


def open_ring(cx: float, cy: float, radius: float, start_deg: float, end_deg: float, stroke: str, width: float, cap: str = "round") -> str:
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
        transformed.append((cx + (x - cx) * sx + dx, cy + (y - cy) * sy + dy))
    return transformed


@dataclass(frozen=True)
class Variant:
    slug: str
    title: str
    kind: Literal["loop", "reference"]
    tweak: str
    note: str
    stroke_mult: float = 1.0
    n_width_mult: float = 1.0
    b_width_mult: float = 1.0
    n1_color: str = "#131B2C"
    b_color: str = "#131B2C"
    n2_color: str = "#131B2C"
    ink: str = "#131B2C"
    orange: str = "#F97316"
    teal: str = "#1B8393"
    gold: str = "#E0A31A"
    symbol_scale: float = 0.246
    symbol_x: int = 62
    symbol_y: int = 46
    wordmark_x: int = 314
    wordmark_y: int = 122


BRIDGE_SOFT_ROUTE = {
    "left_shift": 0,
    "left_drop": 0,
    "right_shift": -8,
    "right_drop": 14,
    "route_raise": -2,
    "cp1_dx": 0,
    "cp1_dy": 0,
    "cp2_dx": 0,
    "cp2_dy": 18,
    "cp3_dx": 8,
    "cp3_dy": 6,
    "cp4_dx": 12,
    "cp4_dy": -16,
    "eye_dx": 0,
    "eye_dy": 0,
    "path_width": 42,
}


def build_wordmark(variant: Variant) -> str:
    x = variant.wordmark_x
    y = variant.wordmark_y
    height = 210
    scale = height / 140
    stroke = 22 * scale * variant.stroke_mult
    n_width = 110 * scale * variant.n_width_mult
    gap = 34 * scale
    b_width = 124 * scale * variant.b_width_mult
    bottom = y + height
    b_x = x + n_width + gap
    n2_x = b_x + b_width + gap

    left_n = path(
        f"M {pt(x, bottom)} L {pt(x, y)} L {pt(x + n_width, bottom)} L {pt(x + n_width, y)}",
        stroke=variant.n1_color,
        stroke_width=fmt(stroke),
        stroke_linecap="round",
        stroke_linejoin="round",
    )
    b = path(
        f"M {pt(b_x, y)} L {pt(b_x, bottom)} "
        f"M {pt(b_x + stroke * 0.4, y)} L {pt(b_x + b_width * 0.62, y)} "
        f"Q {pt(b_x + b_width, y)} {pt(b_x + b_width, y + height * 0.25)} "
        f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width * 0.62, y + height * 0.5)} "
        f"L {pt(b_x + stroke * 0.4, y + height * 0.5)} "
        f"M {pt(b_x + stroke * 0.4, y + height * 0.5)} L {pt(b_x + b_width * 0.62, y + height * 0.5)} "
        f"Q {pt(b_x + b_width, y + height * 0.5)} {pt(b_x + b_width, y + height * 0.75)} "
        f"Q {pt(b_x + b_width, bottom)} {pt(b_x + b_width * 0.62, bottom)} "
        f"L {pt(b_x + stroke * 0.4, bottom)}",
        stroke=variant.b_color,
        stroke_width=fmt(stroke),
        stroke_linecap="round",
        stroke_linejoin="round",
    )
    right_n = path(
        f"M {pt(n2_x, bottom)} L {pt(n2_x, y)} L {pt(n2_x + n_width, bottom)} L {pt(n2_x + n_width, y)}",
        stroke=variant.n2_color,
        stroke_width=fmt(stroke),
        stroke_linecap="round",
        stroke_linejoin="round",
    )
    return group([left_n, b, right_n])


def build_bridge_soft_symbol(variant: Variant) -> str:
    p = BRIDGE_SOFT_ROUTE
    base_rotate = 55
    left_node = (248 + p["left_shift"], 522 + p["left_drop"] + p["route_raise"] / 2)
    right_node = (722 + p["right_shift"], 603 + p["right_drop"] + p["route_raise"] / 2)
    eye_x = 512 + p["eye_dx"]
    eye_y = 512 + p["eye_dy"]
    cp1 = (350 + p["cp1_dx"], 436 + p["route_raise"] + p["cp1_dy"])
    cp2 = (439 + p["cp2_dx"], 380 + p["route_raise"] + p["cp2_dy"])
    cp_mid = (512 + p["cp3_dx"], 426 + p["route_raise"] / 2 + p["cp3_dy"])
    cp3 = (590 + p["cp4_dx"], 459 + p["route_raise"] / 2 + p["cp4_dy"])
    cp4 = (614 + p["right_shift"] / 2, 590 + p["right_drop"])

    base_segments = [
        (208, 248, variant.gold),
        (266, 316, variant.ink),
        (40, 92, variant.ink),
        (112, 154, variant.gold),
        (170, 198, variant.ink),
    ]
    elements: list[str] = []
    for start, end, color in base_segments:
        elements.append(open_ring(512, 512, 286, start + base_rotate, end + base_rotate, color, 70))

    path_d = (
        f"M {pt(*left_node)} "
        f"C {pt(*cp1)} {pt(*cp2)} {pt(*cp_mid)} "
        f"C {pt(*cp3)} {pt(*cp4)} {pt(*right_node)}"
    )
    elements.extend(
        [
            path(
                path_d,
                stroke=variant.teal,
                stroke_width=fmt(p["path_width"]),
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(eye_x, eye_y, 78, stroke=variant.ink, stroke_width="20"),
            circle(left_node[0], left_node[1], 16, fill=variant.orange),
            circle(right_node[0], right_node[1], 16, fill=variant.orange),
        ]
    )
    return group(elements)


def build_diamond_wide_gate_reference(variant: Variant) -> str:
    top_dark = [(512, 188), (636, 312), (596, 352), (512, 268), (428, 352), (388, 312)]
    right_gold = [(836, 512), (712, 636), (672, 596), (756, 512), (672, 428), (712, 388)]
    bottom_dark = [(512, 836), (388, 712), (428, 672), (512, 756), (596, 672), (636, 712)]
    left_gold = [(188, 512), (312, 388), (352, 428), (268, 512), (352, 596), (312, 636)]
    return group(
        [
            polygon(transform_points(top_dark), fill=variant.ink),
            polygon(transform_points(right_gold, dx=-20), fill=variant.gold),
            polygon(transform_points(bottom_dark), fill=variant.ink),
            polygon(transform_points(left_gold, dx=-20), fill=variant.gold),
            path(
                "M 258,512 C 370,428 470,432 512,512 C 560,602 660,596 770,512",
                stroke=variant.teal,
                stroke_width="44",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(512, 512, 72, stroke=variant.ink, stroke_width="18"),
        ]
    )


VARIANTS = [
    Variant(
        slug="bridge-soft-pin",
        title="Bridge Soft",
        kind="loop",
        tweak="baseline route",
        note="Original monocolor wordmark retained only as a baseline for the narrow-B color studies.",
    ),
    Variant(
        slug="bridge-soft-narrow-b-90",
        title="Soft + Narrow B 90",
        kind="loop",
        tweak="10% narrower B",
        note="A first narrow-B step without changing color.",
        b_width_mult=0.90,
    ),
    Variant(
        slug="bridge-soft-narrow-b-80",
        title="Soft + Narrow B 80",
        kind="loop",
        tweak="20% narrower B",
        note="A more obvious narrow-B option while staying monocolor.",
        b_width_mult=0.80,
        stroke_mult=1.02,
    ),
    Variant(
        slug="bridge-soft-narrow-b-72",
        title="Soft + Narrow B 72",
        kind="loop",
        tweak="28% narrower B",
        note="Aggressively narrow B to test the edge of legibility and rhythm.",
        b_width_mult=0.72,
        stroke_mult=1.05,
    ),
    Variant(
        slug="bridge-soft-narrow-teal-b",
        title="Soft + Narrow Teal B",
        kind="loop",
        tweak="narrow teal B",
        note="Same narrower B, but using teal to make the center letter the accent.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        b_color="#1B8393",
    ),
    Variant(
        slug="bridge-soft-narrow-gold-b",
        title="Soft + Narrow Gold B",
        kind="loop",
        tweak="narrow gold B",
        note="Warmer center-letter emphasis using the ring accent color.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        b_color="#E0A31A",
    ),
    Variant(
        slug="bridge-soft-gold-right-n",
        title="Soft + Gold B + Gold N",
        kind="loop",
        tweak="gold B, gold right N",
        note="Extends the gold treatment from the narrow B into the right N as well.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        b_color="#E0A31A",
        n2_color="#E0A31A",
    ),
    Variant(
        slug="bridge-soft-teal-b-gold-n",
        title="Soft + Teal B + Gold N",
        kind="loop",
        tweak="teal B, gold right N",
        note="Keeps the B cool and pushes the warmer accent into the terminal right N.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        b_color="#1B8393",
        n2_color="#E0A31A",
    ),
    Variant(
        slug="diamond-wide-gate-candidate",
        title="Diamond Wide Gate",
        kind="reference",
        tweak="pinned candidate",
        note="Held as the non-loop comparison candidate.",
        symbol_scale=0.244,
    ),
]


def render_icon_svg(variant: Variant) -> str:
    builder = build_bridge_soft_symbol if variant.kind == "loop" else build_diamond_wide_gate_reference
    return svg_doc(ICON_SIZE, ICON_SIZE, [builder(variant)])


def render_logo_svg(variant: Variant) -> str:
    builder = build_bridge_soft_symbol if variant.kind == "loop" else build_diamond_wide_gate_reference
    return svg_doc(
        LOGO_WIDTH,
        LOGO_HEIGHT,
        [
            rect(0, 0, LOGO_WIDTH, LOGO_HEIGHT, fill="white"),
            group([builder(variant)], transform=f"translate({variant.symbol_x} {variant.symbol_y}) scale({fmt(variant.symbol_scale)})"),
            build_wordmark(variant),
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
        draw.rounded_rectangle((x, y + offset, x + width, y + height + offset), radius=radius, fill=(14, 23, 36, alpha))


def build_board() -> None:
    board = Image.new("RGBA", (2460, 2430), "#F5F0E8")
    draw = ImageDraw.Draw(board)
    title_font = load_font(62, bold=True)
    heading_font = load_font(24, bold=True)
    body_font = load_font(19)
    micro_font = load_font(18, bold=True)

    draw.text((84, 44), "Bridge Soft text study", fill="#17212D", font=title_font)
    draw.text((86, 112), "Bridge Soft stays on the board only as a baseline. This pass focuses on narrower Bs, a gold right N, and a teal-plus-gold split.", fill="#56636F", font=body_font)

    card_w = 560
    card_h = 700
    start_x = 84
    gap_x = 24
    start_y = 180
    gap_y = 26

    for index, variant in enumerate(VARIANTS):
        row = index // 4
        col = index % 4
        x = start_x + col * (card_w + gap_x)
        y = start_y + row * (card_h + gap_y)
        add_shadow(board, x, y, card_w, card_h, 30)
        draw.rounded_rectangle((x, y, x + card_w, y + card_h), radius=30, fill="white")
        draw.rounded_rectangle((x, y, x + card_w, y + 16), radius=30, fill="#17212D")
        draw.rectangle((x, y + 16, x + card_w, y + 32), fill="#17212D")
        draw.text((x + 24, y + 44), variant.title, fill="#17212D", font=heading_font)
        badge_color = variant.orange if variant.kind == "loop" else variant.gold
        draw.text((x + 24, y + 76), variant.tweak, fill=badge_color, font=body_font)
        draw.text((x + 24, y + 104), variant.note, fill="#5C6975", font=body_font)

        icon = Image.open(PNG_DIR / f"nbn-{variant.slug}-icon.png").convert("RGBA")
        board.alpha_composite(icon.resize((186, 186), Image.LANCZOS), (x + 22, y + 202))

        logo = Image.open(PNG_DIR / f"nbn-{variant.slug}-logo.png").convert("RGBA")
        logo_scale = min(510 / logo.width, 118 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 18, y + 414))

        draw.text((x + 24, y + 536), "64px", fill="#8894A0", font=micro_font)
        draw.text((x + 124, y + 536), "24px", fill="#8894A0", font=micro_font)
        draw.text((x + 208, y + 536), "16px", fill="#8894A0", font=micro_font)
        for px, size in ((20, 64), (116, 24), (198, 16)):
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 562))

    board.save(PNG_DIR / "nbn-round9-board.png")


def build_shortlist() -> None:
    shortlist = [
        "bridge-soft-narrow-gold-b",
        "bridge-soft-gold-right-n",
        "bridge-soft-teal-b-gold-n",
        "diamond-wide-gate-candidate",
    ]
    board = Image.new("RGBA", (2200, 1100), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    draw.text((96, 56), "Round 9 shortlist", fill="#17212D", font=title_font)
    draw.text((98, 124), "Narrow Gold B leads the loop direction, with two right-N color splits and Diamond Wide Gate for comparison.", fill="#56636F", font=body_font)

    for index, slug in enumerate(shortlist):
        variant = next(item for item in VARIANTS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 24, y + 24), variant.title, fill="#17212D", font=label_font)
        badge_color = variant.orange if variant.kind == "loop" else variant.gold
        draw.text((x + 24, y + 60), variant.tweak, fill=badge_color, font=body_font)
        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(420 / logo.width, 114 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 20, y + 110))
        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px in (("96", 96, 30), ("48", 48, 174), ("24", 24, 294), ("16", 16, 376)):
            draw.text((x + px, y + 286), f"{label}px", fill="#86929E", font=body_font)
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 330))

    board.save(PNG_DIR / "nbn-round9-shortlist.png")


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

    build_board()
    build_shortlist()


if __name__ == "__main__":
    main()
