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


def build_loop_55(mark: Variant) -> str:
    p = mark.params
    base_rotate = 55
    left_shift = p.get("left_shift", 0)
    left_drop = p.get("left_drop", 0)
    right_shift = p.get("right_shift", -24)
    right_drop = p.get("right_drop", 36)
    route_raise = p.get("route_raise", -2)
    cp1_dx = p.get("cp1_dx", 0)
    cp1_dy = p.get("cp1_dy", 0)
    cp2_dx = p.get("cp2_dx", 0)
    cp2_dy = p.get("cp2_dy", 0)
    cp3_dx = p.get("cp3_dx", 0)
    cp3_dy = p.get("cp3_dy", 0)
    cp4_dx = p.get("cp4_dx", 0)
    cp4_dy = p.get("cp4_dy", 0)
    eye_dx = p.get("eye_dx", 0)
    eye_dy = p.get("eye_dy", 0)
    path_width = p.get("path_width", 46)

    base_segments = [
        (208, 248, mark.gold),
        (266, 316, mark.ink),
        (40, 92, mark.ink),
        (112, 154, mark.gold),
        (170, 198, mark.ink),
    ]
    elements: list[str] = []
    for start, end, color in base_segments:
        elements.append(open_ring(512, 512, 286, start + base_rotate, end + base_rotate, color, 70))

    left_node = (248 + left_shift, 522 + left_drop + route_raise / 2)
    right_node = (722 + right_shift, 603 + right_drop + route_raise / 2)
    eye_x = 512 + eye_dx
    eye_y = 512 + eye_dy

    cp1 = (350 + cp1_dx, 436 + route_raise + cp1_dy)
    cp2 = (439 + cp2_dx, 380 + route_raise + cp2_dy)
    cp_mid = (512 + cp3_dx, 426 + route_raise / 2 + cp3_dy)
    cp3 = (590 + cp4_dx, 459 + route_raise / 2 + cp4_dy)
    cp4 = (614 + right_shift / 2, 590 + right_drop)

    path_d = (
        f"M {pt(*left_node)} "
        f"C {pt(*cp1)} {pt(*cp2)} {pt(*cp_mid)} "
        f"C {pt(*cp3)} {pt(*cp4)} {pt(*right_node)}"
    )
    elements.extend(
        [
            path(
                path_d,
                stroke=mark.teal,
                stroke_width=fmt(path_width),
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(eye_x, eye_y, 78, stroke=mark.ink, stroke_width="20"),
            circle(left_node[0], left_node[1], 16, fill=mark.orange),
            circle(right_node[0], right_node[1], 16, fill=mark.orange),
        ]
    )
    return group(elements)


def build_diamond_wide_gate_reference(mark: Variant) -> str:
    top_dark = [(512, 188), (636, 312), (596, 352), (512, 268), (428, 352), (388, 312)]
    right_gold = [(836, 512), (712, 636), (672, 596), (756, 512), (672, 428), (712, 388)]
    bottom_dark = [(512, 836), (388, 712), (428, 672), (512, 756), (596, 672), (636, 712)]
    left_gold = [(188, 512), (312, 388), (352, 428), (268, 512), (352, 596), (312, 636)]
    return group(
        [
            polygon(transform_points(top_dark), fill=mark.ink),
            polygon(transform_points(right_gold, dx=-20), fill=mark.gold),
            polygon(transform_points(bottom_dark), fill=mark.ink),
            polygon(transform_points(left_gold, dx=-20), fill=mark.gold),
            path(
                "M 258,512 C 370,428 470,432 512,512 C 560,602 660,596 770,512",
                stroke=mark.teal,
                stroke_width="44",
                stroke_linecap="round",
                stroke_linejoin="round",
            ),
            circle(512, 512, 72, stroke=mark.ink, stroke_width="18"),
        ]
    )


VARIANTS = [
    Variant(
        slug="loop55-base",
        title="Rotate 55deg Base",
        kind="loop",
        tweak="pinned candidate",
        note="The corrected 55deg loop study, unchanged.",
        params={},
    ),
    Variant(
        slug="loop55-even-flow",
        title="55deg Even Flow",
        kind="loop",
        tweak="flatter route",
        note="Calms the route without moving the ring.",
        params={"cp2_dy": 24, "cp3_dy": 18, "cp4_dy": -12, "right_drop": 18, "path_width": 42},
    ),
    Variant(
        slug="loop55-exit-tuck",
        title="55deg Exit Tuck",
        kind="loop",
        tweak="more tucked exit",
        note="Pulls the right endpoint inward so it hugs the lower-right opening more tightly.",
        params={"right_shift": -56, "right_drop": 20, "cp4_dx": -26, "cp4_dy": -26, "cp3_dx": -14, "cp3_dy": 6},
    ),
    Variant(
        slug="loop55-exit-reach",
        title="55deg Exit Reach",
        kind="loop",
        tweak="longer reaching exit",
        note="Lets the route reach further toward the gap instead of curling inward early.",
        params={"right_shift": 10, "right_drop": 8, "cp4_dx": 30, "cp4_dy": -22, "cp3_dx": 20, "cp3_dy": -10},
    ),
    Variant(
        slug="loop55-high-crest",
        title="55deg High Crest",
        kind="loop",
        tweak="higher arc over eye",
        note="Raises the route before it descends, giving the middle more lift.",
        params={"cp1_dy": -32, "cp2_dy": -44, "cp3_dy": -22, "cp4_dy": -6, "route_raise": -14},
    ),
    Variant(
        slug="loop55-direct-relay",
        title="55deg Direct Relay",
        kind="loop",
        tweak="straighter relay",
        note="Removes some of the curl so the route feels more infrastructural and less ribbon-like.",
        params={"cp1_dx": 36, "cp2_dx": 42, "cp2_dy": 18, "cp3_dx": 32, "cp3_dy": 18, "cp4_dx": -10, "cp4_dy": 8, "right_drop": 24, "path_width": 42},
    ),
    Variant(
        slug="diamond-wide-gate-candidate",
        title="Diamond Wide Gate",
        kind="reference",
        tweak="pinned candidate",
        note="Held as the non-loop comparison candidate.",
        params={},
        symbol_scale=0.244,
    ),
]


def render_icon_svg(variant: Variant) -> str:
    builder = build_loop_55 if variant.kind == "loop" else build_diamond_wide_gate_reference
    return svg_doc(ICON_SIZE, ICON_SIZE, [builder(variant)])


def render_logo_svg(variant: Variant) -> str:
    builder = build_loop_55 if variant.kind == "loop" else build_diamond_wide_gate_reference
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
        draw.rounded_rectangle((x, y + offset, x + width, y + height + offset), radius=radius, fill=(14, 23, 36, alpha))


def build_board() -> None:
    board = Image.new("RGBA", (2460, 1730), "#F5F0E8")
    draw = ImageDraw.Draw(board)
    title_font = load_font(62, bold=True)
    heading_font = load_font(26, bold=True)
    body_font = load_font(20)
    micro_font = load_font(18, bold=True)

    draw.text((84, 44), "Rotate 55deg route study", fill="#17212D", font=title_font)
    draw.text((86, 112), "The ring stays fixed at 55deg; only the teal route is adjusted. Diamond Wide Gate remains pinned at the end.", fill="#56636F", font=body_font)

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
        draw.text((x + 24, y + 48), variant.title, fill="#17212D", font=heading_font)
        draw.text((x + 24, y + 82), variant.tweak, fill=variant.orange if variant.kind == "loop" else variant.gold, font=body_font)
        draw.text((x + 24, y + 112), variant.note, fill="#5C6975", font=body_font)

        icon = Image.open(PNG_DIR / f"nbn-{variant.slug}-icon.png").convert("RGBA")
        board.alpha_composite(icon.resize((186, 186), Image.LANCZOS), (x + 22, y + 208))

        logo = Image.open(PNG_DIR / f"nbn-{variant.slug}-logo.png").convert("RGBA")
        logo_scale = min(510 / logo.width, 118 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 18, y + 420))

        draw.text((x + 24, y + 542), "64px", fill="#8894A0", font=micro_font)
        draw.text((x + 124, y + 542), "24px", fill="#8894A0", font=micro_font)
        draw.text((x + 208, y + 542), "16px", fill="#8894A0", font=micro_font)
        for px, size in ((20, 64), (116, 24), (198, 16)):
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 568))

    board.save(PNG_DIR / "nbn-round7-board.png")


def build_shortlist() -> None:
    shortlist = ["loop55-base", "loop55-even-flow", "loop55-exit-tuck", "loop55-high-crest", "diamond-wide-gate-candidate"]
    board = Image.new("RGBA", (2720, 1100), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    draw.text((96, 56), "Round 7 shortlist", fill="#17212D", font=title_font)
    draw.text((98, 124), "Pinned 55deg candidate, the strongest route tweaks, and Diamond Wide Gate for comparison.", fill="#56636F", font=body_font)

    for index, slug in enumerate(shortlist):
        variant = next(item for item in VARIANTS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 24, y + 24), variant.title, fill="#17212D", font=label_font)
        draw.text((x + 24, y + 60), variant.tweak, fill=variant.orange if variant.kind == "loop" else variant.gold, font=body_font)
        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(420 / logo.width, 114 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 20, y + 110))
        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px in (("96", 96, 30), ("48", 48, 174), ("24", 24, 294), ("16", 16, 376)):
            draw.text((x + px, y + 286), f"{label}px", fill="#86929E", font=body_font)
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 330))

    board.save(PNG_DIR / "nbn-round7-shortlist.png")


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
