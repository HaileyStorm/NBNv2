from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from PIL import Image


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
SVG_DIR = BASE_DIR / "svg"
PNG_DIR = BASE_DIR / "png"
ICO_DIR = BASE_DIR / "ico"
NODE_RENDERER = BASE_DIR / "render_png.mjs"
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
        slug="soft-gold-right-n",
        title="Soft + Gold Right N",
        kind="loop",
        tweak="adopted primary mark",
        note="Bridge Soft route with the retained narrow B proportion and a gold right N terminal accent.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        n2_color="#E0A31A",
    ),
    Variant(
        slug="diamond-wide-gate",
        title="Diamond Wide Gate",
        kind="reference",
        tweak="final secondary candidate",
        note="Diamond icon study paired with the adopted Soft + Gold Right N wordmark.",
        b_width_mult=0.80,
        stroke_mult=0.98,
        n2_color="#E0A31A",
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


def clear_generated_dir(path_value: Path) -> None:
    path_value.mkdir(parents=True, exist_ok=True)
    for child in path_value.iterdir():
        if child.is_file():
            child.unlink()


def save_ico(png_path: Path, ico_path: Path) -> None:
    with Image.open(png_path) as image:
        square = image.convert("RGBA")
        square.save(
            ico_path,
            format="ICO",
            sizes=[(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)],
        )


def main() -> None:
    clear_generated_dir(SVG_DIR)
    clear_generated_dir(PNG_DIR)
    clear_generated_dir(ICO_DIR)

    for variant in VARIANTS:
        icon_svg = render_icon_svg(variant)
        logo_svg = render_logo_svg(variant)
        icon_svg_path = SVG_DIR / f"nbn-{variant.slug}-icon.svg"
        logo_svg_path = SVG_DIR / f"nbn-{variant.slug}-logo.svg"
        save_svg(icon_svg_path, icon_svg)
        save_svg(logo_svg_path, logo_svg)
        render_png(icon_svg_path, PNG_DIR / f"nbn-{variant.slug}-icon.png", ICON_SIZE)
        render_png(logo_svg_path, PNG_DIR / f"nbn-{variant.slug}-logo.png", LOGO_WIDTH)
        save_ico(PNG_DIR / f"nbn-{variant.slug}-icon.png", ICO_DIR / f"nbn-{variant.slug}-icon.ico")


if __name__ == "__main__":
    main()
