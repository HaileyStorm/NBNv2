from __future__ import annotations

import math
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
class StudyMark:
    slug: str
    title: str
    kind: Literal["loop", "reference"]
    angle: int | None
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


def build_loop_offset_rotation(mark: StudyMark) -> str:
    p = mark.params
    rotate = p.get("rotate", 0)
    route_raise = p.get("route_raise", -10)
    left_shift = p.get("left_shift", 0)
    left_drop = p.get("left_drop", 0)
    right_shift = p.get("right_shift", 12)
    right_drop = p.get("right_drop", 0)
    eye_dx = p.get("eye_dx", 18)
    eye_dy = p.get("eye_dy", 0)
    ring_width = p.get("ring_width", 70)
    path_width = p.get("path_width", 46)

    base_segments = [
        (208, 248, mark.gold),
        (266, 316, mark.ink),
        (334, 24, mark.gold),
        (40, 92, mark.ink),
        (112, 154, mark.gold),
        (170, 198, mark.ink),
    ]

    elements: list[str] = []
    for start, end, color in base_segments:
        elements.append(open_ring(512, 512, 286, start + rotate, end + rotate, color, ring_width))

    left_node = (248 + left_shift, 523 + left_drop + route_raise / 2)
    right_node = (710 + right_shift, 585 + right_drop + route_raise / 2)
    eye_x = 512 + eye_dx
    eye_y = 512 + eye_dy
    path_d = (
        f"M {pt(*left_node)} "
        f"C {pt(350 + left_shift / 2, 448 + route_raise)} {pt(430 + eye_dx / 2, 392 + route_raise)} {pt(eye_x, 432 + route_raise / 2)} "
        f"C {pt(586 + right_shift / 3, 466 + route_raise / 2)} {pt(608 + right_shift / 2, 590 + right_drop)} {pt(*right_node)}"
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


def build_diamond_wide_gate_reference(mark: StudyMark) -> str:
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


BASE_LOOP = {"eye_dx": 18, "route_raise": -10, "right_shift": 12}


STUDY_MARKS = [
    StudyMark(
        slug="loop-offset-base",
        title="Baseline 0deg",
        kind="loop",
        angle=0,
        tweak="reference only",
        note="The current Loop Offset Core orientation.",
        params={**BASE_LOOP, "rotate": 0},
    ),
    StudyMark(
        slug="loop-rot-10",
        title="Rotate 10deg",
        kind="loop",
        angle=10,
        tweak="ring only",
        note="Minimal clockwise turn; route left untouched.",
        params={**BASE_LOOP, "rotate": 10},
    ),
    StudyMark(
        slug="loop-rot-25",
        title="Rotate 25deg",
        kind="loop",
        angle=25,
        tweak="slight right-end tuck",
        note="Begins to pull the exit back toward the moving lower-right gap.",
        params={**BASE_LOOP, "rotate": 25, "right_shift": -2, "right_drop": -6},
    ),
    StudyMark(
        slug="loop-rot-40",
        title="Rotate 40deg",
        kind="loop",
        angle=40,
        tweak="right endpoint nudged inward",
        note="Shifts the exit inward so it stops pointing into empty space.",
        params={**BASE_LOOP, "rotate": 40, "right_shift": -18, "right_drop": -12, "route_raise": -6},
    ),
    StudyMark(
        slug="loop-rot-55",
        title="Rotate 55deg",
        kind="loop",
        angle=55,
        tweak="tucked and lowered",
        note="Moves the exit lower to relate to the descending gap position.",
        params={**BASE_LOOP, "rotate": 55, "right_shift": -34, "right_drop": -20, "route_raise": -2},
    ),
    StudyMark(
        slug="loop-rot-70",
        title="Rotate 70deg",
        kind="loop",
        angle=70,
        tweak="lowered route arc",
        note="The route sits lower so the whole gesture tracks the ring rotation.",
        params={**BASE_LOOP, "rotate": 70, "right_shift": -48, "right_drop": -28, "route_raise": 2, "eye_dx": 16},
    ),
    StudyMark(
        slug="loop-rot-85",
        title="Rotate 85deg",
        kind="loop",
        angle=85,
        tweak="both endpoints tightened",
        note="Slightly tightens both sides so the route still feels anchored.",
        params={**BASE_LOOP, "rotate": 85, "left_shift": 4, "right_shift": -60, "right_drop": -36, "route_raise": 6, "eye_dx": 14},
    ),
    StudyMark(
        slug="loop-rot-100",
        title="Rotate 100deg",
        kind="loop",
        angle=100,
        tweak="hardest pullback",
        note="Most aggressive endpoint correction once the gap reaches the bottom zone.",
        params={**BASE_LOOP, "rotate": 100, "left_shift": 8, "right_shift": -74, "right_drop": -44, "route_raise": 10, "eye_dx": 12, "eye_dy": 2},
    ),
    StudyMark(
        slug="diamond-wide-gate-candidate",
        title="Diamond Wide Gate",
        kind="reference",
        angle=None,
        tweak="pinned candidate",
        note="Held as the current non-loop candidate for comparison.",
        params={},
        symbol_scale=0.244,
    ),
]


def render_icon_svg(mark: StudyMark) -> str:
    builder = build_loop_offset_rotation if mark.kind == "loop" else build_diamond_wide_gate_reference
    return svg_doc(ICON_SIZE, ICON_SIZE, [builder(mark)])


def render_logo_svg(mark: StudyMark) -> str:
    builder = build_loop_offset_rotation if mark.kind == "loop" else build_diamond_wide_gate_reference
    return svg_doc(
        LOGO_WIDTH,
        LOGO_HEIGHT,
        [
            rect(0, 0, LOGO_WIDTH, LOGO_HEIGHT, fill="white"),
            group([builder(mark)], transform=f"translate({mark.symbol_x} {mark.symbol_y}) scale({fmt(mark.symbol_scale)})"),
            build_wordmark(mark.wordmark_x, mark.wordmark_y, 210, mark.ink),
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


def build_rotation_board() -> None:
    board = Image.new("RGBA", (2580, 2280), "#F5F0E8")
    draw = ImageDraw.Draw(board)
    title_font = load_font(62, bold=True)
    heading_font = load_font(24, bold=True)
    body_font = load_font(20)
    micro_font = load_font(18, bold=True)

    draw.text((100, 48), "Loop Offset Core rotation study", fill="#17212D", font=title_font)
    draw.text((102, 114), "Clockwise ring rotations from 10deg through 100deg, plus baseline and the pinned Diamond Wide Gate candidate.", fill="#56636F", font=body_font)

    card_w = 760
    card_h = 640
    start_x = 100
    start_y = 180
    gap_x = 26
    gap_y = 30

    for index, mark in enumerate(STUDY_MARKS):
        row = index // 3
        col = index % 3
        x = start_x + col * (card_w + gap_x)
        y = start_y + row * (card_h + gap_y)
        add_shadow(board, x, y, card_w, card_h, 30)
        draw.rounded_rectangle((x, y, x + card_w, y + card_h), radius=30, fill="white")
        draw.rounded_rectangle((x, y, x + card_w, y + 16), radius=30, fill="#17212D")
        draw.rectangle((x, y + 16, x + card_w, y + 32), fill="#17212D")
        draw.text((x + 22, y + 46), mark.title, fill="#17212D", font=heading_font)
        badge = f"{mark.angle}deg" if mark.angle is not None else "reference"
        badge_color = mark.orange if mark.kind == "loop" else mark.gold
        draw.text((x + 22, y + 78), badge, fill=badge_color, font=body_font)
        draw.text((x + 22, y + 104), mark.tweak, fill="#6B7884", font=body_font)
        note = draw.text((x + 22, y + 136), mark.note, fill="#5A6773", font=body_font)

        icon = Image.open(PNG_DIR / f"nbn-{mark.slug}-icon.png").convert("RGBA")
        board.alpha_composite(icon.resize((182, 182), Image.LANCZOS), (x + 20, y + 226))

        logo = Image.open(PNG_DIR / f"nbn-{mark.slug}-logo.png").convert("RGBA")
        logo_scale = min(620 / logo.width, 120 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 16, y + 404))

        draw.text((x + 22, y + 538), "64px", fill="#8894A0", font=micro_font)
        draw.text((x + 122, y + 538), "24px", fill="#8894A0", font=micro_font)
        draw.text((x + 206, y + 538), "16px", fill="#8894A0", font=micro_font)
        for px, size in ((20, 64), (116, 24), (198, 16)):
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 564))

    board.save(PNG_DIR / "nbn-round5-rotation-board.png")


def build_sequence_sheet() -> None:
    board = Image.new("RGBA", (3100, 620), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(56, bold=True)
    body_font = load_font(22)
    label_font = load_font(22, bold=True)

    draw.text((88, 40), "Rotation sequence", fill="#17212D", font=title_font)
    draw.text((90, 102), "Baseline, then clockwise ring rotations, with Diamond Wide Gate pinned at the end.", fill="#56636F", font=body_font)

    for index, mark in enumerate(STUDY_MARKS):
        x = 84 + index * 332
        y = 180
        badge = f"{mark.angle}deg" if mark.angle is not None else "candidate"
        draw.text((x + 18, y), badge, fill=mark.orange if mark.kind == "loop" else mark.gold, font=label_font)
        icon = Image.open(PNG_DIR / f"nbn-{mark.slug}-icon.png").convert("RGBA")
        board.alpha_composite(icon.resize((220, 220), Image.LANCZOS), (x, y + 40))

    board.save(PNG_DIR / "nbn-round5-sequence.png")


def build_shortlist() -> None:
    shortlist = ["loop-rot-40", "loop-rot-55", "loop-rot-70", "diamond-wide-gate-candidate"]
    board = Image.new("RGBA", (2200, 1100), "#F7F3EC")
    draw = ImageDraw.Draw(board)
    title_font = load_font(58, bold=True)
    body_font = load_font(24)
    label_font = load_font(28, bold=True)

    draw.text((96, 56), "Round 5 shortlist", fill="#17212D", font=title_font)
    draw.text((98, 124), "The most convincing rotated loop studies plus the pinned non-loop candidate.", fill="#56636F", font=body_font)

    for index, slug in enumerate(shortlist):
        mark = next(item for item in STUDY_MARKS if item.slug == slug)
        x = 96 + index * 520
        y = 220
        add_shadow(board, x, y, 460, 760, 32)
        draw.rounded_rectangle((x, y, x + 460, y + 760), radius=32, fill="white")
        draw.text((x + 24, y + 24), mark.title, fill="#17212D", font=label_font)
        badge = f"{mark.angle}deg" if mark.angle is not None else "candidate"
        draw.text((x + 24, y + 58), badge, fill=mark.orange if mark.kind == "loop" else mark.gold, font=body_font)
        draw.text((x + 24, y + 88), mark.tweak, fill="#6A7885", font=body_font)
        logo = Image.open(PNG_DIR / f"nbn-{slug}-logo.png").convert("RGBA")
        logo_scale = min(420 / logo.width, 114 / logo.height)
        logo_size = (int(logo.width * logo_scale), int(logo.height * logo_scale))
        board.alpha_composite(logo.resize(logo_size, Image.LANCZOS), (x + 20, y + 132))
        icon = Image.open(PNG_DIR / f"nbn-{slug}-icon.png").convert("RGBA")
        for label, size, px in (("96", 96, 30), ("48", 48, 174), ("24", 24, 294), ("16", 16, 376)):
            draw.text((x + px, y + 306), f"{label}px", fill="#86929E", font=body_font)
            board.alpha_composite(icon.resize((size, size), Image.LANCZOS), (x + px, y + 350))

    board.save(PNG_DIR / "nbn-round5-shortlist.png")


def main() -> None:
    SVG_DIR.mkdir(parents=True, exist_ok=True)
    PNG_DIR.mkdir(parents=True, exist_ok=True)

    for mark in STUDY_MARKS:
        icon_svg = render_icon_svg(mark)
        logo_svg = render_logo_svg(mark)
        icon_svg_path = SVG_DIR / f"nbn-{mark.slug}-icon.svg"
        logo_svg_path = SVG_DIR / f"nbn-{mark.slug}-logo.svg"
        save_svg(icon_svg_path, icon_svg)
        save_svg(logo_svg_path, logo_svg)
        render_png(icon_svg_path, PNG_DIR / f"nbn-{mark.slug}-icon.png", ICON_SIZE)
        render_png(logo_svg_path, PNG_DIR / f"nbn-{mark.slug}-logo.png", LOGO_WIDTH)

    build_rotation_board()
    build_sequence_sheet()
    build_shortlist()


if __name__ == "__main__":
    main()
