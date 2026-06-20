"""Generate the 1200x630 social share banner (frontend/og-image.png).

This is a one-off asset generator, not a runtime dependency. Run it with
Pillow available, e.g.:

    uv run --with pillow python scripts/generate_og_image.py

Re-run whenever the title/tagline/brand colors change.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H = 1200, 630
OUT = Path(__file__).resolve().parent.parent / "frontend" / "og-image.png"

# Brand palette (matches the site gradient theme).
TOP = (7, 18, 42)        # deep navy
BOTTOM = (18, 9, 42)     # deep violet
CYAN = (125, 211, 252)
FUCHSIA = (244, 114, 182)
AMBER = (252, 211, 77)
TEXT = (241, 245, 249)
MUTED = (148, 163, 184)


def load_font(names: list[str], size: int) -> ImageFont.FreeTypeFont:
    candidates = []
    for name in names:
        candidates += [name, f"C:/Windows/Fonts/{name}"]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def vertical_gradient(size: tuple[int, int], top: tuple, bottom: tuple) -> Image.Image:
    w, h = size
    base = Image.new("RGB", size, top)
    draw = ImageDraw.Draw(base)
    for y in range(h):
        t = y / max(h - 1, 1)
        r = round(top[0] + (bottom[0] - top[0]) * t)
        g = round(top[1] + (bottom[1] - top[1]) * t)
        b = round(top[2] + (bottom[2] - top[2]) * t)
        draw.line([(0, y), (w, y)], fill=(r, g, b))
    return base


def glow(size: tuple[int, int], box: tuple, color: tuple, alpha: int) -> Image.Image:
    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    d.ellipse(box, fill=color + (alpha,))
    return layer.filter(ImageFilter.GaussianBlur(150))


def main() -> None:
    img = vertical_gradient((W, H), TOP, BOTTOM).convert("RGBA")

    # Soft corner glows for depth.
    img = Image.alpha_composite(img, glow((W, H), (-220, -260, 540, 360), CYAN, 150))
    img = Image.alpha_composite(img, glow((W, H), (760, 320, 1340, 820), FUCHSIA, 140))

    draw = ImageDraw.Draw(img)

    title_font = load_font(["segoeuib.ttf", "arialbd.ttf", "Arialbd.ttf"], 104)
    pill_font = load_font(["segoeuib.ttf", "arialbd.ttf"], 28)
    tag_font = load_font(["segoeui.ttf", "arial.ttf", "Arial.ttf"], 40)
    foot_font = load_font(["segoeui.ttf", "arial.ttf"], 30)

    margin = 84

    # Eyebrow pill.
    pill_text = "COURSE  AUTOMATION"
    pb = draw.textbbox((0, 0), pill_text, font=pill_font)
    pw, ph = pb[2] - pb[0], pb[3] - pb[1]
    px0, py0 = margin, 96
    draw.rounded_rectangle(
        (px0, py0, px0 + pw + 56, py0 + ph + 34),
        radius=(ph + 34) // 2,
        fill=CYAN + (235,),
        outline=CYAN + (255,),
        width=2,
    )
    draw.text((px0 + 28, py0 + 14), pill_text, font=pill_font, fill=(5, 7, 15))

    # Title.
    ty = 188
    draw.text((margin, ty), "VTU Automate", font=title_font, fill=TEXT)

    # Accent underline (cyan -> fuchsia -> amber segments).
    uy = ty + 128
    seg = 96
    for i, color in enumerate((CYAN, FUCHSIA, AMBER)):
        x0 = margin + i * (seg + 12)
        draw.rounded_rectangle((x0, uy, x0 + seg, uy + 12), radius=6, fill=color)

    # Tagline (two lines).
    lines = [
        "Submit your VTU course once — watch every",
        "lecture get marked complete in real time.",
    ]
    cy = uy + 52
    for ln in lines:
        draw.text((margin, cy), ln, font=tag_font, fill=MUTED)
        cy += 56

    # Footer domain.
    draw.text((margin, H - 86), "vtu-automate.fastapicloud.dev", font=foot_font, fill=CYAN)

    img.convert("RGB").save(OUT, "PNG")
    print(f"wrote {OUT} ({W}x{H})")


if __name__ == "__main__":
    main()
