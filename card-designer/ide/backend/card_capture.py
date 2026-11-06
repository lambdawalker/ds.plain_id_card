"""Capture native-size card images and annotations using the Studio renderer."""
from pathlib import Path


def capture_card(engine, page, design):
    width, height = design.get("width", 640), design.get("height", 400)
    page.set_viewport_size({"width": width, "height": height})
    rendered = engine.evaluate("d => window.cardDesignerRender(d, {scale: 1, pixelRatio: 1})", design)
    page.set_content(
        f'<iframe sandbox="" style="display:block;border:0;width:{width}px;height:{height}px"></iframe>'
        '<style>body{margin:0}</style>'
    )
    page.locator("iframe").evaluate("(frame, html) => frame.srcdoc = html", rendered["html"])
    card = page.frame_locator("iframe").locator("#view-port")
    card.wait_for()
    return rendered, card


def annotation_script():
    return (Path(__file__).resolve().parents[1] / "batch_annotations.js").read_text(encoding="utf-8")
