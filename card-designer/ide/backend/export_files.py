"""Render one paired instance and encode its training files."""
from base64 import b64encode
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import re

import yaml

from .card_capture import annotation_script, capture_card

BUILTIN_CLASSES = ("card", "photo", "barcode", "pdf417", "qrcode", "mrz-text")


def class_names(layouts):
    extra = {
        name
        for layout in layouts
        for name in re.findall(r"""\bdata-class\s*=\s*["']([\w.-]+)["']""", layout["design"]["template"])
    }
    return [*BUILTIN_CLASSES, *sorted(extra.difference(BUILTIN_CLASSES))]


def yolo_labels(elements, width, height, classes):
    lines = ["0 0.500000 0.500000 1.000000 1.000000"]
    for element in elements:
        name = element["class"]
        if name == "card":
            continue
        if name not in classes:
            raise ValueError(f"Unknown YOLO class {name!r}. Declare it as a literal data-class in the layout.")
        left, top, right, bottom = element["bbox"]
        left, top = max(0, left), max(0, top)
        right, bottom = min(width, right), min(height, bottom)
        if right <= left or bottom <= top:
            continue
        coordinates = ((left + right) / (2 * width), (top + bottom) / (2 * height),
                       (right - left) / width, (bottom - top) / height)
        lines.append(f"{classes.index(name)} " + " ".join(f"{value:.6f}" for value in coordinates))
    return "\n".join(lines) + "\n"


@contextmanager
def renderer(origin):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=os.getenv("CARD_CHROMIUM") or None,
            args=["--enable-unsafe-swiftshader"],
        )
        try:
            engine = browser.new_page()
            engine.goto(origin)
            engine.wait_for_function("typeof window.cardDesignerRender === 'function'")
            page = browser.new_page()
            page.goto(origin + "/api/samples")
            yield engine, page, annotation_script()
        finally:
            browser.close()


def expected_files(layouts):
    return {"expected-ocr.yaml", *(layout["stem"] + suffix for layout in layouts for suffix in (".png", ".txt"))}


def render_instance(capture, session, index, target):
    engine, page, annotations = capture
    seed = (session["seed"] + index) % 2**32
    samples = session["samples"]
    sample = samples[index % len(samples)] if samples else ""
    result = {"instance": index + 1, "seed": seed, "sample": sample or None, "layouts": {}}
    for layout in session["layouts"]:
        design = {**layout["design"], "seed": seed, "sample": sample}
        stem = layout["stem"]
        rendered, card = capture_card(engine, page, design)
        measured = card.evaluate(annotations)
        if measured["broken_images"]:
            raise ValueError(f"Could not load images for {stem}: {', '.join(measured['broken_images'])}")
        words = measured["words"]
        fields = defaultdict(list)
        for word in words:
            if word["field"] and word["role"] == "value":
                fields[word["field"]].append(word["text"])
        result["layouts"][stem] = {
            "image": stem + ".png",
            "fields": {field: " ".join(values) for field, values in fields.items()},
            "text": " ".join(word["text"] for word in words),
            "words": words,
            "missing_assets": rendered.get("context", {}).get("missing", []),
        }
        (target / (stem + ".txt")).write_text(
            yolo_labels(measured["elements"], design["width"], design["height"], session["classes"]),
            encoding="utf-8",
        )
        card.screenshot(path=str(target / (stem + ".png")), animations="disabled")
    (target / "expected-ocr.yaml").write_text(
        yaml.safe_dump(result, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def preview_instance(origin, session):
    with TemporaryDirectory() as directory, renderer(origin) as capture:
        folder = Path(directory)
        render_instance(capture, session, 0, folder)
        return {
            "images": [
                {"name": layout["stem"] + ".png",
                 "src": "data:image/png;base64," + b64encode((folder / (layout["stem"] + ".png")).read_bytes()).decode(),
                 "yolo": (folder / (layout["stem"] + ".txt")).read_text(encoding="utf-8")}
                for layout in session["layouts"]
            ],
            "ocr": (folder / "expected-ocr.yaml").read_text(encoding="utf-8"),
            "classes": session["classes"],
        }
