"""Render one paired instance and encode its training files."""
from base64 import b64encode
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import re

import yaml

from .card_capture import annotation_script, capture_card

BUILTIN_CLASSES = ("card", "photo", "barcode", "pdf417", "qrcode", "mrz-text")


CAPTURE_JSON = "expected-card-capture.json"
CAPTURE_FIELDS = (
    "full_name", "given_names", "surnames", "birth_date", "document_number",
    "issued_identifier", "address", "issue_date", "expiry_date", "issuing_country",
)
CAPTURE_LABELS = {
    "name": "full_name", "first name": "given_names", "surname": "surnames",
    "last name": "surnames", "family name": "surnames", "dob": "birth_date",
    "date of birth": "birth_date", "document no": "document_number", "dl no": "document_number",
    "issued": "issue_date", "expires": "expiry_date", "expiration date": "expiry_date",
}


def capture_expectation(layout_words):
    candidates = defaultdict(set)
    for words in layout_words:
        fields = defaultdict(list)
        clipped = set()
        for word in words:
            if word["role"] != "value":
                continue
            label = (word.get("field") or "").strip().casefold().rstrip(".")
            key = word.get("capture_field") or CAPTURE_LABELS.get(label, label.replace(" ", "_"))
            if key == "personal_number":
                key = "issued_identifier"
            if key not in CAPTURE_FIELDS:
                continue
            if word.get("clipped"):
                clipped.add(key)
            fields[key].append(word["text"])
        for key, parts in fields.items():
            value = " ".join(parts)
            if key not in clipped and value not in {"", "—", "–", "-"}:
                candidates[key].add(value)
    expected = {key: next(iter(candidates[key])) if len(candidates[key]) == 1 else ""
                for key in CAPTURE_FIELDS}
    if not candidates["full_name"] and expected["given_names"] and expected["surnames"]:
        expected["full_name"] = f'{expected["given_names"]} {expected["surnames"]}'
    return expected


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
    return {"expected-ocr.yaml", CAPTURE_JSON, *(layout["stem"] + suffix for layout in layouts for suffix in (".png", ".txt"))}


def render_instance(capture, session, index, target):
    engine, page, annotations = capture
    seed = (session["seed"] + index) % 2**32
    samples = session["samples"]
    sample = samples[index % len(samples)] if samples else ""
    result = {"instance": index + 1, "seed": seed, "sample": sample or None, "layouts": {}}
    layout_words = []
    for layout in session["layouts"]:
        design = {**layout["design"], "seed": seed, "sample": sample}
        stem = layout["stem"]
        rendered, card = capture_card(engine, page, design)
        measured = card.evaluate(annotations)
        if measured["broken_images"]:
            raise ValueError(f"Could not load images for {stem}: {', '.join(measured['broken_images'])}")
        words = measured["words"]
        layout_words.append(words)
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

    (target / CAPTURE_JSON).write_text(
        json.dumps(capture_expectation(layout_words), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
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
            "card_capture": (folder / CAPTURE_JSON).read_text(encoding="utf-8"),
            "classes": session["classes"],
        }
