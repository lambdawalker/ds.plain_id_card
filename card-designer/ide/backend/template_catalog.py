"""Editable ports of the repository's original id_cards templates."""
from pathlib import Path
import re

import yaml
from .template_context import ASSET_SOURCES

ROOT = Path(__file__).resolve().parents[1] / "templates" / "id_cards"


def template_source(key):
    # Template IDs come from the fixed repository gallery, never filesystem paths.
    if key not in tuple("ABCDEFG"):
        raise KeyError(f"Unknown template {key!r}")
    folder = ROOT / key
    info = yaml.safe_load((folder / "meta.yaml").read_text(encoding="utf-8"))
    kind = info.get("class", "horizontal_card")
    width, height = (740, 1285) if kind == "vertical_card" else (1350, 806)
    template = (folder / "index.html.j2").read_text(encoding="utf-8")
    required = sorted({ASSET_SOURCES[key][0] for key in re.findall(r'assets\.([a-z_][a-z_0-9]*)', template) if key in ASSET_SOURCES})
    return {"id": key, "template_id": key, "name": f"{key} · {kind.replace('_', ' ').title()}",
            "description": info.get("description", "").strip(), "kind": kind,
            "width": width, "height": height, "required_datasets": required,
            "template": template,
            "css": (ROOT / "base.css").read_text(encoding="utf-8") + "\n" + (folder / "style.css.j2").read_text(encoding="utf-8")}


def template_catalog():
    return [{key: value for key, value in template_source(variant).items() if key not in {"template", "css"}}
            for variant in "ABCDEFG"]
