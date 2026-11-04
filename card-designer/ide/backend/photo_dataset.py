"""Read the photo manifest without loading image bytes into template data."""
import json

import yaml


def photo_index(root):
    root = root.resolve(strict=True)
    manifest = yaml.safe_load((root / "manifest.yaml").read_text(encoding="utf-8-sig"))
    types = {"image": "rawImage", "prompt": "str", "card": "json", "description": "str"}
    if manifest["type"] != "DiskDataset" or manifest["id"].casefold() != "lambdawalker/ds.photo_id":
        raise ValueError("Expected the ds.photo_id DiskDataset manifest")
    fields = {field["name"]: field for field in manifest["fields"]}
    index = {}
    for name, kind in types.items():
        field = fields[name]
        folder = (root / field["source"]).resolve()
        if field["type"] != kind or not folder.is_relative_to(root):
            raise ValueError(f"Invalid manifest field {name}")
        if not folder.exists() and name != "image":
            continue
        for path in folder.iterdir():
            if not path.is_file() or not path.resolve().is_relative_to(root):
                continue
            if name == "image" and path.suffix.lower() not in {".png", ".webp", ".jpg", ".jpeg"}:
                continue
            entry = index.setdefault(path.stem, {})
            if name in entry:
                raise ValueError(f"Duplicate {name} for record {path.stem}")
            entry[name] = path
    return {stem: fields for stem, fields in sorted(index.items()) if "image" in fields}


def metadata(files):
    values = {}
    for name in ("prompt", "card", "description"):
        path = files.get(name)
        if path:
            text = path.read_text(encoding="utf-8-sig")
            values[name] = json.loads(text) if name == "card" else text
    return values
