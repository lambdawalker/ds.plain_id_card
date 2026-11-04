"""Index image DiskDatasets without loading image bytes into template data."""
import json

import yaml


class DatasetIndex(dict):
    def __init__(self, dataset_id, field_types, entries=()):
        super().__init__(entries)
        self.dataset_id = dataset_id
        self.field_types = field_types


def photo_index(root):
    root = root.resolve(strict=True)
    manifest_path = root / "manifest.yaml"
    try:
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8-sig"))
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValueError(f"Invalid manifest {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict) or manifest.get("type") != "DiskDataset":
        raise ValueError(f"Expected type: DiskDataset in {manifest_path}")
    dataset_id = manifest.get("id")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ValueError(f"Missing dataset id in {manifest_path}")
    definitions = manifest.get("fields")
    if not isinstance(definitions, list):
        raise ValueError(f"Expected a fields list in {manifest_path}")
    fields = {}
    for field in definitions:
        if not isinstance(field, dict) or not isinstance(field.get("name"), str) or not field["name"]:
            raise ValueError(f"Each manifest field needs a name in {manifest_path}")
        name = field["name"]
        if name in fields:
            raise ValueError(f"Duplicate manifest field {name!r} in {manifest_path}")
        kind = field.get("type")
        if (name == "image" and kind != "rawImage") or (name != "image" and kind not in {"str", "json"}):
            raise ValueError(f"Unsupported field {name!r} type {kind!r}: image must be rawImage; other fields must be str or json")
        if not isinstance(field.get("source"), str) or not field["source"]:
            raise ValueError(f"Field {name!r} needs a source folder in {manifest_path}")
        fields[name] = field
    if "image" not in fields:
        raise ValueError(f"Missing image field (type: rawImage) in {manifest_path}")
    index = DatasetIndex(dataset_id, {name: field["type"] for name, field in fields.items()})
    for name, field in fields.items():
        folder = (root / field["source"]).resolve()
        if not folder.is_relative_to(root):
            raise ValueError(f"Field {name!r} source points outside the dataset: {folder}")
        if not folder.exists() and name != "image":
            continue
        if not folder.is_dir():
            raise ValueError(f"Field {name!r} source folder is missing or not a directory: {folder}")
        for path in folder.iterdir():
            if not path.is_file() or not path.resolve().is_relative_to(root):
                continue
            if name == "image" and path.suffix.lower() not in {".png", ".webp", ".jpg", ".jpeg"}:
                continue
            entry = index.setdefault(path.stem, {})
            if name in entry:
                raise ValueError(f"Duplicate {name} for record {path.stem} in {folder}")
            entry[name] = path
    return DatasetIndex(dataset_id, index.field_types,
                        sorted((stem, fields) for stem, fields in index.items() if "image" in fields))


def metadata(files, field_types=None):
    field_types = field_types if field_types is not None else {"prompt": "str", "card": "json", "description": "str"}
    values = {}
    for name, kind in field_types.items():
        path = files.get(name)
        if path and kind in {"str", "json"}:
            text = path.read_text(encoding="utf-8-sig")
            values[name] = json.loads(text) if kind == "json" else text
    return values
