"""Persistence and validation for card export runs."""
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory


def digest(data):
    return sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def atomic_write(path, data):
    with TemporaryDirectory(dir=path.parent) as temp:
        stage = Path(temp) / path.name
        stage.write_bytes(data)
        stage.replace(path)


def verified_record(target, plan):
    try:
        record = json.loads((target / "record.json").read_bytes())
        image = (target / "card.png").read_bytes()
        if (record["plan"] == plan and digest(image) == record["image_sha256"]
                and digest(json_bytes({k: v for k, v in record.items() if k != "record_sha256"}))
                == record["record_sha256"]):
            return record
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def report(output, records, failures):
    counts = Counter(record["image_sha256"] for _, record in records)
    rows = []
    for index, record in records:
        issues = list(record["issues"])
        if counts[record["image_sha256"]] > 1:
            issues.append("duplicate_image")
        rows.append({"index": index, "image": f"{index:06d}/card.png",
                     "record": f"{index:06d}/record.json", "split": record["split"],
                     "identity": record["identity"], "issues": issues})
    coverage = {
        key: dict(Counter(str(record.get(key)) for _, record in records))
        for key in ("template_id", "background", "split")
    }
    coverage["font"] = dict(Counter(font for _, record in records for font in record["fonts"]))
    atomic_write(output / "validation.json", json_bytes({
        "completed": len(rows), "failed": failures, "samples": rows, "coverage": coverage,
        "issue_counts": dict(Counter(issue for row in rows for issue in row["issues"])),
    }))
    eligible = [row for row in rows if row["split"] and not row["issues"]]
    atomic_write(output / "training.jsonl", b"".join(
        (json.dumps(row, ensure_ascii=False) + "\n").encode() for row in eligible
    ))
