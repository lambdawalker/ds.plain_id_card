"""Atomic text records with stable numeric IDs."""
import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import HTTPException


def atomic_write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False, mode="w", encoding="utf-8", newline="\n") as file:
        temporary = Path(file.name)
        try:
            file.write(contents)
            file.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def counter_value(root):
    try:
        return int((root / ".next-id").read_text(encoding="utf-8"))
    except FileNotFoundError:
        return 1


def legacy_sequence(db, table):
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'").fetchone():
        row = db.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (table,)).fetchone()
        if row:
            return row[0]
    return 0


class JsonRecords:
    def __init__(self, root):
        if root.is_symlink():
            raise RuntimeError(f"Record storage must be a directory: {root}")
        self.root = root

    def path(self, key):
        path = self.root / f"{key}.json"
        if key <= 0 or path.is_symlink():
            raise HTTPException(404, "Export record was deleted")
        return path

    def keys(self):
        return sorted(int(path.stem) for path in self.root.glob("*.json")
                      if path.stem.isascii() and path.stem.isdecimal() and path.stem[0] != "0"
                      and not path.is_symlink() and path.is_file())

    def load(self, key):
        path = self.path(key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(record, dict):
                raise ValueError("Expected a JSON object")
            return record
        except FileNotFoundError as exc:
            raise HTTPException(404, "Export record was deleted") from exc
        except (ValueError, OSError) as exc:
            raise HTTPException(422, f"Invalid export record: {path}") from exc

    def write(self, key, record):
        atomic_write(self.path(key), json.dumps(record, ensure_ascii=False, indent=2) + "\n")

    def keep_next_id(self, minimum):
        if minimum > counter_value(self.root):
            atomic_write(self.root / ".next-id", f"{minimum}\n")

    def reserve(self):
        key = max(counter_value(self.root), max(self.keys(), default=0) + 1)
        atomic_write(self.root / ".next-id", f"{key + 1}\n")
        return key

    def migrate(self, key, record):
        if not self.path(key).exists():
            self.write(key, record)
        elif self.load(key) != record:
            raise RuntimeError(f"Export record {key} differs between legacy storage and {self.path(key)}")
