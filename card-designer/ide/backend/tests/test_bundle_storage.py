import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from backend import app as service


class BundleStorageTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(patch.object(service, "DB", self.directory / "designs.sqlite"))
        self.design = service.Design(
            name="Front", template="<main>Original</main>\n", css=".card { color: red; }\n",
            seed=42, color="#a4d5c8", background="plain", layout_id="stable-front",
        )
        self.bundle = service.Bundle(name="Custom", layouts=[self.design])

    def test_manual_file_edits_are_visible_to_designer_and_exporter(self):
        key = service.create_bundle(self.bundle)["id"]
        path = self.directory / "bundles" / str(key) / "bundle.yaml"
        self.assertTrue(path.is_file())
        edited = path.read_text(encoding="utf-8").replace("Original", "Edited")
        path.write_text(edited.replace("common: {}", "common: {date: 2026-11-06}"), encoding="utf-8")
        self.assertEqual(service.read_bundle(key)["layouts"][0]["template"], "<main>Edited</main>\n")
        from backend.exporter import ExportBundle
        export = ExportBundle(name="Pair", layouts=[dict(bundle_id=key, layout_id="stable-front", stem="front")])
        design = service.exporter.resolve_layouts(export)[0]["design"]
        self.assertEqual(design["template"], "<main>Edited</main>\n")
        self.assertEqual(design["common"]["date"], "2026-11-06")

    def test_migration_preserves_ids_and_deleted_ids_are_never_reused(self):
        with closing(service.database()) as db, db:
            db.execute("CREATE TABLE bundle(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)")
            db.executemany("INSERT INTO bundle(id, payload) VALUES (?, ?)",
                           [(7, self.bundle.model_dump_json()), (12, self.bundle.model_dump_json())])
            db.execute("DELETE FROM bundle WHERE id=12")
            db.execute("PRAGMA user_version=3")
        service.initialize_bundle_storage()
        self.assertEqual(service.read_bundle(7)["layouts"][0]["layout_id"], "stable-front")
        clone = service.clone_bundle(7)
        self.assertGreater(clone["id"], 12)
        service.delete_bundle(7)
        service.initialize_bundle_storage()
        self.assertEqual([item["id"] for item in service.list_bundles()], [clone["id"]])
