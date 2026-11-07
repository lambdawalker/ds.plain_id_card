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
        self.enterContext(patch.object(service, "records_root", lambda: self.directory / "records"))
        self.design = service.Design(
            name="Front", template="<main>Original</main>\n", css=".card { color: red; }\n",
            seed=42, color="#a4d5c8", background="plain", layout_id="stable-front",
        )
        self.bundle = service.Bundle(name="Custom", layouts=[self.design])

    def test_manual_file_edits_are_visible_to_designer_and_exporter(self):
        key = service.create_bundle(self.bundle)["id"]
        path = service.records_root() / "design-bundles" / str(key) / "bundle.yaml"
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

    def test_workspace_migrates_export_records_and_previous_yaml(self):
        import json
        from backend.exporter import Exporter

        previous = self.directory / "bundles" / "7"
        previous.mkdir(parents=True)
        import yaml
        (previous / "bundle.yaml").write_text(yaml.safe_dump(self.bundle.model_dump()), encoding="utf-8")
        (previous.parent / ".next-id").write_text("13\n", encoding="utf-8")
        with closing(service.database()) as db, db:
            db.execute("PRAGMA user_version=4")
            db.execute("CREATE TABLE export_bundle(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)")
            db.execute("CREATE TABLE export_session(id INTEGER PRIMARY KEY AUTOINCREMENT, folder TEXT, payload TEXT, status TEXT, completed INTEGER, error TEXT)")
            pairing = dict(name="Pair", layouts=[dict(bundle_id=7, layout_id="stable-front", stem="front")])
            db.execute("INSERT INTO export_bundle VALUES (4, ?)", (json.dumps(pairing),))
            layout = {**pairing["layouts"][0], "source_bundle": self.bundle.name, "design": self.design.model_dump()}
            snapshot = dict(name="Session", bundle_id=4, folder="example", layouts=[layout], samples=[],
                            bundle_name="Pair", source="none", count=2, seed=42, classes=["card"])
            db.execute("INSERT INTO export_session VALUES (5, 'example', ?, 'running', 1, '')", (json.dumps(snapshot),))

        service.initialize_bundle_storage()
        exporter = Exporter(service.database, service.load_bundle, lambda: {}, self.directory / "outputs", service.records_root)
        exporter.initialize()
        self.assertTrue((service.records_root() / "design-bundles/7/bundle.yaml").is_file())
        self.assertTrue((service.records_root() / "export-bundles/4.json").is_file())
        self.assertTrue((service.records_root() / "export-sessions/5.json").is_file())
        self.assertEqual(exporter.sessions()[0]["status"], "paused")
        self.assertEqual(exporter.session_records.load(5)["completed"], 1)
        self.assertEqual(exporter.sessions()[0]["completed"], 0)
        (exporter.root / "example" / "000002").mkdir(parents=True)
        self.assertEqual(exporter.sessions()[0]["completed"], 0)
        (exporter.root / "example" / "000001").mkdir()
        self.assertEqual(exporter.sessions()[0]["completed"], 1)
        exporter.delete_bundle(4)
        exporter.initialize()
        self.assertEqual(exporter.bundles(), [])
