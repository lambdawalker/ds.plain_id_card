"""Run after building web/dist: python -m unittest verify."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend import app as service


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.design = service.Design(name="Test", template="{{ range(10000)|random }}",
                                     css="", seed=42, color="#abcdef", background="type-b", sample="")

    def test_replay_after_interleaved_seed(self):
        def render(seed):
            return service.preview(service.Preview(design=self.design.model_copy(update={"seed": seed})))
        original = render(42)
        self.assertNotEqual(original, render(43))
        self.assertEqual(original, render(42))

    def test_bundle_crud_and_clone_are_atomic_and_independent(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(service, "DB", Path(temp) / "test.db"), patch.object(service, "records_root", lambda: Path(temp) / "records"):
            bundle = service.Bundle(name="Test bundle", layouts=[self.design, self.design])
            original = service.create_bundle(bundle)["id"]
            draft = bundle.model_copy(deep=True)
            draft.layouts[1].seed = 9
            copy = service.clone_bundle(original, draft)["id"]
            self.assertEqual(service.read_bundle(original)["layouts"][1]["seed"], 42)
            self.assertEqual(service.read_bundle(copy)["layouts"][1]["seed"], 9)
            service.update_bundle(original, draft)
            self.assertEqual(service.read_bundle(original)["layouts"][1]["seed"], 9)
            service.delete_bundle(copy)
            self.assertEqual(service.list_bundles(), [{"id": original, "name": "Test bundle", "layout_count": 2}])

    def test_existing_design_migrates_once_and_deleted_bundle_stays_deleted(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(service, "DB", Path(temp) / "test.db"), patch.object(service, "records_root", lambda: Path(temp) / "records"):
            with service.database() as db:
                db.execute("CREATE TABLE design(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
                db.execute("INSERT INTO design(payload) VALUES (?)", (self.design.model_dump_json(),))
            service.initialize_bundle_storage()
            migrated = service.list_bundles()[0]["id"]
            self.assertEqual(service.read_bundle(migrated)["layouts"][0]["template"], self.design.template)
            self.assertTrue(any(row["layout_count"] == 2 for row in service.list_bundles()))
            service.delete_bundle(migrated)
            remaining = service.list_bundles()
            service.initialize_bundle_storage()
            self.assertEqual(service.list_bundles(), remaining)

    def test_legacy_a_renders(self):
        design = self.design.model_copy(update=service.sample_template())
        result = service.preview(service.Preview(design=design))
        self.assertIn('data-class="photo"', result["template"])
        self.assertNotIn("{{", result["css"])
