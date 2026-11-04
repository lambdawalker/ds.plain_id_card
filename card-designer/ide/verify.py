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

    def test_clone_is_independent(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(service, "DB", Path(temp) / "test.db"):
            with service.database() as db:
                db.execute("CREATE TABLE design(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            original = service.create(self.design)["id"]
            copy = service.clone(original)["id"]
            service.update(copy, self.design.model_copy(update={"seed": 9}))
            self.assertEqual(service.read(original).seed, 42)
            self.assertEqual(service.read(copy).seed, 9)
            service.delete(copy)
            self.assertEqual(service.designs(), [{"id": original, "name": "Test"}])

    def test_legacy_a_renders(self):
        design = self.design.model_copy(update=service.sample_template())
        result = service.preview(service.Preview(design=design))
        self.assertIn('data-class="photo"', result["template"])
        self.assertNotIn("{{", result["css"])
