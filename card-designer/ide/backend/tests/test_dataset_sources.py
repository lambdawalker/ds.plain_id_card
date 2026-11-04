"""Run with python -m unittest discover -s backend/tests from ide/."""
import json
import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from backend.photo_dataset import photo_index

_app_data = tempfile.TemporaryDirectory()
with patch.dict(os.environ, {"CARD_DATA": _app_data.name, "CARD_PHOTO_DATASET": "", "DS": "", "CARD_PHOTOS": ""}):
    from backend import app as service


class DatasetSourcesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        with service.database() as db:
            db.execute("DELETE FROM dataset_source")
        service.rebuild_sources()

    def dataset(self, name="ds.machine_readable_code", fields=None):
        root = self.root / name
        (root / "img").mkdir(parents=True)
        (root / "img" / "001.png").write_bytes(b"test image")
        manifest = {"type": "DiskDataset", "id": "lambdaWalker/" + name,
                    "fields": fields or [{"name": "image", "type": "rawImage", "source": "img"}]}
        (root / "manifest.yaml").write_text(json.dumps(manifest))
        return root

    def test_other_dataset_id_and_optional_photo_sidecars(self):
        root = self.dataset()
        self.assertEqual(photo_index(root)["001"]["image"], root / "img/001.png")

    def test_record_order_is_sorted_for_stable_pagination(self):
        root = self.dataset()
        for stem in ("zzz", "aaa"):
            (root / "img" / (stem + ".png")).write_bytes(b"test image")
        with patch.object(Path, "iterdir", return_value=iter([root / "img" / (stem + ".png") for stem in ("zzz", "aaa", "001")])):
            self.assertEqual(list(photo_index(root)), ["001", "aaa", "zzz"])

    def test_generic_text_and_json_fields_are_available_in_preview(self):
        fields = [{"name": "image", "type": "rawImage", "source": "img"},
                  {"name": "text", "type": "str", "source": "text"},
                  {"name": "payload", "type": "json", "source": "payload"}]
        root = self.dataset(fields=fields)
        for name, value in (("text", "ABC123"), ("payload", '{"format":"QR"}')):
            (root / name).mkdir()
            (root / name / "001.txt").write_text(value)
        result = service.add_dataset(service.DatasetReference(reference=str(root)))
        record = service.samples(offset=0, source_id=result[0]["id"])[0]
        self.assertEqual(record["dataset"], "lambdaWalker/ds.machine_readable_code")
        design = service.Design(name="test", template="{{ person.text }} {{ person.payload.format }}",
                                css="", seed=1, color="#ffffff", background="plain", sample=record["id"])
        self.assertEqual(service.preview(service.Preview(design=design))["template"], "ABC123 QR")

    def test_photo_ids_remain_stable_and_other_dataset_ids_do_not_collide(self):
        photo = self.dataset("ds.photo_id")
        other = self.dataset()
        service.add_dataset(service.DatasetReference(reference=str(photo)))
        result = service.add_dataset(service.DatasetReference(reference=str(other)))
        self.assertTrue(all(not source["error"] for source in result), result)
        records = [service.samples(offset=0, source_id=source["id"])[0] for source in result]
        from hashlib import sha256
        self.assertEqual(records[0]["id"], "photo-" + sha256(b"001").hexdigest())
        self.assertNotEqual(records[0]["id"], records[1]["id"])

    def test_invalid_manifest_logs_reference_path_and_traceback(self):
        root = self.dataset()
        (root / "manifest.yaml").write_text("fields: [")
        with self.assertLogs("uvicorn.error", level="ERROR") as logs:
            with self.assertRaises(HTTPException) as caught:
                service.add_dataset(service.DatasetReference(reference=str(root)))
        self.assertEqual(caught.exception.status_code, 422)
        self.assertIn("manifest.yaml", caught.exception.detail)
        self.assertIn(str(root), "\n".join(logs.output))
        self.assertIsNotNone(logs.records[0].exc_info)

    def test_request_errors_log_http_details_and_validation_location(self):
        from starlette.requests import Request
        from fastapi.exceptions import RequestValidationError
        request = Request({"type": "http", "method": "POST", "path": "/api/datasets", "headers": []})
        with self.assertLogs("uvicorn.error", level="WARNING") as logs:
            response = asyncio.run(service.log_http_error(request, HTTPException(422, "Missing image field")))
        self.assertEqual(response.status_code, 422)
        self.assertIn("POST /api/datasets -> 422: Missing image field", logs.output[0])
        error = RequestValidationError([{"loc": ("body", "reference"), "msg": "Field required", "type": "missing"}])
        with self.assertLogs("uvicorn.error", level="WARNING") as logs:
            response = asyncio.run(service.log_validation_error(request, error))
        self.assertEqual(response.status_code, 422)
        self.assertIn("reference", logs.output[0])

    def test_bad_fields_report_specific_reason(self):
        for fields, message in [
            ([{"name":"image","type":"rawImage","source":"../escape"}], "outside"),
            ([{"name":"image","type":"str","source":"img"}], "rawImage"),
            ([{"name":"image","type":"rawImage","source":"img"}, {"name":"codes","type":"unknown","source":"codes"}], "codes"),
        ]:
            with self.subTest(message=message):
                root = self.dataset(name=message, fields=fields)
                with self.assertRaisesRegex(ValueError, message):
                    photo_index(root)


if __name__ == "__main__":
    unittest.main()
