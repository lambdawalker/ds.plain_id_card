"""Run with python -m unittest discover -s backend/tests from ide/."""
import json
import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from threading import Event, Thread, Barrier, get_ident
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

    def test_dataset_order_is_saved_without_reindexing_and_survives_rebuild(self):
        for name in ["first", "second", "third"]:
            service.add_dataset(service.DatasetReference(reference=str(self.dataset(name))))
        ids = [source["id"] for source in service.datasets()][::-1]
        with patch.object(service, "index_reference", side_effect=AssertionError("Order must not reindex")):
            self.assertEqual([source["id"] for source in service.reorder_datasets(service.DatasetOrder(ids=ids))], ids)
        self.assertEqual([source["id"] for source in service.pending_sources()], ids)
        self.assertEqual([source["id"] for source in service.rebuild_sources()], ids)
        with self.assertRaises(HTTPException):
            service.reorder_datasets(service.DatasetOrder(ids=[ids[0]] * 3))
        self.assertEqual([source["id"] for source in service.datasets()], ids)
        added = service.add_dataset(service.DatasetReference(reference=str(self.dataset("fourth"))))
        self.assertEqual([source["id"] for source in added][:3], ids)

    def test_random_record_uses_requested_source_and_avoids_current_record(self):
        root = self.dataset("random")
        (root / "img" / "002.png").write_bytes(b"another image")
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        key = sources[0]["id"]
        records = service.samples(source_id=key, offset=0)
        result = service.random_sample(source_id=key, exclude=records[0]["id"])
        self.assertEqual(result["id"], records[1]["id"])
        service.add_dataset(service.DatasetReference(reference=str(self.dataset("other"))))
        self.assertEqual(service.random_sample(source_id=key)["source_id"], key)
        with self.assertRaises(HTTPException):
            service.random_sample(source_id=-1)

    def test_no_builtin_people_and_static_preview_needs_no_sample(self):
        self.assertEqual(service.samples(offset=0), [])
        design = service.Design(name="static", template="<h1>Alex Rivera</h1>", css="", seed=1,
                                color="#ffffff", background="plain", sample="")
        result = service.preview(service.Preview(design=design))
        self.assertEqual(result["template"], "<h1>Alex Rivera</h1>")
        self.assertEqual(result["person"], {})

    def test_template_a_contains_static_content_without_demo_generators(self):
        source = service.template_a()
        self.assertNotIn("gen.", source["template"] + source["css"])
        self.assertNotIn("{{", source["template"] + source["css"])
        design = service.Design(name="A", **source, seed=1, color="#ffffff", background="plain", sample="")
        self.assertIn("Alex Rivera", service.preview(service.Preview(design=design))["template"])

    def test_unknown_selected_sample_still_reports_an_error(self):
        design = service.Design(name="missing", template="<p>text</p>", css="", seed=1,
                                color="#ffffff", background="plain", sample="missing")
        with self.assertRaises(HTTPException) as error:
            service.preview(service.Preview(design=design))
        self.assertEqual(error.exception.status_code, 422)

    def test_startup_indexes_in_background_and_dataset_reads_use_cache(self):
        root = self.dataset()
        with service.database() as db:
            db.execute("INSERT INTO dataset_source(reference) VALUES (?)", (str(root),))
        started, release = Event(), Event()
        original = service.index_reference
        def slow_index(reference):
            started.set()
            if not release.wait(3):
                raise RuntimeError("Test did not release indexer")
            return original(reference)
        async def scenario():
            try:
                async with service.lifespan(service.app):
                    self.assertTrue(started.wait(1))
                    self.assertTrue(service.dataset_status()["loading"])
                    pending = service.datasets()
                    self.assertEqual(pending[0]["reference"], str(root))
                    self.assertTrue(pending[0]["loading"])
                    self.assertEqual(service.designs(), [])
                    release.set()
                    await asyncio.to_thread(service.app.state.dataset_thread.join, 2)
                    self.assertFalse(service.dataset_status()["loading"])
                    self.assertEqual(service.datasets()[0]["count"], 1)
                    with patch.object(service, "index_reference", side_effect=AssertionError("GET rescanned dataset")):
                        self.assertEqual(service.datasets()[0]["count"], 1)
            finally:
                release.set()
        with patch.dict(os.environ, {"CARD_PHOTOS":"", "CARD_PHOTO_DATASET":"", "DS":""}), patch.object(service, "index_reference", side_effect=slow_index):
            asyncio.run(scenario())

    def test_removal_during_startup_does_not_restore_deleted_reference(self):
        root = self.dataset()
        with service.database() as db:
            key = db.execute("INSERT INTO dataset_source(reference) VALUES (?)", (str(root),)).lastrowid
        started, release, removed = Event(), Event(), Event()
        original = service.index_reference
        def slow_index(reference):
            started.set()
            if not release.wait(3):
                raise RuntimeError("Test did not release indexer")
            return original(reference)
        async def scenario():
            try:
                async with service.lifespan(service.app):
                    self.assertTrue(started.wait(1))
                    def remove():
                        service.remove_dataset(key)
                        removed.set()
                    thread = Thread(target=remove)
                    thread.start()
                    self.assertTrue(service.dataset_status()["loading"])
                    release.set()
                    self.assertTrue(await asyncio.to_thread(removed.wait, 2))
                    thread.join(1)
                    self.assertEqual(service.datasets(), [])
                    self.assertEqual(service.samples(offset=0), [])
            finally:
                release.set()
        with patch.dict(os.environ, {"CARD_PHOTOS":"", "CARD_PHOTO_DATASET":"", "DS":""}), patch.object(service, "index_reference", side_effect=slow_index):
            asyncio.run(scenario())

    def test_datasets_scan_in_distinct_parallel_threads(self):
        roots = [self.dataset("dataset-a"), self.dataset("dataset-b")]
        with service.database() as db:
            db.executemany("INSERT INTO dataset_source(reference) VALUES (?)", [(str(root),) for root in roots])
        barrier = Barrier(2, timeout=2)
        workers = set()
        original = service.index_reference
        def parallel_index(reference):
            workers.add(get_ident())
            barrier.wait()
            return original(reference)
        with patch.object(service, "index_reference", side_effect=parallel_index):
            sources = service.rebuild_sources()
        self.assertEqual(len(workers), 2)
        self.assertNotIn(get_ident(), workers)
        self.assertEqual([source["count"] for source in sources], [1, 1])

    def test_shutdown_does_not_wait_for_a_stalled_dataset(self):
        root = self.dataset()
        with service.database() as db:
            db.execute("INSERT INTO dataset_source(reference) VALUES (?)", (str(root),))
        started, release = Event(), Event()
        def stalled(reference):
            started.set()
            release.wait()
            return photo_index(root)
        async def scenario():
            async with service.lifespan(service.app):
                self.assertTrue(started.wait(1))
            self.assertFalse(service.app.state.dataset_thread.is_alive())
        with patch.dict(os.environ, {"CARD_PHOTOS":"", "CARD_PHOTO_DATASET":"", "DS":""}), patch.object(service, "index_reference", side_effect=stalled):
            try:
                asyncio.run(asyncio.wait_for(scenario(), 2))
            finally:
                release.set()

    def test_background_failure_settles_loading_and_reports_error(self):
        async def scenario():
            async with service.lifespan(service.app):
                await asyncio.to_thread(service.app.state.dataset_thread.join, 2)
                self.assertEqual(service.dataset_status(), {"loading": False, "error": "Drive unavailable"})
        with patch.object(service, "load_loose_photos", side_effect=OSError("Drive unavailable")), self.assertLogs("uvicorn.error", level="ERROR"):
            asyncio.run(scenario())

    def test_record_fields_keep_identity_separate_from_reserved_dataset_names(self):
        root = self.dataset(fields=[{"name":"image","type":"rawImage","source":"img"},
                                    {"name":"id","type":"rawImage","source":"img"},
                                    {"name":"source_id","type":"str","source":"source"},
                                    {"name":"images","type":"json","source":"other"}])
        (root / "source").mkdir()
        (root / "source/001.txt").write_text("Dataset source field")
        (root / "other").mkdir()
        (root / "other/001.json").write_text('{"extra":"Dataset image metadata"}')
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        record = service.samples(offset=0, source_id=sources[0]["id"])[0]
        details = service.sample_details(record["id"])
        self.assertEqual(details["record"]["source_id"], sources[0]["id"])
        self.assertEqual(details["fields"]["source_id"], "Dataset source field")
        self.assertEqual(details["fields"]["images"], {"extra":"Dataset image metadata"})
        self.assertEqual(details["fields"]["id"], record["images"]["id"])
        self.assertEqual(details["field_types"]["id"], "rawImage")

    def test_record_fields_report_missing_records(self):
        with self.assertRaises(HTTPException) as error:
            service.sample_details("unknown")
        self.assertEqual(error.exception.status_code, 404)

    def test_other_dataset_id_and_optional_photo_sidecars(self):
        root = self.dataset()
        self.assertEqual(photo_index(root)["001"]["image"], root / "img/001.png")

    def test_multiple_raw_images_are_indexed_and_exposed_as_distinct_urls(self):
        fields = [{"name":"image","type":"rawImage","source":"img"},
                  {"name":"slim_barcode","type":"rawImage","source":"barcodes"}]
        root = self.dataset(fields=fields)
        (root / "barcodes").mkdir()
        barcode = root / "barcodes/001.JPG"
        barcode.write_bytes(b"barcode jpg")
        (root / "barcodes/ignore.txt").write_text("not an image")
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        record = service.samples(offset=0, source_id=sources[0]["id"])[0]
        self.assertNotEqual(record["image"], record["slim_barcode"])
        self.assertEqual(record["images"]["slim_barcode"], record["slim_barcode"])
        self.assertEqual(service.photo(record["slim_barcode"].split("/")[-1]).path, barcode)
        self.assertEqual(sources[0]["count"], 1)
        self.assertEqual(sources[0]["image_fields"], ["image", "slim_barcode"])
        design = service.Design(name="test", template='<img src="{{ person.slim_barcode }}">',
                                css="", seed=1, color="#ffffff", background="plain", sample=record["id"])
        self.assertIn(record["slim_barcode"], service.preview(service.Preview(design=design))["template"])

    def test_dataset_can_use_named_images_without_an_image_field(self):
        root = self.dataset(fields=[{"name":"slim_barcode","type":"rawImage","source":"img"}])
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        record = service.samples(offset=0, source_id=sources[0]["id"])[0]
        self.assertEqual(record["image"], record["slim_barcode"])
        self.assertEqual(record["photo_url"], record["slim_barcode"])

    def test_records_with_only_a_secondary_image_are_browsable(self):
        root = self.dataset(fields=[{"name":"image","type":"rawImage","source":"img"},
                                    {"name":"slim_barcode","type":"rawImage","source":"barcodes"}])
        (root / "barcodes").mkdir()
        (root / "barcodes/002.jpg").write_bytes(b"barcode jpg")
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        records = service.samples(offset=0, source_id=sources[0]["id"])
        self.assertEqual([record["record_id"] for record in records], ["001", "002"])
        self.assertNotIn("slim_barcode", records[0])
        self.assertEqual(records[1]["image"], records[1]["slim_barcode"])

    def test_missing_primary_folder_uses_available_secondary_images(self):
        root = self.dataset(fields=[{"name":"image","type":"rawImage","source":"missing"},
                                    {"name":"slim_barcode","type":"rawImage","source":"img"}])
        sources = service.add_dataset(service.DatasetReference(reference=str(root)))
        record = service.samples(offset=0, source_id=sources[0]["id"])[0]
        self.assertEqual(record["image"], record["slim_barcode"])

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
        details = service.sample_details(record["id"])
        self.assertEqual(details["fields"]["text"], "ABC123")
        self.assertEqual(details["fields"]["payload"], {"format": "QR"})
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
