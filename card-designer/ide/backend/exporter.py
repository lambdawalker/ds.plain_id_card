"""Saved layout pairings and resumable local export sessions."""
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from random import Random
from tempfile import TemporaryDirectory
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .record_files import JsonRecords, legacy_sequence
from .export_files import CAPTURE_JSON, class_names, expected_files, preview_instance, render_instance, renderer

logger = logging.getLogger(__name__)
SAFE_NAME = r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
WINDOWS_DEVICES = {"con", "prn", "aux", "nul", *(f"{kind}{i}" for kind in ("com", "lpt") for i in range(1, 10))}


class LayoutReference(BaseModel):
    bundle_id: int = Field(gt=0)
    layout_id: str = Field(min_length=1, max_length=64)
    stem: str = Field(max_length=64, pattern=SAFE_NAME)


class ExportBundle(BaseModel):
    name: str = Field(min_length=1, max_length=160, pattern=r"\S")
    layouts: list[LayoutReference] = Field(min_length=1, max_length=16)


class SessionRequest(BaseModel):
    name: str = Field(min_length=1, max_length=160, pattern=r"\S")
    bundle_id: int = Field(gt=0)
    source: str = Field(pattern=r"^(none|loose|[0-9]+)$", max_length=24)
    count: int = Field(ge=1, le=100000)
    seed: int = Field(ge=0, lt=2**32)
    folder: str = Field(max_length=80, pattern=SAFE_NAME)


class SnapshotLayout(LayoutReference):
    source_bundle: str
    design: dict


class SavedSession(SessionRequest):
    bundle_name: str
    samples: list[str]
    layouts: list[SnapshotLayout] = Field(min_length=1, max_length=16)
    classes: list[str]
    status: Literal["draft", "running", "pausing", "paused", "failed", "complete"]
    completed: int = Field(ge=0)
    error: str


def validate_filename(value):
    if value.casefold() in WINDOWS_DEVICES:
        raise HTTPException(422, f"{value!r} is a reserved filename")


class Exporter:
    def __init__(self, database, load_bundle, catalog, root, records_root):
        self.database = database
        self.load_bundle = load_bundle
        self.catalog = catalog
        self.root = root.resolve()
        self.records_root = records_root
        self.lock = Lock()
        self.workers = {}
        self.worker_limit = int(os.getenv("CARD_EXPORT_WORKERS", "32"))
        if not 1 <= self.worker_limit <= 32:
            raise ValueError("CARD_EXPORT_WORKERS must be between 1 and 32")
        self.browser_slots = BoundedSemaphore(self.worker_limit)
        self.router = APIRouter(prefix="/api/export")
        for path, method, handler in (
            ("/bundles", "GET", self.bundles), ("/bundles", "POST", self.create_bundle),
            ("/bundles/{key}", "PUT", self.update_bundle), ("/bundles/{key}", "DELETE", self.delete_bundle),
            ("/bundles/{key}/clone", "POST", self.clone_bundle), ("/sources", "GET", self.sources),
            ("/sessions", "GET", self.sessions), ("/sessions", "POST", self.create_session),
            ("/sessions/{key}/preview", "POST", self.preview),
            ("/sessions/{key}/start", "POST", self.start), ("/sessions/{key}/pause", "POST", self.pause),
            ("/sessions/{key}/instances/{index}", "GET", self.instance),
            ("/sessions/{key}/instances/{index}/{filename}", "GET", self.file),
        ):
            self.router.add_api_route(path, handler, methods=[method])

    @property
    def bundle_records(self):
        return JsonRecords(self.records_root() / "export-bundles")

    @property
    def session_records(self):
        return JsonRecords(self.records_root() / "export-sessions")

    @contextmanager
    def writes(self):
        with closing(self.database()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            yield db

    def initialize(self):
        with self.writes() as db:
            if db.execute("PRAGMA user_version").fetchone()[0] < 6:
                for table, records in (("export_bundle", self.bundle_records),
                                       ("export_session", self.session_records)):
                    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                        continue
                    for row in db.execute(f"SELECT * FROM {table} ORDER BY id"):
                        record = json.loads(row["payload"])
                        if table == "export_session":
                            record.update(folder=row["folder"], status=row["status"],
                                          completed=row["completed"], error=row["error"])
                        records.migrate(row["id"], record)
                    records.keep_next_id(legacy_sequence(db, table) + 1)
                db.execute("PRAGMA user_version=6")

        with self.writes():
            for key in self.session_records.keys():
                try:
                    session = self.load_session(key)
                except HTTPException as exc:
                    logger.warning("Skipping export session %s during recovery: %s", key, exc.detail)
                    continue
                if session["status"] in ("running", "pausing"):
                    self.session_records.write(key, {**session, "status": "paused"})

    def resolve_layouts(self, bundle):
        layouts = []
        sources = {}
        names = set()
        for reference in bundle.layouts:
            validate_filename(reference.stem)
            if reference.stem.casefold() in names:
                raise HTTPException(422, "Layout output names must be unique")
            names.add(reference.stem.casefold())
            if reference.bundle_id not in sources:
                sources[reference.bundle_id] = self.load_bundle(reference.bundle_id)
            source = sources[reference.bundle_id]
            design = next((layout for layout in source.layouts if layout.layout_id == reference.layout_id), None)
            if design is None:
                raise HTTPException(422, f"Layout for {reference.stem} is no longer available")
            layouts.append({**reference.model_dump(), "source_bundle": source.name, "design": design.model_dump()})
        return layouts

    def bundles(self):
        records = self.bundle_records
        return [{**records.load(key), "id": key} for key in records.keys()]

    def create_bundle(self, bundle: ExportBundle):
        with self.writes():
            self.resolve_layouts(bundle)
            records = self.bundle_records
            key = records.reserve()
            records.write(key, bundle.model_dump())
        return dict(id=key, **bundle.model_dump())

    def update_bundle(self, key: int, bundle: ExportBundle):
        with self.writes():
            self.resolve_layouts(bundle)
            self.bundle_records.load(key)
            self.bundle_records.write(key, bundle.model_dump())
        return dict(id=key, **bundle.model_dump())

    def delete_bundle(self, key: int):
        with self.writes():
            try:
                self.bundle_records.path(key).unlink()
            except FileNotFoundError as exc:
                raise HTTPException(404, "Export bundle was deleted") from exc
        return {"deleted": key}

    def clone_bundle(self, key: int):
        bundle = ExportBundle.model_validate(self.bundle_records.load(key))
        bundle.name = bundle.name[:155] + " copy"
        return self.create_bundle(bundle)

    def sources(self):
        catalog = self.catalog()
        loose = sum(record.get("source_id") is None for record in catalog["records"].values())
        sources = [{"id": "none", "name": "Seed only", "count": None}]
        if loose:
            sources.append({"id": "loose", "name": "Loose photos", "count": loose})
        sources.extend({"id": str(source["id"]), "name": source["reference"], "count": source["count"]}
                       for source in catalog["sources"] if source.get("count"))
        return sources

    def load_session(self, key):
        records = self.session_records
        try:
            session = SavedSession.model_validate(records.load(key))
            validate_filename(session.folder)
            stems = set()
            for layout in session.layouts:
                validate_filename(layout.stem)
                stems.add(layout.stem.casefold())
            if len(stems) != len(session.layouts) or session.completed > session.count:
                raise ValueError("Invalid layout names or progress")
            return session.model_dump()
        except ValueError as exc:
            raise HTTPException(422, f"Invalid export session: {records.path(key)}") from exc

    def session_summary(self, key, session):
        summary = {k: v for k, v in session.items() if k not in {"samples", "layouts"}}
        folder = self.output_folder(session)
        if session["status"] in ("paused", "failed", "complete"):
            completed = self.local_progress(folder, session["completed"])
            if completed < session["completed"]:
                summary.update(status="paused", completed=completed)
        return {**summary, "id": key, "layout_count": len(session["layouts"]), "output_path": str(folder)}

    def sessions(self):
        records = self.session_records
        return [self.session_summary(key, self.load_session(key)) for key in reversed(records.keys())]

    def output_folder(self, session):
        name = session["folder"]
        if not isinstance(name, str) or not re.fullmatch(SAFE_NAME, name):
            raise HTTPException(422, "Invalid session output folder")
        validate_filename(name)
        return self.root / name

    @staticmethod
    def local_progress(folder, recorded):
        if not recorded or folder.is_symlink() or not folder.is_dir():
            return 0
        instances = {path.name for path in folder.iterdir()
                     if path.name.isascii() and path.name.isdecimal()
                     and not path.is_symlink() and path.is_dir()}
        for index in range(1, recorded + 1):
            if f"{index:06d}" not in instances:
                return index - 1
        return recorded

    def create_session(self, settings: SessionRequest):
        validate_filename(settings.folder)
        samples = []
        if settings.source != "none":
            samples = sorted(record["id"] for record in self.catalog()["records"].values()
                             if (str(record.get("source_id")) if record.get("source_id") is not None else "loose") == settings.source)
            if not samples:
                raise HTTPException(422, "This record source has no available records")
            Random(settings.seed).shuffle(samples)
        folder = self.root / settings.folder
        with self.writes():
            records = self.session_records
            bundle = ExportBundle.model_validate(self.bundle_records.load(settings.bundle_id))
            layouts = self.resolve_layouts(bundle)
            session = {**settings.model_dump(), "bundle_name": bundle.name, "samples": samples,
                       "layouts": layouts, "classes": class_names(layouts),
                       "status": "draft", "completed": 0, "error": ""}
            if any(self.load_session(key)["folder"].casefold() == settings.folder.casefold() for key in records.keys()):
                raise HTTPException(409, "Choose a different output folder")
            self.create_output_folder(folder, session["classes"])
            try:
                key = records.reserve()
                records.write(key, session)
                result = self.session_summary(key, session)
            except Exception:
                (folder / "classes.txt").unlink(missing_ok=True)
                folder.rmdir()
                raise
        return result

    def create_output_folder(self, folder, classes):
        self.root.mkdir(parents=True, exist_ok=True)
        try:
            folder.mkdir()
        except FileExistsError as exc:
            raise HTTPException(409, "Output folder already exists") from exc
        try:
            (folder / "classes.txt").write_text("\n".join(classes) + "\n", encoding="utf-8")
        except Exception:
            (folder / "classes.txt").unlink(missing_ok=True)
            folder.rmdir()
            raise

    @staticmethod
    def origin(request):
        # Use the listener port, not the incoming Host or proxy headers.
        return f"http://127.0.0.1:{request.scope['server'][1]}"

    def preview(self, key: int, request: Request):
        session = self.load_session(key)
        if not self.browser_slots.acquire(blocking=False):
            raise HTTPException(409, "All export browsers are busy. Try preview again when a worker is free.")
        try:
            return preview_instance(self.origin(request), session)
        except Exception as exc:
            logger.exception("Export preview failed for session %s", key)
            raise HTTPException(422, str(exc)) from exc
        finally:
            self.browser_slots.release()

    def start(self, key: int, request: Request):
        origin = self.origin(request)
        with self.lock:
            with self.writes():
                records = self.session_records
                session = self.load_session(key)
                folder = self.output_folder(session)
                missing_output = not folder.exists() and not folder.is_symlink()
                completed = self.local_progress(folder, session["completed"])
                resumable = session["status"] in ("draft", "paused", "failed")
                incomplete = session["status"] == "complete" and completed < session["completed"]
                if key in self.workers or not (resumable or incomplete):
                    raise HTTPException(409, "This session cannot be started right now")
                if missing_output:
                    self.create_output_folder(folder, session["classes"])
                session.update(status="running", error="", completed=completed)
                records.write(key, session)
                result = self.session_summary(key, session)
            stop = Event()
            worker = Thread(target=self.run, args=(key, origin, stop), name=f"card-export-{key}", daemon=True)
            self.workers[key] = stop
            try:
                worker.start()
            except Exception:
                del self.workers[key]
                self.update_session(key, status="failed", error="Could not start the export worker")
                raise
        return result

    def pause(self, key: int):
        with self.lock, self.writes():
            records = self.session_records
            session = self.load_session(key)
            if session["status"] != "running" or key not in self.workers:
                raise HTTPException(409, "This session is not running")
            session["status"] = "pausing"
            records.write(key, session)
            self.workers[key].set()
            return self.session_summary(key, session)

    def update_session(self, key, **changes):
        with self.writes():
            records = self.session_records
            records.write(key, {**self.load_session(key), **changes})

    def render_batch(self, key, origin, session, folder, stop):
        completed = session["completed"]
        indices = iter(range(completed, session["count"]))
        finished = set()
        progress_lock = Lock()
        expected = expected_files(session["layouts"])

        def stop_on_error(exc_type, _exc, _traceback):
            if exc_type is not None:
                stop.set()

        def work():
            nonlocal completed
            try:
                with ExitStack() as resources:
                    # Reserve capacity before taking an index so another session cannot
                    # leave a gap behind a worker that is still waiting for a browser.
                    while not stop.is_set():
                        if self.browser_slots.acquire(timeout=0.1):
                            resources.callback(self.browser_slots.release)
                            break
                    capture = None
                    while not stop.is_set():
                        with progress_lock:
                            index = next(indices, None) if not stop.is_set() else None
                        if index is None:
                            return
                        target = folder / f"{index+1:06d}"
                        if target.exists() or target.is_symlink():
                            # A restart can land between publication and the progress update.
                            if (target.is_symlink() or not target.is_dir()
                                    or {path.name for path in target.iterdir()} | {CAPTURE_JSON} != expected
                                    or any(path.is_symlink() or not path.is_file() or not path.stat().st_size
                                           for path in target.iterdir())):
                                raise ValueError(f"Incomplete existing instance {target.name}. Move it aside and resume.")
                        else:
                            if capture is None:
                                # Sync Playwright objects stay on the thread that created them.
                                capture = resources.enter_context(renderer(origin))
                                resources.push(stop_on_error)
                            with TemporaryDirectory(prefix=".stage-", dir=folder) as temporary:
                                stage = Path(temporary) / "instance"
                                stage.mkdir()
                                render_instance(capture, session, index, stage)
                                stage.rename(target)
                        with progress_lock:
                            finished.add(index)
                            previous = completed
                            while completed in finished:
                                finished.remove(completed)
                                completed += 1
                            if completed != previous:
                                self.update_session(key, completed=completed)
            except Exception:
                stop.set()
                raise

        workers = min(self.worker_limit, session["count"] - completed)
        if workers:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"export-{key}") as pool:
                tasks = [pool.submit(work) for _ in range(workers)]
                for task in tasks:
                    task.result()
        return completed

    def run(self, key, origin, stop):
        status, error = "complete", ""
        try:
            session = self.load_session(key)
            folder = self.output_folder(session)
            if folder.is_symlink() or not folder.is_dir():
                raise ValueError("The original output folder is unavailable")
            class_file = folder / "classes.txt"
            if class_file.is_symlink() or class_file.read_text(encoding="utf-8").splitlines() != session["classes"]:
                raise ValueError("The session class map has changed")
            if self.render_batch(key, origin, session, folder, stop) < session["count"]:
                status = "paused"
        except Exception as exc:
            logger.exception("Card export session %s failed", key)
            status, error = "failed", str(exc)
        finally:
            with self.lock:
                self.workers.pop(key, None)
                self.update_session(key, status=status, error=error)

    def instance_path(self, key, index):
        payload = self.load_session(key)
        if not 1 <= index <= payload["completed"]:
            raise HTTPException(404, "Instance is not available")
        folder = self.output_folder(payload) / f"{index:06d}"
        if folder.resolve() != folder or not folder.is_dir():
            raise HTTPException(404, "Instance folder is not available")
        filenames = expected_files(payload["layouts"])
        if not (folder / CAPTURE_JSON).is_file():
            filenames.remove(CAPTURE_JSON)
        return folder, filenames

    def instance(self, key: int, index: int):
        folder, filenames = self.instance_path(key, index)
        return {"path": str(folder), "files": sorted(filenames)}

    def file(self, key: int, index: int, filename: str):
        folder, filenames = self.instance_path(key, index)
        path = folder / filename
        if filename not in filenames or path.is_symlink() or not path.is_file():
            raise HTTPException(404, "Export file is not available")
        return FileResponse(path)
