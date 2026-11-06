"""Saved layout pairings and resumable local export sessions."""
import json
import logging
from contextlib import closing
from pathlib import Path
from random import Random
from tempfile import TemporaryDirectory
from threading import Lock, Thread

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .export_files import class_names, expected_files, preview_instance, render_instance, renderer

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


def validate_filename(value):
    if value.casefold() in WINDOWS_DEVICES:
        raise HTTPException(422, f"{value!r} is a reserved filename")


def load_row(db, table, key):
    row = db.execute(f"SELECT * FROM {table} WHERE id=?", (key,)).fetchone()
    if row is None:
        raise HTTPException(404, "Export bundle or session was deleted")
    return row


class Exporter:
    def __init__(self, database, load_bundle, catalog, root):
        self.database = database
        self.load_bundle = load_bundle
        self.catalog = catalog
        self.root = root.resolve()
        self.lock = Lock()
        self.workers = {}
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

    def initialize(self):
        with closing(self.database()) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS export_bundle(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS export_session(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, folder TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    payload TEXT NOT NULL, status TEXT NOT NULL, completed INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT ''
                )
            """)
            db.execute("UPDATE export_session SET status='paused' WHERE status IN ('running', 'pausing')")

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
        with closing(self.database()) as db:
            return [dict(id=row["id"], **json.loads(row["payload"]))
                    for row in db.execute("SELECT * FROM export_bundle ORDER BY id")]

    def create_bundle(self, bundle: ExportBundle):
        with closing(self.database()) as db, db:
            self.resolve_layouts(bundle)
            key = db.execute("INSERT INTO export_bundle(payload) VALUES (?)", (bundle.model_dump_json(),)).lastrowid
        return dict(id=key, **bundle.model_dump())

    def update_bundle(self, key: int, bundle: ExportBundle):
        with closing(self.database()) as db, db:
            self.resolve_layouts(bundle)
            if not db.execute("UPDATE export_bundle SET payload=? WHERE id=?", (bundle.model_dump_json(), key)).rowcount:
                raise HTTPException(404, "Export bundle was deleted")
        return dict(id=key, **bundle.model_dump())

    def delete_bundle(self, key: int):
        with closing(self.database()) as db, db:
            if not db.execute("DELETE FROM export_bundle WHERE id=?", (key,)).rowcount:
                raise HTTPException(404, "Export bundle was deleted")
        return {"deleted": key}

    def clone_bundle(self, key: int):
        with closing(self.database()) as db:
            bundle = ExportBundle.model_validate_json(load_row(db, "export_bundle", key)["payload"])
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

    def session_summary(self, row):
        payload = json.loads(row["payload"])
        summary = {k: v for k, v in payload.items() if k not in {"samples", "layouts"}}
        return {**summary, "id": row["id"], "status": row["status"], "completed": row["completed"],
                "error": row["error"], "layout_count": len(payload["layouts"]),
                "output_path": str(self.root / row["folder"])}

    def sessions(self):
        with closing(self.database()) as db:
            return [self.session_summary(row) for row in db.execute("SELECT * FROM export_session ORDER BY id DESC")]

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
        with closing(self.database()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = load_row(db, "export_bundle", settings.bundle_id)
            bundle = ExportBundle.model_validate_json(row["payload"])
            layouts = self.resolve_layouts(bundle)
            session = {**settings.model_dump(), "bundle_name": bundle.name, "samples": samples,
                       "layouts": layouts, "classes": class_names(layouts)}
            if db.execute("SELECT 1 FROM export_session WHERE folder=?", (settings.folder,)).fetchone():
                raise HTTPException(409, "Choose a different output folder")
            self.root.mkdir(parents=True, exist_ok=True)
            try:
                folder.mkdir()
            except FileExistsError as exc:
                raise HTTPException(409, "Output folder already exists") from exc
            try:
                (folder / "classes.txt").write_text("\n".join(session["classes"]) + "\n", encoding="utf-8")
                key = db.execute("INSERT INTO export_session(folder,payload,status) VALUES (?,?,'draft')",
                                 (settings.folder, json.dumps(session))).lastrowid
                result = self.session_summary(load_row(db, "export_session", key))
                db.commit()
            except Exception:
                (folder / "classes.txt").unlink(missing_ok=True)
                folder.rmdir()
                raise
        return result

    @staticmethod
    def origin(request):
        # Use the listener port, not the incoming Host or proxy headers.
        return f"http://127.0.0.1:{request.scope['server'][1]}"

    def preview(self, key: int, request: Request):
        with closing(self.database()) as db:
            row = load_row(db, "export_session", key)
            session = json.loads(row["payload"])
        try:
            return preview_instance(self.origin(request), session)
        except Exception as exc:
            logger.exception("Export preview failed for session %s", key)
            raise HTTPException(422, str(exc)) from exc

    def start(self, key: int, request: Request):
        origin = self.origin(request)
        with self.lock:
            with closing(self.database()) as db, db:
                row = load_row(db, "export_session", key)
                if key in self.workers or row["status"] not in ("draft", "paused", "failed"):
                    raise HTTPException(409, "This session cannot be started right now")
                db.execute("UPDATE export_session SET status='running',error='' WHERE id=?", (key,))
                result = self.session_summary(load_row(db, "export_session", key))
            worker = Thread(target=self.run, args=(key, origin), name=f"card-export-{key}", daemon=True)
            self.workers[key] = worker
            try:
                worker.start()
            except Exception:
                del self.workers[key]
                self.set_status(key, "failed", "Could not start the export worker")
                raise
        return result

    def pause(self, key: int):
        with closing(self.database()) as db, db:
            if not db.execute("UPDATE export_session SET status='pausing' WHERE id=? AND status='running'", (key,)).rowcount:
                raise HTTPException(409, "This session is not running")
            return self.session_summary(load_row(db, "export_session", key))

    def set_status(self, key, status, error=""):
        with closing(self.database()) as db, db:
            db.execute("UPDATE export_session SET status=?,error=? WHERE id=?", (status, error, key))

    def run(self, key, origin):
        status, error = "complete", ""
        try:
            with closing(self.database()) as db:
                row = load_row(db, "export_session", key)
                session = json.loads(row["payload"])
                folder = self.root / row["folder"]
                if folder.is_symlink() or not folder.is_dir():
                    raise ValueError("The original output folder is unavailable")
                class_file = folder / "classes.txt"
                if class_file.is_symlink() or class_file.read_text(encoding="utf-8").splitlines() != session["classes"]:
                    raise ValueError("The session class map has changed")
                with renderer(origin) as capture:
                    for index in range(row["completed"], session["count"]):
                        if load_row(db, "export_session", key)["status"] == "pausing":
                            status = "paused"
                            break
                        target = folder / f"{index+1:06d}"
                        if target.exists():
                            # A restart can land between the atomic publish and the progress update.
                            if (target.is_symlink() or not target.is_dir()
                                    or {path.name for path in target.iterdir()} != expected_files(session["layouts"])
                                    or any(path.is_symlink() or not path.is_file() or not path.stat().st_size
                                           for path in target.iterdir())):
                                raise ValueError(f"Incomplete existing instance {target.name}. Move it aside and resume.")
                        else:
                            with TemporaryDirectory(prefix=".stage-", dir=folder) as temporary:
                                stage = Path(temporary) / "instance"
                                stage.mkdir()
                                render_instance(capture, session, index, stage)
                                stage.rename(target)
                        with db:
                            db.execute("UPDATE export_session SET completed=? WHERE id=?", (index + 1, key))
        except Exception as exc:
            logger.exception("Card export session %s failed", key)
            status, error = "failed", str(exc)
        finally:
            with self.lock:
                self.workers.pop(key, None)
                self.set_status(key, status, error)

    def instance_path(self, key, index):
        with closing(self.database()) as db:
            row = load_row(db, "export_session", key)
            if not 1 <= index <= row["completed"]:
                raise HTTPException(404, "Instance is not available")
            payload = json.loads(row["payload"])
            folder = self.root / row["folder"] / f"{index:06d}"
        if folder.resolve() != folder or not folder.is_dir():
            raise HTTPException(404, "Instance folder is not available")
        return folder, expected_files(payload["layouts"])

    def instance(self, key: int, index: int):
        folder, filenames = self.instance_path(key, index)
        return {"path": str(folder), "files": sorted(filenames)}

    def file(self, key: int, index: int, filename: str):
        folder, filenames = self.instance_path(key, index)
        path = folder / filename
        if filename not in filenames or path.is_symlink() or not path.is_file():
            raise HTTPException(404, "Export file is not available")
        return FileResponse(path)
