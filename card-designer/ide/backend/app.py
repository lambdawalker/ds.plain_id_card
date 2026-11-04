import os
import asyncio
import logging
from hashlib import sha256
import random
import sqlite3
from contextlib import closing, asynccontextmanager, contextmanager
from concurrent.futures import Future, TimeoutError as FutureTimeout
from queue import Queue, Empty
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment
from .path_resolver import resolve
from threading import RLock, Thread, Event
from .photo_dataset import metadata, photo_index
from .legacy import sample_template
from .template_catalog import template_catalog, template_source
from .render_context import build_context
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv("CARD_DATA", ROOT / "data"))
DATA.mkdir(parents=True, exist_ok=True)
DB = DATA / "designs.sqlite"
@asynccontextmanager
async def lifespan(app):
    global dataset_loading, dataset_load_error, active_loader_stop, catalog
    stop = Event()
    active_loader_stop = stop
    dataset_loading, dataset_load_error = True, ""
    # SQLite references are cheap; publish names before any dataset filesystem work.
    catalog = {**catalog, "sources": pending_sources()}
    worker = Thread(target=warm_datasets, args=(stop,), name="dataset-indexer", daemon=True)
    app.state.dataset_thread = worker
    worker.start()
    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(worker.join, 1)


app = FastAPI(lifespan=lifespan)
logger = logging.getLogger("uvicorn.error")


@app.exception_handler(StarletteHTTPException)
async def log_http_error(request: Request, exc: StarletteHTTPException):
    logger.warning("%s %s -> %s: %s", request.method, request.url.path, exc.status_code, exc.detail)
    return await http_exception_handler(request, exc)


@app.exception_handler(RequestValidationError)
async def log_validation_error(request: Request, exc: RequestValidationError):
    details = [{"loc": error["loc"], "msg": error["msg"], "type": error["type"]} for error in exc.errors()]
    logger.warning("%s %s -> 422: request validation failed: %s", request.method, request.url.path, details)
    return await request_validation_exception_handler(request, exc)


class Design(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    template: str
    css: str
    seed: int = Field(ge=0, le=4294967295)
    color: str = Field(pattern=r"^#[0-9a-fA-F]{6}$")
    background: str = Field(pattern=r"^(type-b|type-[abcd]-v1|plain|asset-[0-9a-f]{64})$")
    background_recipe: dict | None = None
    sample: str = ""
    width: int = Field(default=640, ge=160, le=4096)
    height: int = Field(default=400, ge=160, le=4096)
    template_id: str | None = None
    common: dict = Field(default_factory=dict)


class Preview(BaseModel):
    design: Design


def database():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    return db


with closing(database()) as db, db:
    db.execute("CREATE TABLE IF NOT EXISTS design(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")


def load(db, key):
    row = db.execute("SELECT payload FROM design WHERE id=?", (key,)).fetchone()
    if not row:
        raise HTTPException(404, "Design was deleted")
    return Design.model_validate_json(row[0])


def insert(db, design):
    return db.execute("INSERT INTO design(payload) VALUES (?)", (design.model_dump_json(),)).lastrowid


@app.get("/api/designs")
def designs():
    with closing(database()) as db:
        return [dict(id=row[0], name=row[1]) for row in db.execute(
            "SELECT id, json_extract(payload,'$.name') FROM design ORDER BY id")]


@app.get("/api/designs/{key}")
def read(key: int):
    with closing(database()) as db:
        return load(db, key)


@app.post("/api/designs")
def create(design: Design):
    with closing(database()) as db, db:
        return {"id": insert(db, design)}


@app.put("/api/designs/{key}")
def update(key: int, design: Design):
    with closing(database()) as db, db:
        load(db, key)
        db.execute("UPDATE design SET payload=? WHERE id=?", (design.model_dump_json(), key))
    return {"id": key}


@app.post("/api/designs/{key}/clone")
def clone(key: int):
    with closing(database()) as db, db:
        design = load(db, key)
        design.name = design.name[:155] + " copy"
        return {"id": insert(db, design)}


@app.delete("/api/designs/{key}")
def delete(key: int):
    with closing(database()) as db, db:
        load(db, key)
        db.execute("DELETE FROM design WHERE id=?", (key,))
    return {"deleted": key}


# Readers use a single immutable-by-convention snapshot while a worker builds the next one.
def load_loose_photos(stop):
    next_photos = {}
    if os.getenv("CARD_PHOTOS"):
        folder = Path(os.environ["CARD_PHOTOS"]).resolve(strict=True)
        for file in sorted(folder.rglob("*")):
            if stop.is_set():
                return {}, {}
            if file.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} and file.is_file() and file.resolve().is_relative_to(folder):
                key = sha256(file.relative_to(folder).as_posix().encode()).hexdigest()
                next_photos[key] = file
    next_records = {key: {"id": key, "name": file.stem, "photo_url": "/api/photos/" + key}
                    for key, file in next_photos.items()}
    return next_records, next_photos


base_records, base_photos = {}, {}
source_lock = RLock()
dataset_loading, dataset_load_error = False, ""
active_loader_stop = None
catalog = {"records": {}, "photos": {}, "fields": {}, "types": {}, "sources": []}
with closing(database()) as db, db:
    db.execute("CREATE TABLE IF NOT EXISTS dataset_source(id INTEGER PRIMARY KEY, reference TEXT UNIQUE NOT NULL)")


def index_reference(reference):
    resolved = None
    try:
        resolved = resolve(reference, make=False)
        return photo_index(resolved)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.exception("Dataset indexing failed: reference=%r resolved=%s error=%s: %s",
                         reference, resolved, type(exc).__name__, exc)
        raise


@contextmanager
def parallel_indices(rows):
    # Daemon workers do not hold process shutdown hostage to a stalled network drive.
    tasks, stopping = Queue(), Event()
    pending = {row[0]: Future() for row in rows}
    for row in rows:
        tasks.put(row)
    def scan():
        while not stopping.is_set():
            try:
                row = tasks.get_nowait()
            except Empty:
                return
            future = pending[row[0]]
            try:
                future.set_result(index_reference(row[1]))
            except Exception as exc:
                future.set_exception(exc)
    for i in range(min(8, len(rows))):
        Thread(target=scan, name=f"dataset-source-{i}", daemon=True).start()
    try:
        yield pending
    finally:
        stopping.set()


def index_result(future, stop):
    if stop is None:
        return future.result()
    while not stop.is_set():
        try:
            return future.result(timeout=.1)
        except FutureTimeout:
            if future.done():
                return future.result()
    return None


def rebuild_sources(stop=None):
    global catalog
    next_records, next_photos, next_fields = dict(base_records), dict(base_photos), {}
    sources = []
    next_types = {}
    with closing(database()) as db:
        rows = db.execute("SELECT id, reference FROM dataset_source ORDER BY id").fetchall()
    with parallel_indices(rows) as pending:
        for row in rows:
            if stop is not None and stop.is_set():
                return catalog["sources"]
            info = {"id": row[0], "reference": row[1], "count": 0, "error": ""}
            try:
                entries = index_result(pending[row[0]], stop)
                if entries is None:
                    return catalog["sources"]
                info["dataset"] = entries.dataset_id
                def record_key(stem):
                    # Keep existing photo selections stable; namespace other dataset IDs.
                    identity = stem if entries.dataset_id.casefold() == "lambdawalker/ds.photo_id" else entries.dataset_id + "\0" + stem
                    return "photo-" + sha256(identity.encode()).hexdigest()
                if any(record_key(stem) in next_fields for stem in entries):
                    raise ValueError("Record IDs overlap another connected dataset")
                for stem, fields in entries.items():
                    # Preserve saved sample IDs for the original photo dataset.
                    key = record_key(stem)
                    if key in next_fields:
                        raise ValueError("Record IDs overlap another connected dataset")
                    image_names = [name for name, kind in entries.field_types.items() if kind == "rawImage" and name in fields]
                    primary = "image" if "image" in image_names else image_names[0]
                    image_urls = {}
                    for name in image_names:
                        image_key = key if name == primary else "photo-" + sha256((key + "\0" + name).encode()).hexdigest()
                        image_urls[name] = "/api/photos/" + image_key
                        next_photos[image_key] = fields[name]
                    url = image_urls[primary]
                    # Keep identity/thumbnail fields stable even when a dataset uses those names.
                    reserved = {"id", "record_id", "name", "dataset", "source_id", "photo_url", "images"}
                    next_records[key] = {**{name: value for name, value in image_urls.items() if name not in reserved},
                        "id": key, "record_id": stem, "name": stem, "dataset": entries.dataset_id,
                        "source_id": row[0], "image": url, "photo_url": url, "images": image_urls}
                    next_fields[key] = fields
                    next_types[key] = entries.field_types
                info["count"] = len(entries)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                info["error"] = str(exc)
            sources.append(info)
    if stop is not None and (stop.is_set() or active_loader_stop is not stop):
        return catalog["sources"]
    catalog = {"records": next_records, "photos": next_photos, "fields": next_fields,
               "types": next_types, "sources": sources}
    return sources


class DatasetReference(BaseModel):
    reference: str = Field(min_length=1, max_length=2048)


@app.get("/api/datasets")
def datasets():
    return catalog["sources"]


@app.get("/api/datasets/status")
def dataset_status():
    return {"loading": dataset_loading, "error": dataset_load_error}


@app.post("/api/datasets")
def add_dataset(payload: DatasetReference):
    reference = payload.reference.strip()
    with source_lock:
        try:
            index_reference(reference)
            with closing(database()) as db, db:
                db.execute("INSERT OR IGNORE INTO dataset_source(reference) VALUES (?)", (reference,))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise HTTPException(422, str(exc)) from exc
        return rebuild_sources()


@app.delete("/api/datasets/{key}")
def remove_dataset(key: int):
    with source_lock:
        with closing(database()) as db, db:
            db.execute("DELETE FROM dataset_source WHERE id=?", (key,))
        return rebuild_sources()


with closing(database()) as db, db:
    db.execute("CREATE TABLE IF NOT EXISTS source_migration(id INTEGER PRIMARY KEY)")


def migrate_environment_source():
    with closing(database()) as db, db:
        if db.execute("SELECT 1 FROM source_migration").fetchone():
            return
        source = os.getenv("CARD_PHOTO_DATASET")
        if not source and os.getenv("DS"):
            candidate = Path(os.environ["DS"]) / "ds.photo_id"
            if (candidate / "manifest.yaml").is_file():
                source = str(candidate)
        if source:
            db.execute("INSERT OR IGNORE INTO dataset_source(reference) VALUES (?)", (source,))
        db.execute("INSERT INTO source_migration VALUES (1)")


def pending_sources():
    with closing(database()) as db:
        return [{"id": row[0], "reference": row[1], "count": 0, "error": "", "loading": True}
                for row in db.execute("SELECT id, reference FROM dataset_source ORDER BY id")]


def warm_datasets(stop):
    global base_records, base_photos, catalog, dataset_loading, dataset_load_error
    try:
        # Mutations serialize with warmup; cached reads never acquire this scan lock.
        with source_lock:
            migrate_environment_source()
            if stop.is_set():
                return
            catalog = {**catalog, "sources": pending_sources()}
            base_records, base_photos = load_loose_photos(stop)
            rebuild_sources(stop)
        if stop.is_set():
            return
        logger.info("Dataset index ready: %s sources, %s records", len(catalog["sources"]), len(catalog["records"]))
    except Exception as exc:
        logger.exception("Background dataset indexing failed: %s: %s", type(exc).__name__, exc)
        if active_loader_stop is stop:
            dataset_load_error = str(exc)
            catalog = {**catalog, "sources": [{**source, "loading": False, "error": str(exc)}
                                              for source in catalog["sources"]]}
    finally:
        if active_loader_stop is stop:
            dataset_loading = False


@app.get("/api/samples")
def samples(q: str = "", offset: int = Query(default=0, ge=0), source_id: int | None = None):
    from itertools import islice
    matches = (r for r in catalog["records"].values() if (source_id is None or r.get("source_id") == source_id) and q.casefold() in (r["name"] + r["id"]).casefold())
    return list(islice(matches, offset, offset + 24))


@app.get("/api/photos/{key}")
def photo(key: str):
    snapshot = catalog
    if key not in snapshot["photos"]:
        raise HTTPException(404, "Unknown photo")
    return FileResponse(snapshot["photos"][key])


ASSETS = DATA / "artwork"
ASSETS.mkdir(exist_ok=True)


@app.get("/api/artwork")
def artwork():
    return [{"id": "asset-" + p.stem, "name": p.read_text(encoding="utf8")}
            for p in sorted(ASSETS.glob("*.name"))]


@app.post("/api/artwork")
async def upload(request: Request, name: str = Query(max_length=180)):
    formats = {"image/png": "png", "image/webp": "webp", "image/jpeg": "jpg"}
    suffix = formats.get(request.headers.get("content-type"))
    if not suffix:
        raise HTTPException(415, "Choose a PNG, JPEG or WebP image")
    data = bytearray()
    async for chunk in request.stream():
        data.extend(chunk)
        if len(data) > 12 * 1024 * 1024:
            raise HTTPException(413, "Image exceeds 12 MiB")
    key = sha256(data).hexdigest()
    (ASSETS / f"{key}.{suffix}").write_bytes(data)
    (ASSETS / f"{key}.name").write_text(name, encoding="utf8")
    return {"id": "asset-" + key}


@app.get("/api/artwork/{key}")
def image(key: str):
    import re
    if re.fullmatch("[0-9a-f]{64}", key):
        for ext in ("png", "webp", "jpg"):
            file = ASSETS / f"{key}.{ext}"
            if file.is_file():
                return FileResponse(file)
    raise HTTPException(404, "Unknown artwork")


@app.get("/api/templates/a")
def template_a():
    return sample_template()


@app.get("/api/templates")
def list_templates():
    return template_catalog()


@app.get("/api/templates/{key}")
def open_template(key: str):
    try:
        return template_source(key)
    except KeyError as exc:
        raise HTTPException(404, "Unknown template") from exc


@app.post("/api/preview")
def preview(request: Preview):
    d = request.design
    snapshot = catalog
    person = snapshot["records"].get(d.sample) if d.sample else {}
    if person is None:
        if dataset_loading:
            raise HTTPException(503, "Datasets are still loading. Please wait.")
        raise HTTPException(422, "Sample unavailable. Choose another record.")
    if d.sample in snapshot["fields"]:
        try:
            person = {**person, **metadata(snapshot["fields"][d.sample], snapshot["types"][d.sample])}
        except (OSError, ValueError) as exc:
            logger.exception("Dataset metadata failed: dataset=%r record=%r error=%s: %s",
                             person["dataset"], person["record_id"], type(exc).__name__, exc)
            raise HTTPException(422, f"Invalid metadata for dataset {person['dataset']}, record {person['record_id']}: {exc}") from exc
    rng = random.Random(d.seed)
    try:
        render_context, context_summary = build_context(snapshot, d, person, rng)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.exception("Template context failed: template=%r sample=%r: %s", d.template_id, d.sample, exc)
        raise HTTPException(422, f"Could not build template context: {exc}") from exc
    env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
    env.filters["random"] = rng.choice
    result = {}
    for field in ("template", "css"):
        try:
            result[field] = env.from_string(getattr(d, field)).render(**render_context)
        except (TemplateError, IndexError, ArithmeticError, ValueError, TypeError) as exc:
            raise HTTPException(422, f"{field}, line {getattr(exc, 'lineno', '?')}: {exc}") from exc
    return {**result, "person": person, "context": context_summary}


app.mount("/backgrounds", StaticFiles(directory=ROOT.parent / "backgrounds"))
app.mount("/", StaticFiles(directory=ROOT / "web/dist", html=True))
