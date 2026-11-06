import os
import asyncio
import logging
import json
from hashlib import sha256
import random
import sqlite3
import secrets
from uuid import uuid4
from contextlib import closing, asynccontextmanager, contextmanager, suppress
from concurrent.futures import Future, TimeoutError as FutureTimeout
from queue import Queue, Empty
from pathlib import Path

import yaml

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment
from .path_resolver import resolve
from .exporter import Exporter
from .record_files import atomic_write, counter_value, legacy_sequence
from threading import RLock, Thread, Event
from .photo_dataset import metadata, photo_index
from .legacy import sample_template
from .template_catalog import template_catalog, template_source, template_bundles
from .card_fonts import embedded_card_font
from .template_variations import resolve_parameters
from .template_context import project_context, requirements
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv("CARD_DATA", ROOT / "data"))
DATA.mkdir(parents=True, exist_ok=True)
DB = DATA / "designs.sqlite"
@asynccontextmanager
async def lifespan(app):
    global dataset_loading, dataset_load_error, active_loader_stop, catalog
    exporter.initialize()
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
    auto_theme: bool = False
    text_color: str = Field(default="#000000", pattern=r"^#[0-9a-fA-F]{6}$")
    layout_id: str | None = Field(default=None, max_length=64)
    template_id: str | None = None
    bundle_id: str | None = None
    variation_version: int = 1
    template_parameters: dict | None = None
    common: dict = Field(default_factory=dict)


def prepare_design(design):
    try:
        design.template_parameters = resolve_parameters(design.template_id, design.seed, design.variation_version)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if design.template_parameters is not None:
        design.bundle_id = template_source(design.template_id)['bundle_id']
    return design


class Preview(BaseModel):
    design: Design


def database():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    return db


class Bundle(BaseModel):
    name: str = Field(min_length=1, max_length=160, pattern=r"\S")
    layouts: list[Design] = Field(min_length=1)


def insert_bundle(db, bundle):
    return db.execute("INSERT INTO bundle(payload) VALUES (?)", (bundle.model_dump_json(),)).lastrowid


def initialize_bundles(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "bundle" in tables:
        return
    db.execute("CREATE TABLE bundle(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)")
    if "design" in tables:
        for row in db.execute("SELECT payload FROM design ORDER BY id"):
            layout = Design.model_validate_json(row[0])
            insert_bundle(db, Bundle(name=layout.name if layout.name.strip() else "Imported bundle", layouts=[layout]))
    for bundle in template_bundles():
        insert_bundle(db, stock_bundle(bundle))


def stock_bundle(bundle):
    defaults = {"seed": 42, "color": "#a4d5c8", "background": "type-b", "auto_theme": True,
                **bundle.get("defaults", {})}
    layouts = [Design(**template_source(item["id"]), **defaults) for item in bundle["templates"]]
    saved = Bundle(name=bundle["name"], layouts=layouts)
    for layout in saved.layouts:
        layout.layout_id = f"stock-{layout.template_id}"
    return saved


def upgrade_stock_layouts(db):
    """Replace known stock sources once, leaving user-edited source pairs alone."""
    if db.execute("PRAGMA user_version").fetchone()[0] >= 1:
        return
    fingerprints = json.loads(Path(__file__).with_name("stock_layout_v1.json").read_text(encoding="utf-8"))
    sources = {}
    for key, payload in db.execute("SELECT id, payload FROM bundle").fetchall():
        bundle = Bundle.model_validate_json(payload)
        changed = False
        for layout in bundle.layouts:
            expected = fingerprints.get(layout.template_id)
            if expected is None:
                continue
            actual = [sha256(getattr(layout, field).encode()).hexdigest() for field in ("template", "css")]
            if actual != expected:
                continue
            if layout.template_id not in sources:
                sources[layout.template_id] = template_source(layout.template_id)
            source = sources[layout.template_id]
            layout.template, layout.css = source["template"], source["css"]
            changed = True
        if changed:
            db.execute("UPDATE bundle SET payload=? WHERE id=?", (bundle.model_dump_json(), key))
    db.execute("PRAGMA user_version = 1")


def assign_layout_ids(bundle, legacy_key=None):
    seen = set()
    for index, layout in enumerate(bundle.layouts):
        if not layout.layout_id or layout.layout_id in seen:
            layout.layout_id = uuid4().hex if legacy_key is None else f"legacy-{legacy_key}-{index}"
        seen.add(layout.layout_id)


def upgrade_legacy_bundles(db):
    initialize_bundles(db)
    upgrade_stock_layouts(db)
    if db.execute("PRAGMA user_version").fetchone()[0] < 2:
        for key, payload in db.execute("SELECT id,payload FROM bundle").fetchall():
            bundle = Bundle.model_validate_json(payload)
            assign_layout_ids(bundle, legacy_key=key)
            db.execute("UPDATE bundle SET payload=? WHERE id=?", (bundle.model_dump_json(), key))
        db.execute("PRAGMA user_version=2")
    if db.execute("PRAGMA user_version").fetchone()[0] < 3:
        installed = db.execute(
            "SELECT 1 FROM bundle, json_each(bundle.payload, '$.layouts') AS layout "
            "WHERE json_extract(layout.value, '$.template_id') = 'GA-front' LIMIT 1"
        ).fetchone()
        if installed is None:
            insert_bundle(db, stock_bundle(next(item for item in template_bundles() if item["id"] == "georgia-a")))
        db.execute("PRAGMA user_version=3")


class BundleDumper(yaml.SafeDumper):
    pass


BundleDumper.add_representer(
    str, lambda dumper, value: dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None
    )
)


def records_root():
    return Path(os.getenv("CARD_RECORDS", ROOT / "records"))


def bundle_root():
    root = records_root() / "design-bundles"
    if root.is_symlink():
        raise RuntimeError(f"Bundle storage must be a directory: {root}")
    return root


def bundle_file(key):
    folder = bundle_root() / str(key)
    path = folder / "bundle.yaml"
    if key <= 0 or folder.is_symlink() or path.is_symlink():
        raise HTTPException(404, "Bundle was deleted")
    return path


def write_bundle(key, bundle):
    atomic_write(bundle_file(key), yaml.dump(bundle.model_dump(mode="json"), Dumper=BundleDumper,
                                            allow_unicode=True, sort_keys=False, width=120))


def bundle_keys(root=None):
    root = bundle_root() if root is None else root
    names = (path.parent.name for path in root.glob("*/bundle.yaml")
             if not path.parent.is_symlink() and not path.is_symlink() and path.is_file())
    return sorted(int(name) for name in names if name.isascii() and name.isdecimal() and name[0] != "0")


def initialize_bundle_storage():
    with closing(database()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        version = db.execute("PRAGMA user_version").fetchone()[0]
        counter = bundle_root() / ".next-id"
        if version >= 5 and (counter.exists() or bundle_keys()):
            return
        previous = DB.parent / "bundles"
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        sequence = 0
        if version == 4:
            records = [(key, read_bundle_file(previous / str(key) / "bundle.yaml"))
                       for key in bundle_keys(previous)]
            sequence = counter_value(previous) - 1
        elif version < 4 and tables.intersection({"bundle", "design"}):
            upgrade_legacy_bundles(db)
            records = [(key, Bundle.model_validate_json(payload))
                       for key, payload in db.execute("SELECT id,payload FROM bundle ORDER BY id")]
            sequence = legacy_sequence(db, "bundle")
        elif counter.exists() or bundle_keys():
            records = []
        else:
            records = [(key, stock_bundle(item)) for key, item in enumerate(template_bundles(), 1)]
        for key, bundle in records:
            if not bundle_file(key).exists():
                write_bundle(key, bundle)
            elif load_bundle(key) != bundle:
                raise RuntimeError(f"Design bundle {key} differs between legacy storage and {bundle_file(key)}")
        next_id = max(next_bundle_id(), sequence + 1)
        atomic_write(counter, f"{next_id}\n")
        if version < 5:
            db.execute("PRAGMA user_version=5")


def next_bundle_id():
    return max(counter_value(bundle_root()), max(bundle_keys(), default=0) + 1)


def add_bundle(bundle):
    key = next_bundle_id()
    atomic_write(bundle_root() / ".next-id", f"{key + 1}\n")
    write_bundle(key, bundle)
    return key


def read_bundle_file(path):
    try:
        bundle = Bundle.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        ids = {layout.layout_id for layout in bundle.layouts}
        if ids.intersection({None, ""}) or len(ids) != len(bundle.layouts):
            raise ValueError("Each layout must have a unique, non-empty layout_id")
        # Match the JSON API types for YAML scalars such as unquoted dates.
        return Bundle.model_validate_json(bundle.model_dump_json())
    except FileNotFoundError as exc:
        raise HTTPException(404, "Bundle was deleted") from exc
    except (yaml.YAMLError, ValueError, OSError) as exc:
        logger.warning("Invalid bundle file %s: %s", path, exc)
        raise HTTPException(422, f"Invalid bundle file: {path}") from exc


def load_bundle(key):
    return read_bundle_file(bundle_file(key))


initialize_bundle_storage()


def bundle_response(key, bundle):
    return {"id": key, **bundle.model_dump()}


def prepare_bundle(bundle):
    bundle.layouts = [layout.model_copy() for layout in bundle.layouts]
    assign_layout_ids(bundle)
    bundle.layouts = [prepare_design(layout) for layout in bundle.layouts]
    return bundle


@app.get("/api/bundles")
def list_bundles():
    items = []
    for key in bundle_keys():
        try:
            bundle = load_bundle(key)
        except HTTPException as exc:
            if exc.status_code == 422:
                items.append(dict(id=key, name=f"Invalid bundle ({key})", layout_count=0))
            continue
        items.append(dict(id=key, name=bundle.name, layout_count=len(bundle.layouts)))
    return items


@app.get("/api/bundles/{key}")
def read_bundle(key: int):
    return bundle_response(key, prepare_bundle(load_bundle(key)))


@app.post("/api/bundles")
def create_bundle(bundle: Bundle):
    bundle = prepare_bundle(bundle)
    with closing(database()) as db, db:
        # Serialize file writes across Studio processes using the existing database lock.
        db.execute("BEGIN IMMEDIATE")
        return bundle_response(add_bundle(bundle), bundle)


@app.put("/api/bundles/{key}")
def update_bundle(key: int, bundle: Bundle):
    bundle = prepare_bundle(bundle)
    with closing(database()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        if not bundle_file(key).is_file():
            raise HTTPException(404, "Bundle was deleted")
        write_bundle(key, bundle)
    return bundle_response(key, bundle)


@app.post("/api/bundles/{key}/clone")
def clone_bundle(key: int, draft: Bundle | None = None):
    with closing(database()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        original = load_bundle(key)
        copy = prepare_bundle((draft if draft is not None else original).model_copy(deep=True))
        copy.name = copy.name[:155] + " copy"
        return bundle_response(add_bundle(copy), copy)


@app.delete("/api/bundles/{key}")
def delete_bundle(key: int):
    with closing(database()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        path = bundle_file(key)
        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise HTTPException(404, "Bundle was deleted") from exc
        with suppress(OSError):
            path.parent.rmdir()
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
    if "position" not in {row[1] for row in db.execute("PRAGMA table_info(dataset_source)")}:
        db.execute("ALTER TABLE dataset_source ADD COLUMN position INTEGER NOT NULL DEFAULT 0")


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
        rows = db.execute("SELECT id, reference FROM dataset_source ORDER BY position, id").fetchall()
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
                info["image_fields"] = [name for name, kind in entries.field_types.items() if kind == "rawImage"]
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


class DatasetOrder(BaseModel):
    ids: list[int]


@app.put("/api/datasets/order")
def reorder_datasets(payload: DatasetOrder):
    global catalog
    with source_lock:
        with closing(database()) as db, db:
            existing = {row[0] for row in db.execute("SELECT id FROM dataset_source")}
            if len(payload.ids) != len(existing) or set(payload.ids) != existing:
                raise HTTPException(422, "Order must contain every dataset ID exactly once")
            db.executemany("UPDATE dataset_source SET position=? WHERE id=?", enumerate(payload.ids))
        ranks = {key: index for index, key in enumerate(payload.ids)}
        catalog = {**catalog, "sources": sorted(catalog["sources"], key=lambda source: ranks[source["id"]])}
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
                db.execute("INSERT OR IGNORE INTO dataset_source(reference, position) VALUES (?, (SELECT COALESCE(MAX(position), -1) + 1 FROM dataset_source))", (reference,))
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
            db.execute("INSERT OR IGNORE INTO dataset_source(reference, position) VALUES (?, (SELECT COALESCE(MAX(position), -1) + 1 FROM dataset_source))", (source,))
        db.execute("INSERT INTO source_migration VALUES (1)")


def pending_sources():
    with closing(database()) as db:
        return [{"id": row[0], "reference": row[1], "count": 0, "error": "", "loading": True}
                for row in db.execute("SELECT id, reference FROM dataset_source ORDER BY position, id")]


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


@app.get("/api/samples/random")
def random_sample(source_id: int | None = None, exclude: str = ""):
    records = [record for record in catalog["records"].values()
               if source_id is None or record.get("source_id") == source_id]
    if not records:
        raise HTTPException(404, "No records available in this dataset")
    alternatives = [record for record in records if record["id"] != exclude]
    return secrets.choice(alternatives or records)


@app.get("/api/samples/{key}")
def sample_details(key: str):
    snapshot = catalog
    record = snapshot["records"].get(key)
    if record is None:
        if dataset_loading:
            raise HTTPException(503, "Datasets are still loading. Please wait.")
        raise HTTPException(404, "Record unavailable. Choose another record.")
    try:
        kinds = snapshot["types"].get(key, {})
        values = metadata(snapshot["fields"].get(key, {}), kinds)
        for field, kind in kinds.items():
            if kind == "rawImage" and field in record.get("images", {}):
                values[field] = record["images"][field]
        return {"record": record, "fields": values, "field_types": kinds}
    except (OSError, ValueError) as exc:
        logger.exception("Dataset record fields failed: record=%r: %s", key, exc)
        raise HTTPException(422, f"Could not read fields for record {record.get('record_id', key)}: {exc}") from exc


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


@app.get("/api/template-bundles")
def list_template_bundles():
    return template_bundles()


@app.get("/api/template-bundles/{key}")
def open_bundle(key: str):
    bundle = next((bundle for bundle in template_bundles() if bundle['id'] == key), None)
    if bundle is None:
        raise HTTPException(404, 'Unknown template bundle')
    return bundle


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
    d = prepare_design(request.design)
    snapshot = catalog
    person = snapshot["records"].get(d.sample) if d.sample else {}
    if person is None:
        if dataset_loading:
            raise HTTPException(503, "Datasets are still loading. Please wait.")
        raise HTTPException(422, "Sample unavailable. Choose another record.")
    try:
        requested = requirements(d.template, d.css)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if d.sample in snapshot["fields"] and (requested is None or requested['compatibility'] or requested['identity'] or requested['card'] or requested['address']):
        try:
            person = {**person, **metadata(snapshot["fields"][d.sample], snapshot["types"][d.sample])}
        except (OSError, ValueError) as exc:
            logger.exception("Dataset metadata failed: dataset=%r record=%r error=%s: %s",
                             person["dataset"], person["record_id"], type(exc).__name__, exc)
            raise HTTPException(422, f"Invalid metadata for dataset {person['dataset']}, record {person['record_id']}: {exc}") from exc
    rng = random.Random(d.seed)
    try:
        render_context, context_summary = project_context(snapshot, d, person, rng)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.exception("Template context failed: template=%r sample=%r: %s", d.template_id, d.sample, exc)
        raise HTTPException(422, f"Could not build template context: {exc}") from exc
    render_context['variation'] = d.template_parameters or {}
    font_family = render_context['variation'].get('font_family')
    if font_family:
        render_context['card_font'] = embedded_card_font(font_family)
    context_summary['template_selection'] = {'bundle_id': d.bundle_id, 'template_id': d.template_id,
                                              'parameters': d.template_parameters}
    env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
    env.filters["random"] = rng.choice
    result = {}
    for field in ("template", "css"):
        try:
            result[field] = env.from_string(getattr(d, field)).render(**render_context)
        except (TemplateError, IndexError, ArithmeticError, ValueError, TypeError) as exc:
            raise HTTPException(422, f"{field}, line {getattr(exc, 'lineno', '?')}: {exc}") from exc
    if context_summary.get("projection") == "template":
        person = {key: person[key] for key in ("id", "record_id", "name", "dataset", "source_id", "photo_url") if key in person}
    return {**result, "person": person, "context": context_summary}


exporter = Exporter(database, load_bundle, lambda: catalog, Path(os.getenv("CARD_EXPORT_ROOT", DATA / "exports")), records_root)
app.include_router(exporter.router)

app.mount("/backgrounds", StaticFiles(directory=ROOT.parent / "backgrounds"))
app.mount("/", StaticFiles(directory=ROOT / "web/dist", html=True))
