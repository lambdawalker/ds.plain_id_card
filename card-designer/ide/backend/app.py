import os
import logging
from hashlib import sha256
import random
import sqlite3
from contextlib import closing
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
from threading import RLock
from .photo_dataset import metadata, photo_index
from .legacy import sample_template
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv("CARD_DATA", ROOT / "data"))
DATA.mkdir(parents=True, exist_ok=True)
DB = DATA / "designs.sqlite"
app = FastAPI()
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


# Keep file paths private. A sample key remains stable when new images are added.
photos = {}
if os.getenv("CARD_PHOTOS"):
    folder = Path(os.environ["CARD_PHOTOS"]).resolve(strict=True)
    for file in sorted(folder.rglob("*")):
        if file.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} and file.is_file() and file.resolve().is_relative_to(folder):
            key = sha256(file.relative_to(folder).as_posix().encode()).hexdigest()
            photos[key] = file

records = {}
records.update({key: {"id": key, "name": file.stem, "photo_url": "/api/photos/" + key}
                for key, file in photos.items()})


photo_fields = {}
photo_field_types = {}
base_records = dict(records)
base_photos = dict(photos)
source_lock = RLock()
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


def rebuild_sources():
    global records, photos, photo_fields, photo_field_types
    next_records, next_photos, next_fields = dict(base_records), dict(base_photos), {}
    sources = []
    next_types = {}
    with closing(database()) as db:
        rows = db.execute("SELECT id, reference FROM dataset_source ORDER BY id").fetchall()
    for row in rows:
        info = {"id": row[0], "reference": row[1], "count": 0, "error": ""}
        try:
            entries = index_reference(row[1])
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
    records, photos, photo_fields, photo_field_types = next_records, next_photos, next_fields, next_types
    return sources


class DatasetReference(BaseModel):
    reference: str = Field(min_length=1, max_length=2048)


@app.get("/api/datasets")
def datasets():
    with source_lock:
        return rebuild_sources()


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


source = os.getenv("CARD_PHOTO_DATASET")
if not source and os.getenv("DS"):
    candidate = Path(os.environ["DS"]) / "ds.photo_id"
    if (candidate / "manifest.yaml").is_file():
        source = str(candidate)
with closing(database()) as db, db:
    db.execute("CREATE TABLE IF NOT EXISTS source_migration(id INTEGER PRIMARY KEY)")
    if not db.execute("SELECT 1 FROM source_migration").fetchone():
        if source:
            db.execute("INSERT OR IGNORE INTO dataset_source(reference) VALUES (?)", (source,))
        db.execute("INSERT INTO source_migration VALUES (1)")
rebuild_sources()


@app.get("/api/samples")
def samples(q: str = "", offset: int = Query(default=0, ge=0), source_id: int | None = None):
    from itertools import islice
    matches = (r for r in list(records.values()) if (source_id is None or r.get("source_id") == source_id) and q.casefold() in (r["name"] + r["id"]).casefold())
    return list(islice(matches, offset, offset + 24))


@app.get("/api/photos/{key}")
def photo(key: str):
    if key not in photos:
        raise HTTPException(404, "Unknown photo")
    return FileResponse(photos[key])


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


@app.post("/api/preview")
def preview(request: Preview):
    d = request.design
    person = records.get(d.sample) if d.sample else {}
    if person is None:
        raise HTTPException(422, "Sample unavailable. Choose another record.")
    if d.sample in photo_fields:
        try:
            person = {**person, **metadata(photo_fields[d.sample], photo_field_types[d.sample])}
        except (OSError, ValueError) as exc:
            logger.exception("Dataset metadata failed: dataset=%r record=%r error=%s: %s",
                             person["dataset"], person["record_id"], type(exc).__name__, exc)
            raise HTTPException(422, f"Invalid metadata for dataset {person['dataset']}, record {person['record_id']}: {exc}") from exc
    rng = random.Random(d.seed)
    env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
    env.filters["random"] = rng.choice
    result = {}
    for field in ("template", "css"):
        try:
            result[field] = env.from_string(getattr(d, field)).render(person=person, seed=d.seed, data=person)
        except (TemplateError, IndexError, ArithmeticError, ValueError, TypeError) as exc:
            raise HTTPException(422, f"{field}, line {getattr(exc, 'lineno', '?')}: {exc}") from exc
    return {**result, "person": person}


app.mount("/backgrounds", StaticFiles(directory=ROOT.parent / "backgrounds"))
app.mount("/", StaticFiles(directory=ROOT / "web/dist", html=True))
