"""Render a sequence with the IDE renderer. Each finished sample is an atomic directory."""
import argparse
from contextlib import closing
import json
import os
import platform
import re
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen
from batch_manifest import atomic_write, digest, json_bytes, report, verified_record
from backend.template_variations import balanced_variants

from playwright.sync_api import sync_playwright


def fetch(server, path, data=None):
    request = Request(server.rstrip("/") + path, data=json_bytes(data) if data is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        return response.read()


def resolved_inputs(server, design, asset_cache):
    payload = fetch(server, "/api/preview", {"design": design})
    preview = json.loads(payload)
    paths = set(re.findall(r"/api/(?:photos|artwork)/[\w.-]+", preview["template"] + preview["css"]))
    portrait = (preview.get("person") or {}).get("photo_url")
    if portrait and portrait.startswith("/api/photos/"):
        paths.add(portrait)
    if design["background"].startswith("asset-"):
        paths.add("/api/artwork/" + design["background"][6:])
    for path in sorted(paths):
        if path not in asset_cache:
            asset_cache[path] = digest(fetch(server, path))
    return {"preview_sha256": digest(json_bytes(preview)),
            "assets": {path: asset_cache[path] for path in sorted(paths)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8765")
    parser.add_argument("--bundle", required=True, type=int, help="Numeric ID of a saved bundle")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--seed", type=int, help="First seed (defaults to the first saved layout)")
    parser.add_argument("--samples", nargs="+", help="Sample IDs to cycle through")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true", help="Reuse verified samples from this exact run")
    args = parser.parse_args()
    if args.count < 1 or args.seed is not None and not 0 <= args.seed < 2**32:
        parser.error("Expected a positive count and an unsigned 32-bit seed")
    bundle = json.loads(fetch(args.server, f"/api/bundles/{args.bundle}"))
    first = bundle["layouts"][0]["seed"] if args.seed is None else args.seed
    sequence = balanced_variants(list(range(len(bundle["layouts"]))), args.count, first)
    args.output.mkdir(parents=True, exist_ok=True)
    # SQLite releases the run lock automatically if the process is interrupted.
    with closing(sqlite3.connect(args.output / ".batch-lock.sqlite", timeout=0)) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        run(args, bundle, first, sequence)


def run(args, bundle, first, sequence):
    output = args.output
    manifest_path = output / "manifest.json"
    if manifest_path.exists() != args.resume:
        raise SystemExit("Use --resume for an existing manifest, or choose a fresh output directory.")
    if not args.resume and any(output.glob("[0-9][0-9]*")):
        raise SystemExit("Existing samples have no manifest. Choose a fresh output directory.")
    designs = []
    plans = []
    asset_cache = {}
    for index, layout_index in enumerate(sequence):
        item = dict(bundle["layouts"][layout_index])
        item["seed"] = (first + index) % 2**32
        if args.samples:
            item["sample"] = args.samples[index % len(args.samples)]
        designs.append(item)
        try:
            inputs = resolved_inputs(args.server, item, asset_cache)
        except (OSError, ValueError) as error:
            raise SystemExit(f"Preflight failed at sample {index} ({item['sample']}): {error}") from error
        plans.append({"index": index, "seed": item["seed"], "sample": item["sample"],
                      "layout_index": layout_index, "template_id": item.get("template_id"), **inputs})

    annotations = Path(__file__).with_name("batch_annotations.js").read_text(encoding="utf-8")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=os.getenv("CARD_CHROMIUM") or None)
        engine = browser.new_page()
        engine.goto(args.server)
        engine.wait_for_function("typeof window.cardDesignerRender === 'function'")
        scripts = engine.locator("script[src], link[rel=modulepreload]").evaluate_all(
            "nodes => nodes.map(node => node.src || node.href)"
        )
        static = {}
        for url in sorted(set(scripts)):
            response = engine.request.get(url)
            if not response.ok:
                raise RuntimeError(f"Could not fingerprint renderer asset {url}")
            static[url] = digest(response.body())
        manifest = {"version": 2, "bundle": bundle, "plans": plans,
                    "browser": browser.version, "platform": platform.platform(), "renderer": static,
                    "exporter_sha256": digest(Path(__file__).read_bytes()),
                    "annotations_sha256": digest(annotations.encode()),
                    "manifest_code_sha256": digest(Path(__file__).with_name("batch_manifest.py").read_bytes())}
        if args.resume:
            if json.loads(manifest_path.read_bytes()) != manifest:
                raise SystemExit("Run inputs changed. Use a fresh output directory.")
        else:
            atomic_write(manifest_path, json_bytes(manifest))
        page = browser.new_page()
        page.goto(args.server + "/api/samples")
        records, failures = [], []
        for item, plan in zip(designs, plans):
            index = plan["index"]
            target = output / f"{index:06d}"
            try:
                if target.exists():
                    record = verified_record(target, plan)
                    if record is None:
                        raise ValueError("Existing sample failed integrity verification. Move it aside and resume.")
                else:
                    record = render_sample(engine, page, item, plan, annotations, target, args.bundle)
                records.append((index, record))
            except Exception as error:
                failures.append({"index": index, "sample": plan["sample"], "error": str(error)})
        report(output, records, failures)
        browser.close()
    print(f"{len(records)} complete, {len(failures)} failed. See {output / 'validation.json'}.")
    if failures:
        raise SystemExit(1)


def render_sample(engine, page, design, plan, annotations, target, saved_bundle_id):
    width, height = design.get("width", 640), design.get("height", 400)
    page.set_viewport_size({"width": width, "height": height})
    rendered = engine.evaluate("d => window.cardDesignerRender(d, {scale: 1, pixelRatio: 1})", design)
    page.set_content(f'<iframe sandbox="" style="display:block;border:0;width:{width}px;height:{height}px"></iframe><style>body{{margin:0}}</style>')
    page.locator("iframe").evaluate("(frame, html) => frame.srcdoc = html", rendered["html"])
    card = page.frame_locator("iframe").locator("#view-port")
    card.wait_for()
    measured = card.evaluate(annotations)
    context = rendered.get("context", {})
    selection = context.get("template_selection", {})
    person = rendered.get("person") or {}
    portrait = person.get("photo_url")
    identity = plan["assets"].get(portrait) or person.get("id")
    split = None if not identity else (
        "validation" if int(digest(identity.encode())[:8], 16) % 5 == 0 else "train"
    )
    issues = []
    if context.get("missing") or measured["broken_images"]:
        issues.append("missing_asset")
    if any(word["clipped"] for word in measured["words"]):
        issues.append("clipped_text")
    if not measured["words"]:
        issues.append("no_text")
    if not identity:
        issues.append("unresolved_identity")
    with TemporaryDirectory(dir=target.parent) as temp:
        stage = Path(temp) / "sample"
        stage.mkdir()
        image = card.screenshot(path=str(stage / "card.png"), animations="disabled")
        record = dict(bundle_id=saved_bundle_id, layout_index=plan["layout_index"],
                      layout_name=design["name"], template_bundle_id=selection.get("bundle_id"),
                      template_id=selection.get("template_id"),
                      template_parameters=selection.get("parameters"),
                      seed=plan["seed"], sample=plan["sample"], background=design["background"],
                      background_recipe=rendered.get("background_recipe"),
                      theme=context.get("env", {}).get("theme", {}), plan=plan,
                      image_sha256=digest(image), width=width, height=height,
                      identity=identity, split=split, issues=issues,
                      missing=context.get("missing", []), **measured)
        record["record_sha256"] = digest(json_bytes(record))
        (stage / "record.json").write_bytes(json_bytes(record))
        stage.rename(target)
    return record


if __name__ == "__main__":
    main()
