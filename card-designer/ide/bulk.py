"""Render a sequence with the IDE renderer. Each finished sample is an atomic directory."""
import argparse
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8765")
    parser.add_argument("--design", required=True, type=int)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--seed", type=int, help="First seed (defaults to saved design)")
    parser.add_argument("--samples", nargs="+", help="Sample IDs to cycle through")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.count < 1 or args.seed is not None and not 0 <= args.seed < 2**32:
        parser.error("Expected a positive count and an unsigned 32-bit seed")
    with urlopen(f"{args.server}/api/designs/{args.design}") as response:
        design = json.load(response)
    samples = args.samples or [design["sample"]]
    first = design["seed"] if args.seed is None else args.seed
    args.output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=os.getenv("CARD_CHROMIUM") or None)
        engine = browser.new_page()
        engine.goto(args.server)
        engine.wait_for_function("typeof window.cardDesignerRender === 'function'")
        page = browser.new_page(viewport={"width": 640, "height": 400})
        page.goto(args.server + "/api/samples")
        for i in range(args.count):
            target = args.output / f"{i:06d}"
            if target.exists():
                raise FileExistsError(target)
            design.update(seed=(first + i) % 2**32, sample=samples[i % len(samples)])
            html = engine.evaluate("async d => (await window.cardDesignerRender(d)).html", design)
            page.set_content('<iframe sandbox="" style="display:block;border:0;width:640px;height:400px"></iframe><style>body{margin:0}</style>')
            page.locator("iframe").evaluate("(frame, html) => frame.srcdoc = html", html)
            card = page.frame_locator("iframe").locator("#view-port")
            card.wait_for()
            card.evaluate("async root => {await Promise.all([...root.querySelectorAll('img[src]:not([src=\\\"\\\"])')].map(img=>img.decode())); await document.fonts.ready}")
            boxes = card.evaluate("""root => {
                const origin = root.getBoundingClientRect();
                return Array.from(root.querySelectorAll('[data-class]')).filter(el=>el.getClientRects().length).map(el=>{
                  const b=el.getBoundingClientRect();
                  return {class:el.dataset.class,bbox:[b.left-origin.left,b.top-origin.top,b.right-origin.left,b.bottom-origin.top]};
                })}""")
            with TemporaryDirectory(dir=args.output) as temp:
                stage = Path(temp) / "sample"
                stage.mkdir()
                card.screenshot(path=str(stage / "card.png"))
                (stage / "record.json").write_text(json.dumps(dict(design_id=args.design,
                    seed=design["seed"], sample=design["sample"], elements=boxes), indent=2))
                stage.rename(target)
        browser.close()


if __name__ == "__main__":
    main()
