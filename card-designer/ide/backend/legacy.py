"""Deterministic preview values for the helpers used by the repository's A template."""
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace as NS

ROOT = Path(__file__).resolve().parents[3] / "templates/id_cards/A"


def helpers(rng):
    def length(size):
        return rng.randint(*size) if isinstance(size, tuple) else size

    def word(size):
        return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(length(size))).title()

    def value(text):
        return {"data": text}

    return NS(
        text=NS(word=word, sentence=lambda size, p=False: " ".join(word((3, 8)) for _ in range(length(size)))),
        random=NS(boolean=lambda: bool(rng.getrandbits(1)), lf_number=rng.randint,
                  hex=lambda n: "".join(rng.choice("0123456789ABCDEF") for _ in range(n))),
        time=NS(date=lambda: value(date(1970, 1, 1) + timedelta(days=rng.randrange(18000))),
                year_as_number=lambda: value(rng.randint(1990, 2030))),
        address=NS(number=lambda: value(rng.randint(1, 999)), block=lambda: value(word(7)),
                   city=lambda: value(word(6)), state=lambda: value(word(5))))


def sample_template():
    raw = (ROOT / "index.html.j2").read_text()
    body = raw.split('<div id="view-port">', 1)[1].rsplit("</div>", 1)[0]
    body = body.replace('/ds/lambdaWalker/ds.photo_id/body/{{ data.id }}/image', '{{ person.photo_url }}')
    base = """body{font:13px system-ui;color:#193837}.card{padding:18px}.card-body,.data-group,.key-curp{display:flex;gap:12px}.main-photo{width:125px;height:165px;object-fit:cover}.small-photo{position:absolute;top:10px;width:40px;height:48px;object-fit:cover}.data-section{flex:1}p,h1,h2{margin:2px}label{font-size:10px}"""
    return {"template": body, "css": base + "\n" + (ROOT / "style.css.j2").read_text()}
