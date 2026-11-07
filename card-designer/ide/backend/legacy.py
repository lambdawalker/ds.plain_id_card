"""Static example layout used by the IDE's Open template A action."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "records" / "layouts"


def sample_template():
    return {"template": (ROOT / "a.html").read_text(encoding="utf-8"),
            "css": (ROOT / "a.css").read_text(encoding="utf-8")}
