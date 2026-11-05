"""Bundled faces from the visual study, pinned for reproducible card rendering."""
from base64 import b64encode
from functools import lru_cache
import json
from pathlib import Path
import random

FONT_ROOT = Path(__file__).resolve().parents[1] / 'fonts'
HORIZONTAL_FONT_POOL = json.loads((FONT_ROOT / 'horizontal-v1.json').read_text(encoding='utf-8'))
FONTS_BY_FAMILY = {font['family']: font for font in HORIZONTAL_FONT_POOL}


def horizontal_font_family(template_id, seed):
    rng = random.Random(f'horizontal-card-font:v1:{template_id}:{seed}')
    return rng.choice(HORIZONTAL_FONT_POOL)['family']


@lru_cache(maxsize=len(HORIZONTAL_FONT_POOL))
def embedded_card_font(family):
    font = FONTS_BY_FAMILY[family]
    binary = (FONT_ROOT / font['file']).read_bytes()
    return {'weight': font['weight'], 'url': 'data:font/ttf;base64,' + b64encode(binary).decode('ascii')}
