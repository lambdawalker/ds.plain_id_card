"""Versioned small variations; structural layout is always an explicit template."""
import random

SCHEMA_V1 = {'margin': [24, 32], 'font_size': [25, 28], 'label_size': [15, 17],
             'field_gap': [10, 14], 'column_gap': [28, 36], 'header_gap': [20, 26],
             'watermark_opacity': [.08, .14], 'dates_reversed': [0, 1]}


def resolve_parameters(template_id, seed, version=1):
    if template_id not in ('A-left', 'A-right'):
        return None
    if version != 1:
        raise ValueError(f'Unsupported template variation version: {version}')
    rng = random.Random(f'template-variation:{version}:{template_id}:{seed}')
    values = {key: rng.randint(*bounds) for key, bounds in SCHEMA_V1.items()
              if key != 'watermark_opacity'}
    values['watermark_opacity'] = round(rng.uniform(*SCHEMA_V1['watermark_opacity']), 3)
    return {'version': version, 'seed': seed, 'template_id': template_id, **values}


def balanced_variants(template_ids, count, seed=0):
    if not template_ids:
        raise ValueError('Bundle has no templates')
    return [template_ids[(seed + index) % len(template_ids)] for index in range(count)]
