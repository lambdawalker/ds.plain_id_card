"""Versioned small variations; structural layout is always an explicit template."""
import random

SCHEMA_V1 = {'margin': [24, 32], 'font_size': [25, 28], 'label_size': [15, 17],
             'field_gap': [10, 14], 'column_gap': [28, 36], 'header_gap': [20, 26],
             'watermark_opacity': [.08, .14], 'dates_reversed': [0, 1]}


TEMPLATE_IDS = ('A-left','A-right','B-left','B-right','C-top','C-bottom','D-left','D-right',
                'E-codes-top','E-codes-bottom','F-rail-left','F-rail-right','G-photo-left','G-photo-right')


def variation_schema(template_id, version=2):
    if version == 2:
        schema = {**SCHEMA_V1, 'margin':[32,40], 'font_size':[30,34], 'label_size':[17,19]}
        if template_id.startswith(('C-', 'D-')):
            schema.update(margin=[28,34], font_size=[24,27], label_size=[16,18])
        elif template_id.startswith(('E-', 'F-', 'G-')):
            schema.update(font_size=[27,30], label_size=[17,19])
        if template_id.startswith('D-'):
            schema.update(font_size=[32,34], label_size=[18,20])
        return schema
    if template_id.startswith(('C-', 'D-')):
        return {**SCHEMA_V1, 'margin':[24,30], 'font_size':[20,23], 'label_size':[14,16], 'field_gap':[8,12]}
    if template_id.startswith(('E-', 'F-', 'G-')):
        return {**SCHEMA_V1, 'margin':[24,28], 'font_size':[20,23], 'label_size':[14,16], 'field_gap':[8,12]}
    return SCHEMA_V1


def resolve_parameters(template_id, seed, version=2):
    if template_id not in TEMPLATE_IDS:
        return None
    if version not in (1,2):
        raise ValueError(f'Unsupported template variation version: {version}')
    rng = random.Random(f'template-variation:{version}:{template_id}:{seed}')
    schema = variation_schema(template_id, version)
    values = {key: rng.randint(*bounds) for key, bounds in schema.items()
              if key != 'watermark_opacity'}
    values['watermark_opacity'] = round(rng.uniform(*schema['watermark_opacity']), 3)
    if version == 2:
        values['photo_background'] = rng.choice(('transparent','rgba(255,255,255,0.55)','#ffffff'))
    return {'version': version, 'seed': seed, 'template_id': template_id, **values}


def balanced_variants(template_ids, count, seed=0):
    if not template_ids:
        raise ValueError('Bundle has no templates')
    return [template_ids[(seed + index) % len(template_ids)] for index in range(count)]
