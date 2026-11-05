"""Bundle registry and editable template sources, with a legacy A alias."""
from pathlib import Path
import re
import yaml
from .template_context import ASSET_SOURCES
from .template_variations import SCHEMA_V1

ROOT = Path(__file__).resolve().parents[1] / 'templates' / 'id_cards'


def bundle_registry():
    return yaml.safe_load((ROOT.parent / 'bundles.yaml').read_text(encoding='utf-8'))['bundles']


def template_source(key):
    key = 'A-left' if key == 'A' else key
    match = next(((bundle, variant) for bundle in bundle_registry() for variant in bundle['templates'] if variant['id'] == key), None)
    if match is None:
        raise KeyError(f'Unknown template {key!r}')
    bundle, variant = match
    folder = ROOT / variant['folder']
    info = yaml.safe_load((folder / 'meta.yaml').read_text(encoding='utf-8'))
    kind = info.get('class', 'horizontal_card')
    width, height = (740, 1285) if kind == 'vertical_card' else (1350, 806)
    template = (folder / 'index.html.j2').read_text(encoding='utf-8')
    css = (ROOT / 'base.css').read_text(encoding='utf-8') + '\n' + (folder / 'style.css.j2').read_text(encoding='utf-8')
    if variant.get('layout'):
        css += '\n' + (folder / (variant['layout'] + '.css')).read_text(encoding='utf-8')
    required = sorted({ASSET_SOURCES[field][0] for field in re.findall(r'assets\.([a-z_][a-z_0-9]*)', template) if field in ASSET_SOURCES})
    return {'id': key, 'template_id': key, 'bundle_id': bundle['id'], 'bundle_name': bundle['name'],
            'name': variant['name'], 'description': info.get('description', '').strip(), 'kind': kind,
            'layout': variant.get('layout'), 'variation_version': variant.get('variation_version', 1),
            'variation_schema': SCHEMA_V1 if variant.get('layout') else {},
            'width': width, 'height': height, 'required_datasets': required,
            'template': template, 'css': css}


def template_catalog():
    return [{key: value for key, value in template_source(variant['id']).items() if key not in ('template', 'css')}
            for bundle in bundle_registry() for variant in bundle['templates']]


def template_bundles():
    catalog = template_catalog()
    return [{**{key: value for key, value in bundle.items() if key != 'templates'},
             'templates': [item for item in catalog if item['bundle_id'] == bundle['id']]}
            for bundle in bundle_registry()]
