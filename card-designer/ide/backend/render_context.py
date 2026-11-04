"""Request-local compatibility context for the original drawing-tool templates.

Generators deliberately produce synthetic filler; dataset image fields always refer
only to connected records (or an explicitly labelled missing-data placeholder).
"""
from datetime import date, timedelta
from html import escape
import re
from types import SimpleNamespace
from urllib.parse import quote

from .photo_dataset import metadata


class Color(str):
    def to_rgb_hex(self):
        return str(self)


def _count(rng, value):
    if isinstance(value, (tuple, list)):
        return rng.randint(int(value[0]), int(value[1]))
    return int(value)


class TextGenerator:
    def __init__(self, rng):
        self.rng = rng

    def word(self, syllables=(1, 3)):
        # Compose pronounceable filler without embedding a fixed example identity.
        return ''.join(self.rng.choice(('ba', 'de', 'fi', 'go', 'ka', 'le', 'mi', 'no', 'ra', 'su', 'ti', 'vo'))
                       for _ in range(max(1, min(100, _count(self.rng, syllables))))).capitalize()

    def sentence(self, words=(3, 8), p=True):
        value = ' '.join(self.word().lower() for _ in range(max(1, min(200, _count(self.rng, words)))))
        return value.capitalize() + ('.' if p else '')

    def letter(self):
        return self.rng.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')


class RandomGenerator:
    def __init__(self, rng):
        self.rng = rng

    def boolean(self):
        return bool(self.rng.getrandbits(1))

    def number(self, minimum=0, maximum=100):
        return self.rng.randint(int(minimum), int(maximum))

    def lf_number(self, minimum=0, maximum=100):
        return str(self.number(minimum, maximum))

    def hex(self, length=16):
        return ''.join(self.rng.choice('0123456789abcdef') for _ in range(max(0, min(10000, int(length)))))

    def mrz(self):
        return '\n'.join(''.join(self.rng.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<') for _ in range(44))
                         for _ in range(2))


class TimeGenerator:
    def __init__(self, rng):
        self.rng = rng

    def date(self):
        value = date(1950, 1, 1) + timedelta(days=self.rng.randint(0, 365 * 100))
        return {'data': value.isoformat(), 'source': 'synthetic'}

    def year_as_number(self):
        return {'data': self.rng.randint(1950, 2050), 'source': 'synthetic'}


class AddressGenerator:
    def __init__(self, rng, text, person):
        self.rng, self.text, self.person = rng, text, person

    def _value(self, name):
        address = self.person.get('address', {})
        value = address.get(name) if isinstance(address, dict) else None
        if value is None:
            value = self.person.get(name)
        if value is not None:
            return {'data': value, 'source': self.person.get('dataset', 'selected record')}
        return {'data': self.rng.randint(1, 999) if name == 'number' else self.text.word((2, 4)),
                'source': 'synthetic'}

    def number(self):
        return self._value('number')

    def block(self):
        return self._value('block')

    def city(self):
        return self._value('city')

    def state(self):
        return self._value('state')


def normalize_dataset_id(value):
    """Normalize the historical camelCase code-dataset alias, after exact lookup."""
    return value.casefold().replace('ds.machinereadablecode', 'ds.machine_readable_code')


def missing_image(dataset, field):
    label = escape(f'Missing {dataset}: {field}')
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="480" height="180" viewBox="0 0 480 180">'
           f'<rect width="480" height="180" fill="#eeeeee"/>'
           f'<text x="240" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#555555">{label}</text></svg>')
    return 'data:image/svg+xml,' + quote(svg, safe='')


class DatasetView:
    def __init__(self, handler, dataset):
        self.handler, self.dataset = handler, dataset

    def __getitem__(self, record):
        return self.handler.record(self.dataset, record)

    def random(self):
        rows = self.handler.rows(self.dataset)
        if not rows:
            return {}
        return self.handler.load(self.handler.rng.choice(rows))

    def __len__(self):
        return len(self.handler.rows(self.dataset))


class DatasetHandler:
    def __init__(self, snapshot, person, ordinal, rng, summary):
        self.snapshot, self.person, self.ordinal, self.rng = snapshot, person, ordinal, rng
        self.summary = summary
        self.groups = {}
        for record in snapshot.get('records', {}).values():
            if record.get('dataset'):
                self.groups.setdefault(record['dataset'], []).append(record)
        for rows in self.groups.values():
            rows.sort(key=lambda row: str(row.get('record_id', row['id'])))
        self.cache = {}

    def resolve(self, dataset):
        if dataset in self.groups:
            return dataset
        candidates = [key for key in self.groups if normalize_dataset_id(key) == normalize_dataset_id(dataset)]
        if len(candidates) > 1:
            raise ValueError(f'Ambiguous dataset alias: {dataset}')
        return candidates[0] if candidates else dataset

    def rows(self, dataset):
        return self.groups.get(self.resolve(dataset), [])

    def load(self, record):
        key = record['id']
        if key not in self.cache:
            loaded = {**record, **metadata(self.snapshot.get('fields', {}).get(key, {}),
                                          self.snapshot.get('types', {}).get(key, {}))}
            if key == self.person.get('id'):
                loaded.update(self.person)
            loaded['images'] = dict(record.get('images', {}))
            loaded['id'] = record.get('record_id', record['id'])
            loaded['index'] = self.groups[record['dataset']].index(record)
            self.cache[key] = loaded
            self.summary['datasets'].setdefault(record['dataset'], {'records': {}, 'count': len(self.rows(record['dataset']))})['records'][str(loaded['id'])] = loaded
        return self.cache[key]

    def record(self, dataset, record=None):
        rows = self.rows(dataset)
        if not rows:
            self._missing(dataset, '', 'dataset unavailable')
            return {}
        if isinstance(record, int):
            chosen = rows[record % len(rows)]
        else:
            stem = str(record if record is not None else self.person.get('record_id', self.ordinal))
            chosen = next((row for row in rows if str(row.get('record_id', row['id'])) == stem), None)
            chosen = chosen or rows[self.ordinal % len(rows)]
        return self.load(chosen)

    def _missing(self, dataset, field, reason):
        entry = {'dataset': dataset, 'field': field, 'reason': reason}
        if entry not in self.summary['missing']:
            self.summary['missing'].append(entry)

    def image(self, dataset, record, field='image'):
        item = self.record(dataset, record)
        url = item.get('images', {}).get(field)
        if not url:
            self._missing(dataset, field, 'image field unavailable')
        return url or missing_image(dataset, field)

    def __getitem__(self, key):
        match = re.fullmatch(r'(.+)/body/([^/]+)/([^/]+)', key)
        if match:
            dataset, record, field = match.groups()
            item = self.record(dataset, record)
            if field in item.get('images', {}):
                return item['images'][field]
            if field in item:
                return item[field]
            return self.image(dataset, record, field)
        return DatasetView(self, key)


def build_context(snapshot, design, person, rng):
    """Return render-only objects and a live JSON-ready provenance summary.

    Render both HTML and CSS with the same returned context. The summary's
    datasets/missing collections update as templates access lazy dataset fields.
    """
    get = (lambda key, default=None: design.get(key, default)) if isinstance(design, dict) else (lambda key, default=None: getattr(design, key, default))
    seed = get('seed', 0)
    person = dict(person or {})
    selected = snapshot.get('records', {}).get(person.get('id'), person)
    rows = sorted((r for r in snapshot.get('records', {}).values() if r.get('dataset') == selected.get('dataset')),
                  key=lambda row: str(row.get('record_id', row['id'])))
    ordinal = next((i for i, row in enumerate(rows) if row['id'] == person.get('id')), seed)
    data = {**person, 'id': person.get('record_id', person.get('name', str(seed))), 'index': ordinal}
    color = get('color', '#000000')
    text_color = get('text_color', '#000000')
    summary = {'data': data, 'common': get('common', {}) or {},
               'env': {'theme': {'primary_color': color, 'text_color': text_color}},
               'datasets': {}, 'sources': [dict(source) for source in snapshot.get('sources', [])],
               'missing': [], 'generator': {'kind': 'synthetic', 'seed': seed}}
    ds = DatasetHandler(snapshot, person, ordinal, rng, summary)
    # Publish one aligned real record per source, including metadata and image URLs.
    for dataset in ds.groups:
        ds.record(dataset, person.get('record_id', str(seed)))
    text = TextGenerator(rng)
    gen = SimpleNamespace(text=text, random=RandomGenerator(rng), time=TimeGenerator(rng),
                          address=AddressGenerator(rng, text, person))
    context = {'person': person, 'data': data, 'seed': seed, 'common': summary['common'],
               'env': SimpleNamespace(theme=SimpleNamespace(primary_color=Color(color), text_color=Color(text_color))),
               'gen': gen, 'ds': ds}
    return context, summary
