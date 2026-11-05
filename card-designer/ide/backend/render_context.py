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
            rows.sort(key=record_order)
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

    def choose(self, dataset, record=None):
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
        return chosen

    def record(self, dataset, record=None):
        chosen = self.choose(dataset, record)
        return self.load(chosen) if chosen else {}

    def _missing(self, dataset, field, reason):
        entry = {'dataset': dataset, 'field': field, 'reason': reason}
        if entry not in self.summary['missing']:
            self.summary['missing'].append(entry)

    def image(self, dataset, record, field='image'):
        item = self.choose(dataset, record)
        if item:
            resolved = item['dataset']
            key = str(item.get('record_id', item['id']))
            self.summary['datasets'].setdefault(resolved, {'records': {}, 'count': len(self.rows(resolved))})['records'].setdefault(key,
                {'id': key, 'index': self.groups[resolved].index(item)})
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


def identity_fields(person):
    """Normalize structured card metadata, preserving missing values as empty strings."""
    def mapping(value):
        return value if isinstance(value, dict) else {}
    def scalar(value):
        return str(value).strip() if isinstance(value, (str, int, float)) and not isinstance(value, bool) else ''
    card = mapping(person.get('card'))
    profile = mapping(card.get('person', card.get('identity', card)))
    nested = mapping(person.get('person', person.get('identity')))
    sources = [profile, card, nested, person]
    def value(*keys):
        return next((scalar(source.get(key)) for source in sources for key in keys if scalar(source.get(key))), '')
    name = next((mapping(source.get('name')) for source in sources if isinstance(source.get('name'), dict)), {})
    given = scalar(name.get('first')) or value('given_names', 'given_name', 'first_name')
    middle = scalar(name.get('middle')) or value('middle_name', 'middle_names')
    family = scalar(name.get('last')) or value('family_name', 'last_name', 'surname', 'surnames')
    suffix = scalar(name.get('suffix')) or value('suffix')
    full = value('full_name', 'display_name') or next((scalar(source.get('name')) for source in sources[:-1] if scalar(source.get('name'))), '')
    birth = next((mapping(source.get('birth')) for source in sources if isinstance(source.get('birth'), dict)), {})
    demographics = next((mapping(source.get('demographics')) for source in sources if isinstance(source.get('demographics'), dict)), {})
    document = next((mapping(source.get('document')) for source in sources if isinstance(source.get('document'), dict)), {})
    contact = mapping(card.get('contact')) or mapping(person.get('contact'))
    address = contact.get('address') or next((source.get('address') for source in sources if source.get('address')), {})
    if isinstance(address, str):
        line1, line2 = address, ''
    else:
        address = mapping(address)
        def part(*keys):
            return next((scalar(address.get(key)) for key in keys if scalar(address.get(key))), '')
        line1 = part('line1', 'address_line1', 'street_address') or ' '.join(filter(None, [part('number', 'house_number'), part('street', 'street_name')]))
        locality = ', '.join(filter(None, [part('city', 'locality'), part('state', 'region'), part('postal_code', 'zip_code'), part('country')]))
        line2 = ', '.join(filter(None, [part('line2', 'address_line2'), locality]))
    return {'given_names': ' '.join(filter(None, [given, middle])) or full or family,
            'family_name': ' '.join(filter(None, [family, suffix])) if given else '',
            'full_name': full or ' '.join(filter(None, [given, middle, family, suffix])),
            'birth_date': scalar(birth.get('date')) or value('date_of_birth', 'birth_date', 'dob'),
            'sex': scalar(demographics.get('gender')) or scalar(demographics.get('sex')) or value('sex', 'gender'),
            'address_line1': line1 or value('address_line1', 'street_address'),
            'address_line2': line2 or value('address_line2'),
            'document_number': scalar(document.get('number')) or value('document_number', 'document_no', 'id_number'),
            'document_type': scalar(document.get('type')) or value('document_type'),
            'issuing_authority': scalar(document.get('issuing_authority')) or value('issuing_authority'),
            'personal_number': value('personal_number', 'personal_no', 'curp'),
            'nationality': scalar(demographics.get('nationality')) or value('nationality', 'country'),
            'issue_date': scalar(document.get('issued')) or value('issue_date', 'date_of_issue'),
            'expiry_date': scalar(document.get('expires')) or value('expiry_date', 'expiration_date', 'date_of_expiry')}


def record_order(record):
    """Natural record order: numeric IDs sort 2, 10, 42 rather than 10, 2, 42."""
    return tuple((0, int(part)) if part.isdigit() else (1, part.casefold())
                 for part in re.split(r'(\d+)', str(record.get('record_id', record['id']))))


def build_context(snapshot, design, person, rng, *, publish=None, include_identity=True):
    """Return render-only objects and a live JSON-ready provenance summary.

    Render both HTML and CSS with the same returned context. The summary's
    datasets/missing collections update as templates access lazy dataset fields.
    """
    get = (lambda key, default=None: design.get(key, default)) if isinstance(design, dict) else (lambda key, default=None: getattr(design, key, default))
    seed = get('seed', 0)
    person = dict(person or {})
    selected = snapshot.get('records', {}).get(person.get('id'), person)
    rows = sorted((r for r in snapshot.get('records', {}).values() if r.get('dataset') == selected.get('dataset')),
                  key=record_order)
    ordinal = next((i for i, row in enumerate(rows) if row['id'] == person.get('id')), seed)
    data = {**person, 'id': person.get('record_id', person.get('name', str(seed))), 'index': ordinal}
    color = get('color', '#000000')
    text_color = get('text_color', '#000000')
    identity = identity_fields(person) if include_identity else {}
    summary = {'data': data, 'identity': identity, 'common': get('common', {}) or {},
               'env': {'theme': {'primary_color': color, 'text_color': text_color}},
               'datasets': {}, 'sources': [dict(source) for source in snapshot.get('sources', [])],
               'missing': [], 'generator': {'kind': 'synthetic', 'seed': seed}}
    ds = DatasetHandler(snapshot, person, ordinal, rng, summary)
    # Publish one aligned real record per source, including metadata and image URLs.
    for dataset in ds.groups:
        if publish is not None and not any(normalize_dataset_id(dataset) == normalize_dataset_id(required) for required in publish):
            continue
        ds.record(dataset, person.get('record_id', str(seed)))
    # Use one complete identity source. Never combine fields from unrelated people.
    primary = person
    origin = 'selected' if person else 'none'
    if include_identity and not any(identity.values()):
        photo_dataset = ds.resolve('lambdaWalker/ds.photo_id')
        if ds.rows(photo_dataset):
            primary = ds.record(photo_dataset, person.get('record_id', str(seed)))
            identity = identity_fields(primary)
            origin = 'aligned_photo'
    card = primary.get('card', {})
    card = card if isinstance(card, dict) else {}
    identity_source = {'kind': origin, 'dataset': primary.get('dataset'),
                       'record_id': primary.get('record_id'), 'index': primary.get('index', ordinal) if primary else None}
    summary.update(identity=identity, card=card, identity_source=identity_source,
                   selection={'sample_id': person.get('id'), 'record_id': person.get('record_id'),
                              'dataset': person.get('dataset'), 'mode': 'selected' if person else 'seed_fallback'})
    text = TextGenerator(rng)
    gen = SimpleNamespace(text=text, random=RandomGenerator(rng), time=TimeGenerator(rng),
                          address=AddressGenerator(rng, text, person))
    context = {'person': person, 'identity': identity, 'card': card, 'identity_source': identity_source, 'data': data, 'seed': seed, 'common': summary['common'],
               'env': SimpleNamespace(theme=SimpleNamespace(primary_color=Color(color), text_color=Color(text_color))),
               'gen': gen, 'ds': ds}
    return context, summary
