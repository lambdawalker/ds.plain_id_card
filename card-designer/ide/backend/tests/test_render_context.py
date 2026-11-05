import json
from pathlib import Path
import random
import tempfile
import unittest

from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from backend.render_context import build_context, identity_fields


class RenderContextTests(unittest.TestCase):
    def snapshot(self):
        records = {}
        for dataset, stems in [('lambdaWalker/ds.photo_id', ['photo-a', 'photo-b']),
                               ('lambdaWalker/ds.machine_readable_code', ['code-a', 'code-b', 'photo-b'])]:
            for stem in stems:
                key = dataset + ':' + stem
                field = 'image' if 'photo_id' in dataset else 'qrcode'
                records[key] = dict(id=key, record_id=stem, dataset=dataset,
                                    source_id=1, images={field: '/api/photos/' + stem})
        return dict(records=records, fields={}, types={}, sources=[{'id': 1, 'reference': '/real', 'count': 2}])

    def context(self, snapshot=None, seed=7, selected='photo-b'):
        snapshot = snapshot or self.snapshot()
        person = next((row for row in snapshot['records'].values() if row['record_id'] == selected and 'photo_id' in row['dataset']), {})
        return build_context(snapshot, {'seed': seed, 'color': '#123456'}, person, random.Random(seed))

    def test_identity_uses_structured_card_metadata_and_does_not_invent_values(self):
        person = {'name':'person_001', 'card':{'person':{'first_name':'Elena', 'last_name':'Solano',
                  'date_of_birth':'1994-03-18', 'gender':'F', 'document_number':'SPEC-100',
                  'address':{'number':42,'street':'Cedar Lane','city':'El Paso','state':'TX','postal_code':'79901'}}}}
        identity = identity_fields(person)
        self.assertEqual(identity['given_names'], 'Elena')
        self.assertEqual(identity['family_name'], 'Solano')
        self.assertEqual(identity['birth_date'], '1994-03-18')
        self.assertEqual(identity['address_line1'], '42 Cedar Lane')
        self.assertEqual(identity['address_line2'], 'El Paso, TX, 79901')
        self.assertEqual(identity['expiry_date'], '')
        self.assertEqual(identity_fields({'name':'person_001'})['full_name'], '')
        self.assertEqual(identity_fields({'card':{'display_name':'Elena Solano','surname':'Solano'}})['family_name'], '')

    def test_horizontal_a_renders_identity_metadata_with_readable_labels(self):
        from backend.template_catalog import template_source
        context, _ = self.context()
        context['identity'] = identity_fields({'card':{'full_name':'Elena <Solano>', 'birth_date':'1994-03-18'}})
        env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
        html = env.from_string(template_source('A')['template']).render(**context)
        self.assertIn('Elena &lt;Solano&gt;', html)
        self.assertIn('1994-03-18', html)
        self.assertIn('DATE OF BIRTH', html)
        self.assertNotIn('gen.', template_source('A')['template'])

    def test_selected_identity_and_cross_source_exact_stem(self):
        context, summary = self.context()
        self.assertEqual(context['data']['id'], 'photo-b')
        self.assertEqual(context['data']['index'], 1)
        self.assertEqual(context['person']['id'], 'lambdaWalker/ds.photo_id:photo-b')
        self.assertEqual(context['ds'].image('lambdaWalker/ds.machineReadableCode', 'photo-b', 'qrcode'), '/api/photos/photo-b')
        self.assertEqual(context['ds'].image('lambdawalker/ds.machinereadablecode', 'photo-b', 'qrcode'), '/api/photos/photo-b')
        self.assertEqual(context['env'].theme.primary_color.to_rgb_hex(), '#123456')
        self.assertEqual(summary['env']['theme']['primary_color'], '#123456')
        json.dumps(summary)

    def test_cross_source_fallback_is_same_ordinal(self):
        context, _ = self.context(selected='photo-a')
        self.assertEqual(context['ds'].image('lambdaWalker/ds.machineReadableCode', 'photo-a', 'qrcode'), '/api/photos/code-a')
        context, _ = self.context(selected='absent', seed=4)
        self.assertEqual(context['data']['index'], 4)
        self.assertEqual(context['ds'].image('lambdaWalker/ds.machineReadableCode', '4', 'qrcode'), '/api/photos/code-b')

    def test_lookup_facades_and_real_metadata_summary(self):
        snapshot = self.snapshot()
        key = 'lambdaWalker/ds.photo_id:photo-b'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'card.json'
            path.write_text(json.dumps({'city': 'Actual dataset city'}))
            snapshot['fields'][key] = {'card': path}
            snapshot['types'][key] = {'card': 'json'}
            context, summary = self.context(snapshot)
            ds = context['ds']
            self.assertEqual(ds['lambdaWalker/ds.photo_id'][1]['card']['city'], 'Actual dataset city')
            self.assertEqual(ds['lambdaWalker/ds.photo_id']['photo-a']['id'], 'photo-a')
            self.assertEqual(ds['lambdaWalker/ds.photo_id/body/photo-b/image'], '/api/photos/photo-b')
            self.assertIn(ds['lambdaWalker/ds.photo_id'].random()['id'], ['photo-a', 'photo-b'])
            self.assertEqual(summary['datasets']['lambdaWalker/ds.photo_id']['records']['photo-b']['card']['city'], 'Actual dataset city')
            json.dumps(summary)

    def test_missing_fields_are_labelled_neutral_placeholders(self):
        context, summary = self.context()
        url = context['ds'].image('<missing>', 'photo-b', '<barcode>')
        self.assertTrue(url.startswith('data:image/svg+xml,'))
        from urllib.parse import unquote
        svg = unquote(url.split(',', 1)[1])
        self.assertIn('&lt;missing&gt;', svg)
        self.assertNotIn('<missing>', svg)
        self.assertIn('Missing', svg)
        self.assertGreaterEqual(len(summary['missing']), 1)
        self.assertTrue(context['ds'].image('lambdaWalker/ds.photo_id', 'photo-b', 'barcode').startswith('data:'))

    def test_aliases_exact_first_ambiguous_rejected(self):
        snapshot = self.snapshot()
        canonical = 'lambdaWalker/ds.machine_readable_code'
        for dataset in ['LAMBDaWalker/ds.machine_readable_code']:
            snapshot['records'][dataset] = dict(id=dataset, dataset=dataset, record_id='photo-b', images={'qrcode': '/other'})
        context, _ = self.context(snapshot)
        self.assertEqual(context['ds'].image(canonical, 'photo-b', 'qrcode'), '/api/photos/photo-b')
        with self.assertRaisesRegex(ValueError, 'Ambiguous'):
            context['ds'].image('lambdaWalker/ds.machineReadableCode', 'photo-b', 'qrcode')

    def test_seeded_generators_and_global_rng_unchanged(self):
        state = random.getstate()
        template = ('{{ gen.text.word(4) }} {{ gen.text.sentence((3,7),p=False) }} '
                    '{{ gen.text.letter() }} {{ gen.random.boolean() }} {{ gen.random.number(1,9) }} '
                    '{{ gen.random.lf_number(1,99) }} {{ gen.random.hex(18) }} {{ gen.random.mrz() }} '
                    '{{ gen.time.date().data }} {{ gen.time.year_as_number().data }} '
                    '{{ gen.address.number().data }} {{ gen.address.block().data }} '
                    '{{ gen.address.city().data }} {{ gen.address.state().data }} '
                    '{{ env.theme.primary_color.to_rgb_hex() }}')
        env = SandboxedEnvironment(undefined=StrictUndefined)
        def render(seed):
            return env.from_string(template).render(**self.context(seed=seed)[0])
        self.assertEqual(render(45), render(45))
        self.assertNotEqual(render(45), render(46))
        self.assertEqual(state, random.getstate())

    def test_explicit_text_color_is_available_to_templates_and_context(self):
        context, summary = build_context(self.snapshot(), {'seed':7, 'color':'#abcdef', 'text_color':'#123456'}, {}, random.Random(7))
        self.assertEqual(context['env'].theme.text_color.to_rgb_hex(), '#123456')
        self.assertEqual(summary['env']['theme']['text_color'], '#123456')

    def test_original_seven_templates_render_with_sandbox(self):
        root = Path(__file__).resolve().parents[4] / 'templates' / 'id_cards'
        env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
        for letter in 'ABCDEFG':
            with self.subTest(template=letter):
                context, summary = self.context()
                env.filters['random'] = random.Random(3).choice
                for field in ['index.html.j2', 'style.css.j2']:
                    result = env.from_string((root / letter / field).read_text()).render(**context)
                    self.assertTrue(result.strip())
                json.dumps(summary)


if __name__ == '__main__':
    unittest.main()
