import random
import unittest
from unittest.mock import patch
from backend import app as service
from backend.template_context import project_context, requirements
from backend.template_catalog import template_source


class TemplateProjectionTest(unittest.TestCase):
    def snapshot(self):
        photo = {'id':'photo-1','record_id':'person_001','name':'person_001','dataset':'lambdaWalker/ds.photo_id',
                 'source_id':1,'images':{'image':'/api/photos/photo-1'},'photo_url':'/api/photos/photo-1'}
        code = {'id':'code-1','record_id':'1','name':'1','dataset':'lambdaWalker/ds.machineReadableCode',
                'source_id':2,'images':{key:'/api/photos/'+key for key in ('barcode','slim_barcode','pdf417','qrcode')}}
        return {'records':{'photo-1':photo,'code-1':code},'fields':{},'types':{},'sources':[]}

    def test_image_only_templates_never_read_metadata_and_omit_catalog_payloads(self):
        snapshot = self.snapshot()
        for letter in 'BCDEFG':
            with self.subTest(letter=letter), patch('backend.render_context.metadata', side_effect=AssertionError('Unneeded metadata read')):
                design = service.Design(name=letter,template='<img src="{{ assets.photo }}">',css='',seed=42,color='#ffffff',background='plain')
                context, summary = project_context(snapshot, design, {}, random.Random(42))
                self.assertNotIn('ds', context)
                self.assertNotIn('datasets', summary)
                self.assertNotIn('card', summary)
                self.assertNotIn('identity', summary)
                self.assertEqual(set(summary['assets']), set(requirements(design.template, design.css)['assets']))
                self.assertEqual(summary['missing'], [])

    def test_projected_preview_does_not_publish_selected_raw_metadata(self):
        snapshot = self.snapshot()
        selected = {**snapshot['records']['photo-1'],'prompt':'PRIVATE LARGE PROMPT','description':'LONG DESCRIPTION'}
        design = service.Design(**template_source('A'), seed=42, color='#ffffff', background='plain')
        context, summary = project_context(snapshot, design, selected, random.Random(42))
        self.assertEqual(summary['data'], {'id':'person_001','index':0})
        self.assertNotIn('PRIVATE LARGE PROMPT', str(summary))
        self.assertEqual(summary['assets'], {'photo':'/api/photos/photo-1'})
        self.assertEqual(set(summary['bindings']), {'lambdaWalker/ds.photo_id'})

    def test_legacy_custom_templates_keep_dataset_facade(self):
        design = service.Design(name='Legacy', template="{{ ds.image('lambdaWalker/ds.photo_id', data.id, 'image') }}",
                                css='', seed=42, color='#ffffff', background='plain')
        context, summary = project_context(self.snapshot(), design, {}, random.Random(42))
        self.assertIn('ds', context)
        self.assertIn('datasets', summary)

    def test_explicit_legacy_fields_retain_compatibility_and_css_card_is_detected(self):
        for extra in ('{{ person.name }}', '{{ data.prompt }}', "{{ data['prompt'] }}"):
            self.assertTrue(requirements('{{ assets.photo }}'+extra)['compatibility'])
        design = service.Design(name='Mixed',template='{{ assets.photo }} {{ person.name }} {{ data.prompt }}',css='',seed=42,color='#ffffff',background='plain')
        selected = {**self.snapshot()['records']['photo-1'], 'prompt':'Explicit prompt'}
        context, _ = project_context(self.snapshot(), design, selected, random.Random(42))
        self.assertEqual(context['person']['name'], 'person_001')
        self.assertEqual(context['data']['prompt'], 'Explicit prompt')
        self.assertEqual(context['assets']['photo'], '/api/photos/photo-1')
        self.assertTrue(requirements('{{ assets.photo }}', '{{ card.physical.height }}')['card'])
        self.assertEqual(requirements("{{ assets['photo'] }}")['assets'], ['photo'])
        self.assertIsNone(requirements('{{ assets.photo }'))

    def test_unknown_asset_has_clear_error(self):
        with self.assertRaisesRegex(ValueError, 'Unknown template assets: unknown'):
            requirements('{{ assets.unknown }}')

    def test_address_generators_keep_explicit_selected_metadata(self):
        snapshot = self.snapshot()
        snapshot['fields']['photo-1'] = {'address':'fixture'}
        snapshot['types']['photo-1'] = {'address':'json'}
        design = service.Design(name='Address',template='<img src="{{ assets.photo }}">{{ gen.address.city().data }}',css='',sample='photo-1',seed=42,color='#ffffff',background='plain')
        with patch.object(service, 'catalog', snapshot), patch.object(service, 'metadata', return_value={'address':{'city':'Dataset City'}}):
            response = service.preview(service.Preview(design=design))
        self.assertIn('Dataset City', response['template'])
        self.assertNotIn('address', response['person'])

    def test_http_image_only_projection_does_not_read_selected_metadata(self):
        snapshot = self.snapshot()
        snapshot['fields']['photo-1'] = {'card':'unreadable'}
        snapshot['types']['photo-1'] = {'card':'json'}
        design = service.Design(name='Image',template='<img src="{{ assets.pdf417 }}">',css='',sample='photo-1',seed=42,color='#ffffff',background='plain')
        with patch.object(service, 'catalog', snapshot), patch.object(service, 'metadata', side_effect=AssertionError('Unused selected metadata read')):
            response = service.preview(service.Preview(design=design))
        self.assertEqual(response['context']['projection'], 'template')
        self.assertNotIn('datasets', response['context'])
        self.assertNotIn('card', response['person'])
