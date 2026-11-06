import unittest
from fastapi import HTTPException
from backend import app as service


class TemplateCatalogTest(unittest.TestCase):
    def test_catalog_contains_all_original_variants_and_native_dimensions(self):
        items = service.list_templates()
        from backend.template_variations import TEMPLATE_IDS
        self.assertEqual([item['id'] for item in items], list(TEMPLATE_IDS))
        for item in items:
            self.assertGreater(item['height'], item['width']) if item['kind']=='vertical_card' else self.assertGreater(item['width'], item['height'])
            self.assertEqual(sorted((item['width'],item['height'])),[850,1350])
            self.assertEqual(item['variation_version'], 3 if item['id'] in ('A-left', 'A-right') else 2)
            source = service.open_template(item['id'])
            self.assertNotIn('/ds/', source['template'])
            self.assertIn('assets.', source['template'])
            self.assertIn('.card', source['css'])
            self.assertTrue(source['required_datasets'])

    def test_each_port_renders_without_datasets_with_explicit_missing_provenance(self):
        with service.source_lock:
            original = service.catalog
            service.catalog = {'records': {}, 'photos': {}, 'fields': {}, 'types': {}, 'sources': []}
            try:
                for letter in 'ABCDEFG':
                    with self.subTest(template=letter):
                        source = service.open_template(letter)
                        design = service.Design(**source, seed=42, color='#a4d5c8', background='plain')
                        first = service.preview(service.Preview(design=design))
                        second = service.preview(service.Preview(design=design))
                        self.assertEqual(first, second)
                        self.assertIn('data:image/svg+xml', first['template'])
                        self.assertTrue(first['context']['missing'])
                        self.assertEqual(first['person'], {})
                        self.assertNotIn('{{', first['css'])
            finally:
                service.catalog = original

    def test_unknown_template_is_rejected(self):
        for key in ('../A', 'H', '/etc/passwd'):
            with self.assertRaises(HTTPException) as exc:
                service.open_template(key)
            self.assertEqual(exc.exception.status_code, 404)

    def test_dimensions_and_template_identity_survive_saved_design(self):
        from pathlib import Path
        import tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp, patch.object(service, 'DB', Path(tmp) / 'design.db'), patch.object(service, "records_root", lambda: Path(tmp) / "records"):
            design = service.Design(**service.open_template('C'), seed=42, color='#a4d5c8', background='plain')
            key = service.create_bundle(service.Bundle(name="Test bundle", layouts=[design]))['id']
            saved = service.Design.model_validate(service.read_bundle(key)["layouts"][0])
            self.assertEqual((saved.width, saved.height, saved.template_id), (850, 1350, 'C-top'))
