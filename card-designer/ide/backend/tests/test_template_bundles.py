import json
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import HTTPException
from backend import app as service
from backend.template_variations import resolve_parameters, SCHEMA_V1, balanced_variants, TEMPLATE_IDS, variation_schema


class TemplateBundleTest(unittest.TestCase):
    def test_bundle_contains_explicit_layouts_and_legacy_alias_is_stable(self):
        bundle = service.open_bundle('horizontal-identity')
        self.assertEqual([item['id'] for item in bundle['templates']], ['A-left','A-right'])
        self.assertEqual(service.open_template('A')['template_id'], 'A-left')
        for key, column in [('A-left',1),('A-right',2)]:
            source = service.open_template(key)
            self.assertIn(f'--portrait-column: {column}', source['css'])
            self.assertNotIn('| random', source['css'])
        with self.assertRaises(HTTPException):
            service.open_bundle('../invalid')

    def test_every_gallery_template_has_fixed_layout_and_no_synthetic_fields(self):
        self.assertEqual(len(service.list_template_bundles()),8)
        self.assertEqual(len(service.list_templates()),16)
        for item in service.list_templates():
            source=service.open_template(item['id'])
            self.assertTrue(source['layout'])
            self.assertNotIn('gen.',source['template']+source['css'])
            self.assertNotIn('| random',source['template']+source['css'])
            self.assertIn('identity.',source['template'])
            self.assertIn('lambdaWalker/ds.photo_id',source['required_datasets'])
            design=service.Design(**source,seed=42,color='#ffffff',background='plain')
            preview=service.preview(service.Preview(design=design))
            self.assertNotIn('generator',preview['context'])
        for alias, key in [('B','B-left'),('C','C-top'),('D','D-left'),('E','E-codes-top'),('F','F-rail-left'),('G','G-photo-left')]:
            self.assertEqual(service.open_template(alias)['template_id'],key)

    def test_parameters_are_bounded_deterministic_and_separate_from_global_rng(self):
        state = random.getstate()
        orders = set()
        for key in TEMPLATE_IDS:
            for seed in range(128):
                parameters = resolve_parameters(key, seed)
                self.assertEqual(parameters, resolve_parameters(key, seed))
                for name, (low, high) in variation_schema(key).items():
                    self.assertLessEqual(low, parameters[name])
                    self.assertLessEqual(parameters[name], high)
                orders.add(parameters['dates_reversed'])
        self.assertEqual(orders,{0,1})
        self.assertEqual(state,random.getstate())
        with self.assertRaises(ValueError):
            resolve_parameters('A-left',42,99)

    def test_bundle_generation_is_balanced_and_reproducible(self):
        sequence = balanced_variants(['A-left','A-right'],7,42)
        self.assertEqual(sequence, balanced_variants(['A-left','A-right'],7,42))
        self.assertLessEqual(abs(sequence.count('A-left')-sequence.count('A-right')),1)
        self.assertEqual(sequence[:4],['A-left','A-right','A-left','A-right'])

    def test_saved_design_and_preview_record_resolved_parameters(self):
        source = service.open_template('A-right')
        design = service.Design(**source,seed=123,color='#ffffff',background='plain')
        with tempfile.TemporaryDirectory() as directory, patch.object(service,'DB',Path(directory)/'design.db'), patch.object(service, "records_root", lambda: Path(directory) / "records"):
            key=service.create_bundle(service.Bundle(name="Test bundle", layouts=[design]))['id']
            saved=service.Design.model_validate(service.read_bundle(key)["layouts"][0])
            self.assertEqual(saved.bundle_id,'horizontal-identity')
            self.assertEqual(saved.template_parameters,resolve_parameters('A-right',123, source['variation_version']))
            payload=json.loads(saved.model_dump_json())
            payload['seed']=124
            payload['template_parameters']['margin']=999
            saved=service.Design(**payload)
            service.update_bundle(key, service.Bundle(name="Test bundle", layouts=[saved]))
            saved=service.Design.model_validate(service.read_bundle(key)["layouts"][0])
            self.assertEqual(saved.template_parameters,resolve_parameters('A-right',124, source['variation_version']))
            preview=service.preview(service.Preview(design=saved))
            self.assertEqual(preview['context']['template_selection']['parameters'],saved.template_parameters)
            self.assertNotIn('{{',preview['css'])

    def test_portrait_backplates_are_seeded_and_cover_all_treatments(self):
        for key in TEMPLATE_IDS:
            treatments={resolve_parameters(key,seed).get('photo_background') for seed in range(64)}
            self.assertEqual(treatments, {'transparent','rgba(255,255,255,0.55)','#ffffff'})

    def test_original_variations_remain_available_for_saved_designs(self):
        parameters=resolve_parameters('A-left',42,1)
        self.assertEqual(parameters['version'],1)
        self.assertNotIn('photo_background',parameters)
        for key,(low,high) in variation_schema('A-left',1).items():
            self.assertLessEqual(low,parameters[key])
            self.assertLessEqual(parameters[key],high)

    def test_font_replays_and_is_embedded_only_in_current_layouts(self):
        for key in TEMPLATE_IDS:
            with self.subTest(template=key):
                source = service.open_template(key)
                if source['variation_version'] == 2:
                    design = service.Design(**source, seed=42, color='#ffffff', background='plain')
                    self.assertNotIn('@font-face', service.preview(service.Preview(design=design))['css'])
                    continue
                designs = [service.Design(**source, seed=seed, color='#ffffff', background='plain')
                           for seed in range(8)]
                previews = [service.preview(service.Preview(design=design)) for design in designs]
                families = set()
                for design, preview in zip(designs, previews):
                    parameters = preview['context']['template_selection']['parameters']
                    family = parameters['font_family']
                    families.add(family)
                    self.assertIn(f'font-family: "{family}"', preview['css'])
                    self.assertIn('data:font/ttf;base64,', preview['css'])
                    self.assertEqual(preview, service.preview(service.Preview(design=design)))
                    original = resolve_parameters(key, design.seed, 2)
                    self.assertEqual({k: v for k, v in parameters.items() if k not in ('version', 'font_family')},
                                     {k: v for k, v in original.items() if k != 'version'})
                self.assertGreater(len(families), 1)
                legacy = designs[0].model_copy(update={'variation_version': 2})
                self.assertNotIn('@font-face', service.preview(service.Preview(design=legacy))['css'])
