import json
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import HTTPException
from backend import app as service
from backend.template_variations import resolve_parameters, SCHEMA_V1, balanced_variants


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

    def test_parameters_are_bounded_deterministic_and_separate_from_global_rng(self):
        state = random.getstate()
        orders = set()
        for key in ['A-left','A-right']:
            for seed in range(128):
                parameters = resolve_parameters(key, seed)
                self.assertEqual(parameters, resolve_parameters(key, seed))
                for name, (low, high) in SCHEMA_V1.items():
                    self.assertLessEqual(low, parameters[name])
                    self.assertLessEqual(parameters[name], high)
                orders.add(parameters['dates_reversed'])
        self.assertEqual(orders,{0,1})
        self.assertEqual(state,random.getstate())
        with self.assertRaises(ValueError):
            resolve_parameters('A-left',42,2)

    def test_bundle_generation_is_balanced_and_reproducible(self):
        sequence = balanced_variants(['A-left','A-right'],7,42)
        self.assertEqual(sequence, balanced_variants(['A-left','A-right'],7,42))
        self.assertLessEqual(abs(sequence.count('A-left')-sequence.count('A-right')),1)
        self.assertEqual(sequence[:4],['A-left','A-right','A-left','A-right'])

    def test_saved_design_and_preview_record_resolved_parameters(self):
        source = service.open_template('A-right')
        design = service.Design(**source,seed=123,color='#ffffff',background='plain')
        with tempfile.TemporaryDirectory() as directory, patch.object(service,'DB',Path(directory)/'design.db'):
            with service.database() as db:
                db.execute('CREATE TABLE design(id INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
            key=service.create(design)['id']
            saved=service.read(key)
            self.assertEqual(saved.bundle_id,'horizontal-identity')
            self.assertEqual(saved.template_parameters,resolve_parameters('A-right',123))
            payload=json.loads(saved.model_dump_json())
            payload['seed']=124
            payload['template_parameters']['margin']=999
            saved=service.Design(**payload)
            service.update(key,saved)
            saved=service.read(key)
            self.assertEqual(saved.template_parameters,resolve_parameters('A-right',124))
            preview=service.preview(service.Preview(design=saved))
            self.assertEqual(preview['context']['template_selection']['parameters'],saved.template_parameters)
            self.assertNotIn('{{',preview['css'])
