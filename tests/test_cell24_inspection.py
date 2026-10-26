"""Synthetic metadata substitutes only; no PySpark or producer execution."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace as NS
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
import rcaip_cell24_inspection as i


class Frame:
    def __init__(self, columns, session, kind='string'):
        self.schema = NS(fields=[NS(name=c, dataType=NS(typeName=lambda: kind)) for c in columns])
        self.sparkSession = session

    def __getattr__(self, name):
        raise AssertionError('Forbidden access: ' + name)


def fixture(zone='UTC', kind='string'):
    session = NS(conf=NS(get=lambda key: zone))
    ns = {i.READY: True, i.STATUS: 'CLOSED_FOR_CURRENT_RUN', i.POST: 'example_post_a', i.RUN: 'synthetic-run'}
    for name, columns in zip(i.PRODUCTS.values(), (i.CASE_COLUMNS, i.MEMBER_COLUMNS, i.CLOSURE_COLUMNS)):
        ns[name] = Frame(columns, session, kind)
    return ns


def inspect(ns):
    return i.inspect_cell24(ns, expected_post='example_post_a', expected_run='synthetic-run')


class InspectionTest(unittest.TestCase):
    def test_matching_metadata_never_certifies_or_authorizes(self):
        ns = fixture(); original = dict(ns); result = inspect(ns)
        self.assertEqual(ns, original)
        self.assertEqual(result['common_session'], 'SUPPORTED')
        self.assertEqual(result['certification_retention'], 'UNVERIFIED')
        self.assertFalse(result['collection_authorized'])
        self.assertTrue(result['supported_prerequisites'])

    def test_each_absence_is_session_local(self):
        for name in fixture():
            ns = fixture(); del ns[name]; result = inspect(ns)
            entry = result['products'].get(name, result['authority'].get(name))
            self.assertEqual(entry['state'] if isinstance(entry, dict) else entry, 'ABSENT_FROM_THIS_SESSION')

    def test_authority_and_schema_mismatch(self):
        ns = fixture(); ns[i.READY] = 'True'; ns[i.RUN] = 'PRIVATE-RUN'
        frame = ns[i.PRODUCTS['members']]; frame.schema.fields.pop(0)
        result = inspect(ns)
        self.assertEqual(result['authority'][i.READY], 'MISMATCH')
        self.assertEqual(result['authority'][i.RUN], 'MISMATCH')
        self.assertEqual(result['products'][i.PRODUCTS['members']]['schema'], 'MISMATCH')
        self.assertNotIn('PRIVATE-RUN', json.dumps(result))

    def test_utc_native_only_and_no_configuration_write(self):
        for zone in ('UTC', 'Etc/UTC', 'Z', '+00:00'):
            self.assertEqual(inspect(fixture(zone, 'timestamp'))['utc_requirement'], 'SUPPORTED')
        self.assertEqual(inspect(fixture('Europe/London', 'timestamp'))['utc_requirement'], 'UNSUPPORTED')
        for kind in ('timestamp_ntz', 'date', 'string'):
            self.assertEqual(inspect(fixture('Europe/London', kind))['utc_requirement'], 'NOT_APPLICABLE')

    def test_mixed_session_and_output_safety(self):
        ns = fixture(); ns[i.PRODUCTS['closure']].sparkSession = object()
        self.assertEqual(inspect(ns)['common_session'], 'UNVERIFIED_OR_MISMATCH')
        ns = fixture(); ns['PRIVATE-IDENTIFIER'] = object()
        ns[i.PRODUCTS['cases']].schema.fields.append(NS(name='PRIVATE-COLUMN', dataType=NS(typeName=lambda: 'string')))
        self.assertNotIn('PRIVATE-', json.dumps(inspect(ns)))


if __name__ == '__main__':
    unittest.main(verbosity=2)
