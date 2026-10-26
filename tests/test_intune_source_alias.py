"""Config AST-only synthetic resolution tests. No Spark, bootstrap or data reads."""
import ast
import hashlib
from pathlib import Path
import subprocess
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
CONFIG = 'notebooks/RCAIP_Config_000_Post_Run_Config.py'
TARGET = 'example_sources.intune_devices_1'


def selected(source, catalog):
    tree = ast.parse(source)
    nodes = [n for n in tree.body if
             isinstance(n, ast.FunctionDef) and n.name in
             ('table_exists_safe', 'resolve_first_available_table') or
             isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and
             t.id in ('RCAIP_SOURCE_ALIASES', 'RCAIP_INTUNE_TABLE') for t in n.targets)]
    ns = {'spark': SimpleNamespace(catalog=catalog), 'RCAIP_RESOLVED_SOURCE_TABLES': {},
          'RCAIP_POST_LOOKUP_CANDIDATES': ['synthetic.lookup']}
    exec(compile(ast.Module(nodes, type_ignores=[]), CONFIG, 'exec'), ns)
    return ns


class Catalog:
    def __init__(self, answer):
        self.answer, self.calls = answer, []

    def tableExists(self, name):
        self.calls.append(name)
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class AliasReview(unittest.TestCase):
    def resolve(self, answer):
        cat = Catalog(answer)
        ns = selected((ROOT / CONFIG).read_text(encoding='utf-8'), cat)
        result = ns['resolve_first_available_table'](ns['RCAIP_SOURCE_ALIASES']['intune_devices'])
        self.assertEqual(cat.calls, [TARGET])
        self.assertEqual(ns['RCAIP_INTUNE_TABLE'], TARGET)
        return ns, result

    def test_available_exact_target(self):
        self.assertEqual(self.resolve(True)[1], TARGET)

    def test_missing_no_alternate(self):
        self.assertIsNone(self.resolve(False)[1])

    def test_access_error_existing_resolver_returns_unavailable(self):
        self.assertIsNone(self.resolve(PermissionError('synthetic denied'))[1])

    def test_required_stops(self):
        ns, _ = self.resolve(False)
        with self.assertRaises(RuntimeError):
            ns['resolve_first_available_table']([TARGET], required=True)

    def test_only_alias_bytes_changed(self):
        before = subprocess.check_output([
            'git', 'show', '08ce04c6228f5a7c1f88489203dcd625737323dd:' + CONFIG
        ], cwd=ROOT)
        self.assertEqual(
            hashlib.sha256(before.replace(b'\r\n', b'\n')).hexdigest(),
            '2b72d706741f9b5c98d02a3d55f528be680ee3e7be3a77dbe12cdf10a8412d74'
        )
        after = (ROOT / CONFIG).read_bytes()
        old = b'example_sources.binding_2af97025.microsoft_example_sources.binding_2af97025_ms365intune_v2_silver'
        self.assertEqual(before.count(old), 1)
        self.assertEqual(after, before.replace(old, TARGET.encode()))

    def test_availability_does_not_read_schema_or_rows(self):
        # This substitute exposes only tableExists. Success proves resolution only;
        # missing required columns, empty data and unusable values are still unknown.
        for hypothetical_evidence in ('missing_schema', 'empty', 'unusable', 'usable'):
            with self.subTest(evidence=hypothetical_evidence):
                self.assertEqual(self.resolve(True)[1], TARGET)


if __name__ == '__main__':
    unittest.main(verbosity=2)
