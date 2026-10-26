"""Exercise the actual documented loader with temporary synthetic Python files.

No validator calls, Spark, Department inputs or production cache operations.
"""
import ast
import hashlib
import json
import os
from pathlib import Path
import py_compile
import re
import tempfile
import unittest
from unittest.mock import patch


def documented_loader():
    document = (Path(__file__).parent / 'INTUNE_DISTRIBUTED_VALIDATION_OPERATOR.md').read_text(encoding='utf-8')
    blocks = [b for b in re.findall(r'```python\s*\n(.*?)```', document, re.S)
              if 'def load_reviewed_distributed_operator(' in b]
    assert len(blocks) == 1
    scope = {}
    exec(compile(ast.parse(blocks[0]), '<documented-loader>', 'exec'), scope)
    return scope['load_reviewed_distributed_operator']


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.op, self.val, self.manifest = [root / n for n in ('operator.py', 'validator.py', 'manifest.json')]
        self.source = b"MARKER = 'verified'\n"
        self.changed = b"MARKER = 'modified'\n"
        self.validator = b"raise AssertionError('validator must not execute')\n"
        self.op.write_bytes(self.source)
        self.val.write_bytes(self.validator)
        self.manifest.write_text(json.dumps({
            'notebooks/RCAIP_Intune_Distributed_Validation.py': {'lf_sha256': hashlib.sha256(self.source).hexdigest()},
            'notebooks/rcaip_intune_validation.py': {'lf_sha256': hashlib.sha256(self.validator).hexdigest()},
        }), encoding='utf-8')
        self.load = documented_loader()

    def call(self):
        return self.load(self.op, self.val, self.manifest)

    def test_normal_load_fresh_modules_and_validator_text(self):
        first, source = self.call()
        second, _ = self.call()
        self.assertEqual(first.MARKER, 'verified')
        self.assertEqual(source, self.validator.decode('utf-8'))
        self.assertIsNot(first, second)

    def test_bad_operator_hash_stops_before_execution(self):
        self.op.write_bytes(b"raise AssertionError('must not execute')\n")
        with self.assertRaisesRegex(RuntimeError, '^REVIEWED_SOURCE_HASH_MISMATCH$'):
            self.call()

    def test_bad_validator_hash_stops_before_operator_execution(self):
        self.val.write_bytes(b'# unreviewed\n')
        with patch('builtins.compile', side_effect=AssertionError('operator compiled before verification')):
            with self.assertRaisesRegex(RuntimeError, '^REVIEWED_SOURCE_HASH_MISMATCH$'):
                self.call()

    def test_stale_bytecode_is_ignored(self):
        self.assertEqual(len(self.source), len(self.changed))
        self.op.write_bytes(self.changed)
        os.utime(self.op, (1700000000, 1700000000))
        cached = Path(py_compile.compile(str(self.op), doraise=True,
                      invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP))
        cache_before = cached.read_bytes()
        self.op.write_bytes(self.source)
        os.utime(self.op, (1700000000, 1700000000))
        loaded, _ = self.call()
        self.assertEqual(loaded.MARKER, 'verified')
        self.assertEqual(cached.read_bytes(), cache_before)

    def test_path_change_after_read_executes_verified_bytes(self):
        original = Path.read_bytes
        reads = []

        def read_then_change(path):
            data = original(path)
            reads.append(path)
            if path == self.op:
                self.op.write_bytes(self.changed)
            return data

        with patch.object(Path, 'read_bytes', read_then_change):
            loaded, source = self.call()
        self.assertEqual(loaded.MARKER, 'verified')
        self.assertEqual(source, self.validator.decode('utf-8'))
        self.assertEqual(reads, [self.op, self.val])
        self.assertEqual(self.op.read_bytes(), self.changed)


if __name__ == '__main__':
    unittest.main(verbosity=2)
