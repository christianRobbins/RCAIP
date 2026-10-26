"""Synthetic pagination tests. Never import or execute the publication script.

Extract only the DBFS read statements and following size guards by AST.
No SDK, credentials, Spark, notebook evidence, or workbook generation is used.
"""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'notebooks' / 'rcaip_cell24_retained_publication.py'
MIB = 1024 * 1024
LIMIT = 64 * MIB


def read_statements():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    def assigns(node, name):
        return isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets)
    begin = next(i for i, n in enumerate(tree.body) if assigns(n, 'notebook_buffer'))
    end = next(i for i, n in enumerate(tree.body) if assigns(n, 'input_sha'))
    body = tree.body[begin:end]
    # Ensure the extracted test section cannot silently expand to other work.
    calls = {ast.unparse(n.func) for item in body for n in ast.walk(item)
             if isinstance(n, ast.Call)}
    assert calls <= {'bytearray', 'client.dbfs.download', 'min', 'len',
                     'stream.read', 'notebook_buffer.extend', 'RuntimeError', 'bytes'}, calls
    return compile(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])),
                   '<isolated production DBFS read>', 'exec')


class Stream:
    def __init__(self, data, partial=None, fail_after=None):
        self.data, self.partial, self.fail_after = data, partial, fail_after
        self.offset = 0
        self.requests = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def read(self, length):
        self.requests.append(length)
        if not 0 < length <= MIB:
            raise ValueError('Cannot read more than 1048576 bytes in one read')
        if self.fail_after is not None and self.offset >= self.fail_after:
            raise TimeoutError('Synthetic timeout')
        size = min(length, self.partial) if self.partial else length
        chunk = self.data[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class DBFS:
    def __init__(self, stream):
        self.stream = stream
        self.paths = []

    def download(self, path):
        self.paths.append(path)
        return self.stream


class PaginationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compiled = read_statements()

    def attempt(self, data, size=None, partial=None, fail_after=None):
        stream = Stream(data, partial, fail_after)
        dbfs = DBFS(stream)
        ns = dict(client=SimpleNamespace(dbfs=dbfs), source_path='/synthetic.ipynb',
                  max_bytes=LIMIT,
                  info=SimpleNamespace(file_size=len(data) if size is None else size))
        try:
            exec(self.compiled, ns)
        finally:
            self.assertTrue(stream.closed)
            self.assertTrue(all(0 < n <= MIB for n in stream.requests))
            self.assertEqual(dbfs.paths, ['/synthetic.ipynb'])
        return ns['notebook_bytes']

    def test_byte_exact_boundaries(self):
        for size in (1, MIB-1, MIB, MIB+1, 3*MIB+17, LIMIT):
            with self.subTest(size=size):
                data = (bytes(range(256)) * ((size+255)//256))[:size]
                self.assertEqual(self.attempt(data), data)

    def test_short_positive_reads_continue(self):
        data = b'abc\x00\xff' * (MIB//5 + 100)
        self.assertEqual(self.attempt(data, partial=65537), data)

    def test_early_eof_stops(self):
        with self.assertRaisesRegex(RuntimeError, 'size does not match'):
            self.attempt(b'a'*99, size=100)

    def test_growth_stops(self):
        with self.assertRaisesRegex(RuntimeError, 'size does not match'):
            self.attempt(b'a'*101, size=100)

    def test_overflow_stops(self):
        with self.assertRaisesRegex(RuntimeError, 'exceeds the byte bound'):
            self.attempt(b'a'*(LIMIT+1), size=LIMIT)

    def test_timeout_propagates(self):
        with self.assertRaises(TimeoutError):
            self.attempt(b'a'*(MIB+1), fail_after=MIB)

    def test_old_single_read_fails_control(self):
        with self.assertRaisesRegex(ValueError, '1048576'):
            Stream(b'abc').read(LIMIT+1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
