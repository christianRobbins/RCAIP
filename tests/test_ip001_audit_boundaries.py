"""Synthetic boundary controls; no Spark/service/data access."""
from dataclasses import replace
from datetime import datetime, timezone, timedelta
import hashlib
import unittest
import test_ip001_retention as t


class Boundaries(unittest.TestCase):
    def test_cross_product_equivalence_preserves_rows(self):
        a = t.observation(generated_at_utc='2026-10-14 10:00:00.1')
        b = t.observation(generated_at_utc='2026-10-14T10:00:00.100000Z')
        products = {'general': {'binding': t.binding(), 'rows': [a]},
                    'report_detail': {'binding': replace(t.binding(), generation=b['generated_at_utc']), 'rows': [b]}}
        self.assertEqual(t.run_a([t.artifact()], products).status()['general'], 'AVAILABLE_POSITIVE')
        self.assertEqual(a['generated_at_utc'], '2026-10-14 10:00:00.1')
        self.assertEqual(b['generated_at_utc'], '2026-10-14T10:00:00.100000Z')
        products['report_detail'] = {'binding': replace(t.binding(), generation='2026-10-14 10:00:01'), 'rows': []}
        with self.assertRaisesRegex(t.audit.AuditError, 'MIXED_PRODUCT_GENERATIONS'):
            t.run_a([t.artifact()], products)

    def test_scope_and_timezone_still_rejected(self):
        for value in [datetime(2026, 10, 14), datetime(2026, 10, 14, tzinfo=timezone(timedelta(hours=1))),
                      '2026-10-14 10:00:00+01:00', '2026-10-14 10:00:00.1234567', 'opaque']:
            with self.subTest(value=repr(value)), self.assertRaises(t.audit.AuditError):
                t.audit.generation_key(value)
        self.assertEqual(t.audit.generation_key(datetime(2026,10,14,tzinfo=timezone.utc)),
                         t.audit.generation_key('2026-10-14 00:00:00'))
        for field in ['post_slug', 'rcaip_run_id']:
            row=t.observation(generated_at_utc=t.binding().generation, **{field:'foreign'})
            with self.assertRaisesRegex(t.audit.AuditError, 'FOREIGN_OR_MISSING_SCOPE'):
                t.run_a([t.artifact()], {'general': {'binding': t.binding(), 'rows':[row]}})

    def test_encoding_controls_and_original_hash(self):
        for encoding in ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-32', 'utf-32-le', 'utf-32-be']:
            item=t.artifact(); raw=item['bytes'].decode().encode(encoding)
            item.update(bytes=raw, sha256=hashlib.sha256(raw).hexdigest())
            with self.subTest(encoding=encoding), self.assertRaisesRegex(t.audit.AuditError, 'UNSUPPORTED_XML_ENCODING'):
                t.run_a([item])
        for declaration, accepted in [('<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE nmaprun>', True),
                                     ('<?xml version="1.0" encoding="ISO-8859-1"?>', False)]:
            item=t.artifact();raw=declaration.encode()+item['bytes'];digest=hashlib.sha256(raw).hexdigest()
            item.update(bytes=raw, sha256=digest);item['scope_lineage']['sha256']=digest
            if accepted:
                self.assertEqual(t.run_a([item]).status()['positive_observations'],1)
                self.assertEqual(item['bytes'],raw);self.assertEqual(item['sha256'],digest)
            else:
                with self.assertRaisesRegex(t.audit.AuditError, 'UNSUPPORTED_XML_ENCODING'): t.run_a([item])


if __name__ == '__main__':
    unittest.main(verbosity=2)
