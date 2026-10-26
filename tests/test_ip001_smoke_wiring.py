"""Pure standard-library handoff checks. Neither Spark smoke section is called."""
import ast
import hashlib
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
import ip001_real_spark_smoke as smoke

ROOT = Path(__file__).resolve().parents[1]
raw = (ROOT/'notebooks/rcaip_ip001_audit.py').read_bytes()
assert hashlib.sha256(raw.replace(b'\r\n',b'\n')).hexdigest() == smoke.SOURCES['audit'][1]
spec = importlib.util.spec_from_file_location('smoke_pure_audit', ROOT/'notebooks/rcaip_ip001_audit.py')
audit = importlib.util.module_from_spec(spec); sys.modules[spec.name] = audit; spec.loader.exec_module(audit)
tree = ast.parse(Path(smoke.__file__).read_bytes())
functions = {n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}


def base_row():
    node = next(n.value for n in functions['fixture'].body if isinstance(n,ast.Assign) and any(getattr(t,'id',None)=='base' for t in n.targets))
    return dict(eval(compile(ast.Expression(node), '<invented-fixture>', 'eval'), {'POST':smoke.POST,'RUN':smoke.RUN}), generated_at_utc=smoke.GENERATION)


def frame_metadata(row):
    # Metadata-only stand-in, not a DataFrame or a Spark implementation.
    return SimpleNamespace(schema=SimpleNamespace(fields=[SimpleNamespace(name=k,
        dataType=SimpleNamespace(simpleString=lambda k=k:'int' if k in ('port','service_confidence') else 'string')) for k in row]))


def actual_binding(row, captured=smoke.GENERATION):
    node = next(n for n in ast.walk(functions['run_memory']) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='Binding')
    return eval(compile(ast.Expression(node), '<actual-binding>', 'eval'),
                {'audit':audit,'captured':captured,'timestamp_frame':frame_metadata(row)})


class Wiring(unittest.TestCase):
    def test_precise_binding_and_timestamp_controls(self):
        for captured, text in [(smoke.GENERATION,'2026-10-14 10:00:00.1'),('2026-10-14 10:00:00.000000','2026-10-14 10:00:00')]:
            row=dict(base_row(),generated_at_utc=text);binding=actual_binding(row,captured)
            self.assertEqual(tuple(k for k,_ in binding.schema),tuple(row))
            self.assertEqual(smoke.audit_controls(audit,smoke.synthetic_xml([row]),binding,[row])[0]['general'],'AVAILABLE_POSITIVE')
            self.assertEqual(row['generated_at_utc'],text)
            with self.assertRaises(audit.AuditError):
                smoke.assert_audit(audit,smoke.synthetic_xml([row]),binding,[dict(row,generated_at_utc='2026-10-14 10:00:01')],'AVAILABLE_POSITIVE')

    def test_multiplicity_protocol_null_service_and_empty(self):
        row=base_row()
        rows=[row,dict(row),dict(row,protocol='udp',service=None),dict(row,scan_id='scan_b',port=80,product='literal<&',version=None)]
        statuses=smoke.audit_controls(audit,smoke.synthetic_xml(rows),actual_binding(row),rows)
        self.assertEqual([s['general'] for s in statuses],['AVAILABLE_POSITIVE','AVAILABLE_ZERO_POSITIVES','MEMBERSHIP_MISMATCH'])
        self.assertEqual((statuses[2]['general_missing'],statuses[2]['general_extra']),(1,1))
        empty=smoke.audit_controls(audit,smoke.synthetic_xml([]),actual_binding(row),[])
        self.assertTrue(all(s['general']=='AVAILABLE_ZERO_POSITIVES' for s in empty))
        self.assertTrue(all(s['overall_compliance']=='NOT_ASSESSED' for s in statuses+empty))

    def test_unexpected_mismatch_fails_and_empty_oracle_control(self):
        row=base_row();binding=actual_binding(row)
        status=audit.audit([],{'general':{'binding':binding,'rows':[row]}},post=smoke.POST,run=smoke.RUN).status()
        self.assertEqual((status['general'],status['general_missing'],status['general_extra']),('MEMBERSHIP_MISMATCH',0,1))
        with self.assertRaisesRegex(AssertionError,'AUDIT_STAGE_UNEXPECTED'):
            smoke.audit_controls(audit,smoke.synthetic_xml([]),binding,[row])
        with self.assertRaisesRegex(AssertionError,'AUDIT_DISCREPANCY_UNEXPECTED'):
            smoke.assert_audit(audit,[],binding,[row],'MEMBERSHIP_MISMATCH',1,1)

    def test_both_sections_capture_checked_results(self):
        for name in ['run_memory','run_delta']:
            calls=[n for n in ast.walk(functions[name]) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='audit_controls']
            self.assertEqual(len(calls),1)
            self.assertTrue(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='extend' and calls[0] in n.args for n in ast.walk(functions[name])))
        self.assertNotIn('pyspark',sys.modules)


if __name__ == '__main__':
    unittest.main(verbosity=2)
