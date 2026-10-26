"""Synthetic substitutes only; not a Spark/Delta service test."""
import ast
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, date
import importlib.util
from pathlib import Path
import re
import sys
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import patch
import ip001_synthetic_frame as sf

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('iv', ROOT/'notebooks/rcaip_intune_validation.py')
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
ENGINE = (ROOT/'notebooks/RCAIP_Asset_Engine_v3.py').read_text(encoding='utf-8')
CONFIG = (ROOT/'notebooks/RCAIP_Config_000_Post_Run_Config.py').read_text(encoding='utf-8')
FIELDS = ['properties_DeviceName', 'properties_LastContact', 'properties_EncryptionStatusString']


def runtime():
    return dict(RCAIP_POST_NAME='SYNTHETIC', RCAIP_POST_SLUG='synthetic', RCAIP_RUN_ID='synthetic-run',
                RCAIP_POST_DEVICE_PREFIX='SYN-', RCAIP_ASSESSMENT_END_DATE_TEXT='2026-10-16',
                RCAIP_POST_TOKENS=[], RCAIP_DNS_TOKENS=[], RCAIP_USER_LOCATION_TOKENS=[],
                RCAIP_MDE_TOKENS=[], certification_reference='SYNTHETIC certified fixture')


def metadata():
    return dict(source=v.SOURCE, version=2558, provider='delta', table_id='SYNTHETIC-ID',
                schema_origin='VERSION_AS_OF_2558', session_timezone='UTC',
                schema={'type':'struct', 'fields':[{'name':n,'type':'string','nullable':True,'metadata':{}} for n in FIELDS]})


def plan(**kw):
    return v.prepare(engine_source=ENGINE, config_source=CONFIG,
                     runtime=kw.pop('runtime', runtime()), metadata=kw.pop('metadata', metadata()), **kw)


def consent(p):
    return v.approve(p, exact_plan_id=p.plan_id, allow_values=True, allow_full_snapshot_scan=True)


class Spark:
    def __init__(self, rows=()):
        self.rows, self.actions, self.options, self.tables = list(rows), 0, [], []
        self.conf = SimpleNamespace(get=lambda key:'UTC')
        self.read = self
        self.identity = 'SYNTHETIC-ID'

    def sql(self, sql):
        assert sql == 'DESCRIBE DETAIL ' + v.SOURCE
        return SimpleNamespace(first=lambda:{'id':self.identity,'format':'delta'})

    def format(self, value):
        assert value == 'delta'
        return self

    def option(self, key, value):
        self.options.append((key,value))
        return self

    def table(self, name):
        self.tables.append(name)
        return sf.D(self.rows, FIELDS)


@contextmanager
def synthetic_spark(spark):
    """Evaluate the actual selected Spark expressions with a labelled substitute."""
    def cast(c, t):
        def convert(row):
            x=c.v(row)
            if x is None:return None
            if t=='string':return x.isoformat(sep=' ') if isinstance(x,datetime) else str(x)
            if t in ('integer','int'):return int(x)
            return x
        return sf.C(convert,c.name)
    def parser(sql):
        name=re.search(r'`([^`]+)`',sql)[1]
        def parse(row):
            x=row.get(name)
            if x is None:return None
            try:
                if 'from_unixtime' in sql:return datetime.fromtimestamp(float(x))
                return datetime.fromisoformat(str(x))
            except (ValueError,OverflowError,OSError):return None
        return sf.C(parse)
    def schema(frame):
        fields=[]
        for n in frame.columns:
            vals=[r.get(n) for r in frame.rows if r.get(n) is not None]
            typ='timestamp' if vals and isinstance(vals[0],datetime) else 'string'
            fields.append(SimpleNamespace(name=n,dataType=SimpleNamespace(typeName=lambda t=typ:t)))
        return SimpleNamespace(fields=fields,jsonValue=lambda:metadata()['schema'])
    def collect(frame):
        spark.actions+=1
        return [SimpleNamespace(asDict=lambda recursive=True,r=r:deepcopy(r)) for r in frame.rows]
    f=SimpleNamespace(**vars(sf.F))
    class Conditional(sf.C):
        # SQL CASE distinguishes a matched NULL from an unmatched branch.
        def __init__(self,cases):
            self.cases=cases
            super().__init__(lambda r:next((sf.val(x).v(r) for c,x in cases if c.v(r)),None))
        def when(self,c,x):return Conditional(self.cases+[(c,x)])
        def otherwise(self,x):
            return sf.C(lambda r:next((sf.val(y).v(r) for c,y in self.cases if c.v(r)),sf.val(x).v(r)))
    f.when=lambda c,x:Conditional([(c,x)])
    f.upper=lambda c:sf.rowfn(str.upper,c)
    f.length=lambda c:sf.rowfn(len,c)
    f.regexp_extract=lambda c,p,i:sf.rowfn(lambda x:re.search(p,x)[i] if re.search(p,x) else '',c)
    f.regexp_replace=lambda c,p,r:sf.rowfn(lambda x:re.sub(p,r,x),c)
    f.expr=parser
    f.to_date=lambda c:sf.rowfn(lambda x:x.date() if isinstance(x,datetime) else date.fromisoformat(x),c)
    f.datediff=lambda a,b:sf.C(lambda row:None if sf.col(a).v(row) is None or sf.col(b).v(row) is None else (sf.col(a).v(row)-sf.col(b).v(row)).days)
    sql=ModuleType('pyspark.sql');sql.functions=f
    with patch.dict(sys.modules, {'pyspark':ModuleType('pyspark'),'pyspark.sql':sql}), \
         patch.object(sf.C,'cast',cast), \
         patch.object(sf.C,'__gt__',lambda a,b:sf.C(lambda r:a.v(r) is not None and a.v(r)>sf.val(b).v(r))), \
         patch.object(sf.C,'__lt__',lambda a,b:sf.C(lambda r:a.v(r) is not None and a.v(r)<sf.val(b).v(r)),create=True), \
         patch.object(sf.C,'__le__',lambda a,b:sf.C(lambda r:a.v(r) is not None and a.v(r)<=sf.val(b).v(r)),create=True), \
         patch.object(sf.C,'startswith',lambda a,b:sf.C(lambda r:(a.v(r) or '').startswith(b)),create=True), \
         patch.object(sf.D,'schema',property(schema),create=True), \
         patch.object(sf.D,'toDF',lambda a,*names:sf.D([dict(zip(names,[row.get(n) for n in a.columns])) for row in a.rows],list(names)),create=True), \
         patch.object(sf.D,'collect',collect,create=True):
        yield


def row(name='SYN-A', time='2026-10-15', encryption='true'):
    return dict(zip(FIELDS,[name,time,encryption]))


class Validation(unittest.TestCase):
    def test_plan_inert_default_and_optional_fields(self):
        p=plan()
        self.assertEqual(p.status()['phase'],'PLAN_ONLY')
        self.assertFalse(p.status()['values_read'])
        self.assertGreater(p.status()['optional_fields_missing'],0)

    def test_fingerprints_and_schema(self):
        with self.assertRaisesRegex(v.ValidationError,'ENGINE_FINGERPRINT'):
            v.prepare(engine_source=ENGINE+'# edit',config_source=CONFIG,runtime=runtime(),metadata=metadata())
        m=metadata();m['schema']['fields'].pop()
        with self.assertRaisesRegex(v.ValidationError,'REQUIRED_FIELDS'):plan(metadata=m)
        for key,value in [('version',2559),('source','other'),('schema_origin','CURRENT')]:
            m=metadata();m[key]=value
            with self.assertRaises(v.ValidationError):plan(metadata=m)

    def test_consent_before_any_spark_access(self):
        class Bomb:
            def __getattr__(self,k):raise AssertionError('Spark touched before consent')
        p=plan()
        for approval in [None,{'plan_id':p.plan_id,'allow_values':True}]:
            with self.assertRaises(v.ValidationError):
                v.execute(p,approval=approval,spark=Bomb(),engine_source=ENGINE,config_source=CONFIG)
        with self.assertRaisesRegex(v.ValidationError,'VALUE_APPROVAL'):v.approve(p,exact_plan_id=p.plan_id)
        with self.assertRaisesRegex(v.ValidationError,'SCAN_APPROVAL'):v.approve(p,exact_plan_id=p.plan_id,allow_values=True)

    def test_changed_plan(self):
        p=plan();a=consent(p);p.private['runtime']['RCAIP_RUN_ID']='changed'
        with self.assertRaisesRegex(v.ValidationError,'CHANGED_PLAN'):
            v.execute(p,approval=a,spark=None,engine_source=ENGINE,config_source=CONFIG)

    def run_rows(self, rows, max_rows=100):
        p=plan(max_rows=max_rows);s=Spark(rows)
        with synthetic_spark(s):
            result=v.execute(p,approval=consent(p),spark=s,engine_source=ENGINE,config_source=CONFIG)
        self.assertEqual(s.actions,1)
        self.assertEqual(s.options,[('versionAsOf',2558)])
        self.assertEqual(s.tables,[v.SOURCE])
        return result

    def test_actual_stage_raw_unknown_null_ties(self):
        result=self.run_rows([row(encryption=' True '),row(encryption='false'),
                             row('SYN-B',encryption='weird'),row('SYN-C',encryption=None),
                             row('OTHER'),row('SYN-D',time='malformed'),
                             row('SYN-E',time=None),row('SYN-F',time='2027-01-01'),
                             row('SYN-G',time='2025-01-01')])
        s=result.status()
        self.assertEqual(s['scoped_rows'],8)
        self.assertEqual(s['equal_time_groups'],1)
        self.assertEqual(s['conflicting_encryption_equal_time_groups'],1)
        self.assertEqual(s['timestamp_parse_failure_rows'],1)
        self.assertEqual(s['null_or_blank_timestamp_rows'],1)
        self.assertEqual(s['future_rows'],1);self.assertEqual(s['stale_rows'],1)
        self.assertEqual(sum(s['encryption_categories'].values()),8)
        self.assertEqual(result.private_rows[0]['_intune_encryption_state_raw'],'True')
        self.assertIn(' True ',[x for k,x in result.private_rows[0].items() if k.startswith('_validation_raw_')])
        self.assertNotIn('SYN-A',str(s))

    def test_empty_and_out_of_scope_not_empty_source(self):
        for rows in [[],[row('OTHER')]]:
            s=self.run_rows(rows).status()
            self.assertEqual(s['state'],'NO_SCOPED_MATCHES');self.assertEqual(s['source_empty'],'UNVERIFIED')

    def test_limit_overflow_no_partial_result(self):
        with self.assertRaisesRegex(v.ValidationError,'LIMIT_EXCEEDED'):self.run_rows([row(),row()],max_rows=1)

    def test_identity_and_timezone_before_values(self):
        p=plan();s=Spark([row()]);s.identity='OTHER'
        with synthetic_spark(s),self.assertRaisesRegex(v.ValidationError,'IDENTITY_CHANGED'):
            v.execute(p,approval=consent(p),spark=s,engine_source=ENGINE,config_source=CONFIG)
        self.assertEqual(s.actions,0)
        s=Spark();s.conf.get=lambda key:'Europe/London'
        with self.assertRaisesRegex(v.ValidationError,'TIMEZONE_CHANGED'):
            v.execute(p,approval=consent(p),spark=s,engine_source=ENGINE,config_source=CONFIG)

    def test_selected_ast_excludes_writers_and_dedup(self):
        helpers,stage=v.expressions(ENGINE,CONFIG)
        text=ast.unparse(ast.Module(helpers+[stage],type_ignores=[]))
        for forbidden in ['save_asset_engine_table','deduplicate_by_asset','spark.table','mde_stage']:
            self.assertNotIn(forbidden,text)
        self.assertIn('REPORTED_ENCRYPTED',text)

    def test_inert_import(self):
        source=(ROOT/'notebooks/rcaip_intune_validation.py').read_text(encoding='utf-8')
        with patch('builtins.open',side_effect=AssertionError('unexpected file access')):
            exec(compile(source,'<inert import>','exec'),{'__name__':'synthetic_import'})

    def test_metadata_exact_version_and_safe_unavailable(self):
        s=Spark()
        with synthetic_spark(s):
            m=v.source_metadata(s,expected_table_id='SYNTHETIC-ID')
        self.assertEqual(m,metadata());self.assertEqual(s.actions,0)
        self.assertEqual(s.options,[('versionAsOf',2558)])
        class Missing(Exception):
            def getCondition(self):return 'TABLE_OR_VIEW_NOT_FOUND'
            def getErrorClass(self):raise AssertionError('obsolete diagnostic called')
        s.sql=lambda sql:(_ for _ in ()).throw(Missing('private details'))
        with self.assertRaisesRegex(v.ValidationError,'SOURCE_UNAVAILABLE:TABLE_OR_VIEW_NOT_FOUND'):
            v.source_metadata(s,expected_table_id='SYNTHETIC-ID')

    def test_schema_change_stops_before_collection(self):
        p=plan();s=Spark([row()])
        changed=deepcopy(p.private['metadata']['schema']);changed['fields'][0]['nullable']=False
        with synthetic_spark(s),patch.object(sf.D,'schema',property(lambda obj:SimpleNamespace(jsonValue=lambda:changed))):
            with self.assertRaisesRegex(v.ValidationError,'PINNED_SCHEMA_CHANGED'):
                v.execute(p,approval=consent(p),spark=s,engine_source=ENGINE,config_source=CONFIG)
        self.assertEqual(s.actions,0)

    def test_missing_identity_and_blank_encryption_preserved(self):
        rt=runtime();rt['RCAIP_POST_DEVICE_PREFIX']='!!!'
        p=plan(runtime=rt);s=Spark([row('!!!',encryption='  ')])
        with synthetic_spark(s):
            result=v.execute(p,approval=consent(p),spark=s,engine_source=ENGINE,config_source=CONFIG)
        self.assertEqual(result.status()['missing_identity_rows'],1)
        self.assertEqual(result.status()['state'],'SCOPED_EVIDENCE_UNUSABLE')
        self.assertEqual(result.status()['encryption_categories'],{'NO_ENCRYPTION_VALUE':1})


if __name__=='__main__':
    unittest.main(verbosity=2)
