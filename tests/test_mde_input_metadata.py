"""Metadata-only synthetic Spark substitutes. No real Spark, bridge or asset reads."""
from pathlib import Path
import sys
import json
import unittest
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'notebooks'))
import rcaip_mde_input_metadata as m

PRIVATE='PRIVATE_SYNTHETIC_METADATA_DO_NOT_SHARE'

class Row(dict):
    def asDict(self,recursive=True):return deepcopy(dict(self))

class Result:
    def __init__(self,rows):self.rows=rows
    def first(self):return self.rows[0] if self.rows else None
    def collect(self):return self.rows

class Frame:
    def __init__(self,spark,table,version):
        self.spark,self.table,self.version=spark,table,version
        self.columns=list(spark.records.get(table,[{}])[0])
    def filter(self,predicate):self.spark.events.append(('filter',self.table,predicate));return self
    def select(self,*fields):self.fields=fields;return self
    def limit(self,n):self.bound=n;return self
    def collect(self):
        assert self.table in (m.RUN_REGISTRY,m.PROFILE_REGISTRY),'ASSET DATA READ'
        assert self.bound==2
        self.spark.events.append(('registry_collect',self.table))
        return [Row({k:r[k] for k in self.fields}) for r in self.spark.records[self.table]][:2]
    @property
    def schema(self):
        assert self.table not in (m.RUN_REGISTRY,m.PROFILE_REGISTRY)
        self.spark.events.append(('schema',self.table,self.version))
        class Schema:
            def json(self):return '{"type":"struct","fields":[{"name":"synthetic_only","type":"string","nullable":true,"metadata":{}}]}'
        return Schema()

class Spark:
    def __init__(self):
        self.events=[];self.history_length=2;self.bad_id=False
        self.records={m.RUN_REGISTRY:[Row(run_id='synthetic_run',post_profile_key=PRIVATE,profile_hash=PRIVATE,results_namespace='synthetic',run_status='HISTORICAL',active=False)],
                      m.PROFILE_REGISTRY:[Row(post_profile_key=PRIVATE,post_slug='synthetic_post',profile_hash=PRIVATE,profile_status='CERTIFIED',active=False)]}
        self.read=self
    def format(self,value):assert value=='delta';return self
    def option(self,key,value):assert key=='versionAsOf';self.version=value;return self
    def table(self,name):self.events.append(('table',name,self.version));return Frame(self,name,self.version)
    def sql(self,query):
        self.events.append(('sql',query))
        assert query.startswith(('DESCRIBE DETAIL ','DESCRIBE HISTORY ')),query
        if query.startswith('DESCRIBE DETAIL'):return Result([Row(format='delta',id=PRIVATE,name=PRIVATE,properties={'private':PRIVATE})])
        return Result([Row(version=n,operation='WRITE',userMetadata=PRIVATE) for n in range(self.history_length-1,-1,-1)])


def versions():return {name:None if 'CONTROL' in name else 0 for name in m.STEMS}

def run(spark=None,**kw):return m.collect_mde_input_metadata(spark=spark or Spark(),post='synthetic_post',run='synthetic_run',versions=kw.get('versions',versions()),expected_results_namespace='synthetic')

class Tests(unittest.TestCase):
    def test_exact_targets_no_asset_rows_and_separated_evidence(self):
        spark=Spark();result=run(spark)
        self.assertNotIn(PRIVATE,repr(result));self.assertNotIn(PRIVATE,json.dumps(result.status()))
        self.assertEqual(result.status()['state'],'METADATA_COLLECTED_NOT_BRIDGE_READY')
        targets={e[1] for e in spark.events if e[0]=='table'}
        expected={m.RUN_REGISTRY,m.PROFILE_REGISTRY}|{'synthetic.synthetic_post_'+stem for name,stem in m.STEMS.items() if 'CONTROL' not in name}
        self.assertEqual(targets,expected)
        schemas=[e for e in spark.events if e[0]=='schema'];self.assertEqual(len(schemas),2);self.assertTrue(all(e[2]==0 for e in schemas))
        self.assertEqual(len([e for e in spark.events if e[0]=='registry_collect']),2)
        self.assertIn(PRIVATE,str(result._enterprise_evidence))
        self.assertTrue(all(x['historical_assessment_association']=='NOT_ESTABLISHED' for x in result._enterprise_evidence['products'].values()))

    def test_no_version_selection_and_inactive_exact_run(self):
        spark=Spark();run(spark,versions={n:None for n in m.STEMS})
        self.assertFalse(any(e[0]=='schema' for e in spark.events))
        self.assertFalse(any('active' in e[2] for e in spark.events if e[0]=='filter'))

    def test_explicit_control_version_only(self):
        spark=Spark();v=versions();v['IP006_CONTROL_ASSURANCE_EVALUATION']=37;run(spark,versions=v)
        self.assertIn(('schema','synthetic.synthetic_post_ip006_control_assurance_evaluation',37),spark.events)

    def test_invalid_parameters_before_reads(self):
        for v in ({},dict(versions(),UNRELATED=0),{n:'latest' for n in m.STEMS},{n:True for n in m.STEMS}):
            spark=Spark()
            with self.assertRaises(m.MetadataError):run(spark,versions=v)
            self.assertEqual(spark.events,[])

    def test_registry_scope_missing_ambiguous_and_binding_conflict(self):
        for mode in ('absent','duplicate','post','namespace','run'):
            spark=Spark()
            if mode=='absent':spark.records[m.RUN_REGISTRY]=[]
            if mode=='duplicate':spark.records[m.RUN_REGISTRY]*=2
            if mode=='post':spark.records[m.PROFILE_REGISTRY][0]['post_slug']='foreign'
            if mode=='namespace':spark.records[m.RUN_REGISTRY][0]['results_namespace']='bad;SQL'
            if mode=='run':spark.records[m.RUN_REGISTRY][0]['run_id']='foreign'
            with self.assertRaises(m.MetadataError):run(spark)
            self.assertFalse(any('synthetic_post_ip006' in str(e) for e in spark.events))

    def test_profile_hash_mismatch_is_evidence_not_prerequisite_proof(self):
        spark=Spark();spark.records[m.PROFILE_REGISTRY][0]['profile_hash']='different_current_profile'
        result=run(spark)
        self.assertEqual(result.status()['registry_profile_hash_relation'],'MISMATCH_REVIEW_REQUIRED')
        self.assertTrue(any(e[0]=='schema' for e in spark.events))

    def test_history_window_not_snapshot_proof(self):
        spark=Spark();spark.history_length=101;result=run(spark)
        self.assertTrue(all(p['history']=='BOUNDED_WINDOW' for p in result.status()['products'].values()))
        self.assertTrue(all(p['assessment_association']=='UNESTABLISHED' for p in result.status()['products'].values()))

    def test_safe_failures_and_resource_interruption(self):
        for error in (RuntimeError,MemoryError,KeyboardInterrupt):
            class Fault:
                def sql(self,q):raise error(PRIVATE)
            try:run(Fault())
            except BaseException as exc:
                import traceback
                self.assertNotIn(PRIVATE,''.join(traceback.format_exception(exc)))
                self.assertIsInstance(exc,(m.MetadataError,MemoryError,KeyboardInterrupt))
            else:self.fail('Must remain fatal')

    def test_status_copy_and_source_registry_stems(self):
        result=run();report=result.status();report['state']='ALTERED';self.assertNotEqual(report,result.status())
        source=(Path(__file__).resolve().parents[1]/'notebooks/RCAIP_Config_001_Table_Registry.py').read_text(encoding='utf-8')
        import ast
        found={}
        for node in ast.parse(source).body:
            if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id=='_add_registry_entry':
                args=node.value.args
                if isinstance(args[0],ast.Constant) and args[0].value in m.STEMS:
                    self.assertEqual(args[1].func.id,'_rcaip_post_table');found[args[0].value]=ast.literal_eval(args[1].args[0])
        self.assertEqual(found,m.STEMS)

if __name__=='__main__':unittest.main()
