"""Actual delivery entry point; synthetic Spark and local typed-file substitutes.

Workers are NEW Python processes. No real Spark/Parquet/Department I/O.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime,date
from decimal import Decimal
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import test_intune_evidence_profile as t

ROOT=t.old.ROOT
spec=importlib.util.spec_from_file_location('delivery',ROOT/'notebooks/rcaip_intune_delivery.py')
d=importlib.util.module_from_spec(spec);sys.modules[spec.name]=d;d.__reviewed_source__=Path(spec.origin).read_text(encoding='utf-8');exec(compile(d.__reviewed_source__,spec.origin,'exec'),d.__dict__)
SOURCES={k:(ROOT/'notebooks'/n).read_text(encoding='utf-8') for k,n in {
 'validator':'rcaip_intune_validation.py','profile':'rcaip_intune_evidence_profile.py',
 'engine':'RCAIP_Asset_Engine_v3.py','config':'RCAIP_Config_000_Post_Run_Config.py'}.items()}


def encode(x):
    if isinstance(x,dict):return ['dict',[[k,encode(v)] for k,v in x.items()]]
    if isinstance(x,list):return ['list',[encode(v) for v in x]]
    if isinstance(x,datetime):return ['datetime',x.isoformat()]
    if isinstance(x,date):return ['date',x.isoformat()]
    if isinstance(x,Decimal):return ['decimal',str(x)]
    if isinstance(x,float):return ['float',x.hex()]
    return ['scalar',x]


def decode(x):
    tag,v=x
    if tag=='dict':return {k:decode(v) for k,v in v}
    if tag=='list':return [decode(v) for v in v]
    return {'datetime':datetime.fromisoformat,'date':date.fromisoformat,
            'decimal':Decimal,'float':float.fromhex,'scalar':lambda x:x}[tag](v)


class SyntheticIO(d.SparkIO):
    def __init__(self,v,p,disabled=False):
        self.v,self.p,self.disabled=v,p,disabled
        self.raw_calls=0;self.profile_calls=0;self.output_calls=0
        rows=[t.old.row(),t.old.row(encryption='false'),t.old.row(encryption=' True '),
              t.old.row('SYN-B',time='bad',encryption=None),
              t.old.row('SYN-C',time='2020-01-01',encryption='unknown'),
              t.old.row('SYN-D',time='2099-01-01',encryption='true'),t.old.row()]
        self.spark=t.old.Spark(rows)
        if disabled:
            self.spark.sql=lambda *a:(_ for _ in ()).throw(AssertionError('RAW_SOURCE_DISABLED'))
            self.spark.table=lambda *a:(_ for _ in ()).throw(AssertionError('RAW_SOURCE_DISABLED'))

    def timezone(self):return 'UTC'

    def schema(self,frame):
        if '_saved_schema' in frame.__dict__:return deepcopy(frame._saved_schema)
        return {'type':'struct','fields':[{'name':c,'type':'timestamp' if c=='intune_last_contact' else
                'long' if frame.rows and type(frame.rows[0].get(c)) is int else 'string',
                'nullable':True,'metadata':{}} for c in frame.columns]}

    def write(self,frame,path):
        path.mkdir()
        d.exclusive(path/'synthetic-typed.json',d.raw_json(encode({'schema':self.schema(frame),
                      'columns':frame.columns,'rows':frame.rows})))

    def read(self,path):
        saved=decode(json.loads((path/'synthetic-typed.json').read_bytes()))
        f=t.sf.D(saved['rows'],saved['columns']);f._saved_schema=saved['schema'];return f

    def equal(self,a,b):
        d.require(self.schema(a)==self.schema(b),'SCHEMA_CHANGED')
        d.require(Counter(d.raw_json(encode(r)) for r in a.rows)==Counter(d.raw_json(encode(r)) for r in b.rows),
                  'MEMBER_MULTISET_CHANGED')

    def summary(self,frame):
        with t.adapter(t.old.Spark()) as f:
            return self.v.complete_summary(self.v.distributed_summary_query(frame,f).limit(2).collect()[0].asDict())

    def acquire(self,plan,approval,sources):
        self.raw_calls+=1
        if self.disabled:raise AssertionError('RAW_SOURCE_DISABLED')
        with t.dist.distributed_adapter(self.spark):return super().acquire(plan,approval,sources)

    def profile(self,frame,summary):
        self.profile_calls+=1
        with t.adapter(t.old.Spark()) as f:
            products=self.p.queries(frame,f,self.v,[n for n in self.p.VARIATIONS if n in frame.columns])
            keys,groups,missing,query,base=products
            result=self.p.complete(query.limit(2).collect()[0].asDict(),base,
                                  d.SavedProfileContract({'summary':summary},self.v))
        return {'keys':keys,'contact_groups':groups,'missing_identity':missing},result

    def render_detail(self,frame,path,title):
        self.output_calls+=1;path.mkdir()
        pages=list(d.detail_pages(frame.rows,title)) or ['<!doctype html><h1>No retained rows in this branch</h1>']
        for i,page in enumerate(pages):d.exclusive(path/('part-%05d.html'%i),page.encode())


def inputs(root):
    (root/'checkpoints').mkdir();(root/'output').mkdir()
    parent=t.old.plan()
    return dict(runtime=parent.private['runtime'],metadata=parent.private['metadata'],
                checkpoint_directory=str(root/'checkpoints'),output_directory=str(root/'output'),
                workflow_id='SYNTHETIC_RUN_01',storage_authorization_reference='SYNTHETIC_STORAGE_ONLY',
                input_authority_reference='SYNTHETIC_CERTIFIED_FIXTURE')


def consent(cfg,mode,cp=None):
    plan=d._filesystem_review_only(cfg,SOURCES)
    return dict(plan_id=plan['plan_id'],reference='SYNTHETIC_'+mode,operations=d.OPERATIONS if mode=='FRESH' else d.OPERATIONS[2:],
                allow_values=True,allow_recomputation=True,allow_writes=True,checkpoint_hash=cp)


def worker(config_path,mode,fault_name):
    cfg=json.loads(Path(config_path).read_text());v,p=d.modules(SOURCES);io=SyntheticIO(v,p,mode=='RESUME')
    cp_path=Path(cfg['checkpoint_directory'])/cfg['workflow_id']/'checkpoint.json'
    trusted=Path(config_path).parent/'trusted-checkpoint-sha.txt'
    cp=trusted.read_text().strip() if mode=='RESUME' and trusted.exists() else None
    def fault(point):
        if point=='after_checkpoint':trusted.write_text(d.sha(cp_path.read_bytes()))
        if point==fault_name:raise RuntimeError('SYNTHETIC_INTERRUPTION_'+point)
    try:
        receipt=d._filesystem_review_only(cfg,SOURCES,mode=mode,approval=consent(cfg,mode,cp),checkpoint_hash=cp,backend=io,fault=fault)
        result={'receipt':receipt,'raw_calls':io.raw_calls,'profile_calls':io.profile_calls,'output_calls':io.output_calls}
    except Exception as e:
        result={'failure':str(e),'raw_calls':io.raw_calls,'profile_calls':io.profile_calls,'output_calls':io.output_calls}
    print(json.dumps(result));return result


class RestartTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.cfg=inputs(self.root)
        self.config=self.root/'inputs.json';self.config.write_text(json.dumps(self.cfg))

    def process(self,mode,fault='none'):
        result=subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--worker',str(self.config),mode,fault],
                              capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        record=json.loads(result.stdout)
        print('NEW_PROCESS',mode,fault,json.dumps(record),flush=True)
        return record

    def test_fresh_and_new_process_restore_no_raw_reader(self):
        fresh=self.process('FRESH');self.assertEqual(fresh['receipt']['status'],'COMPLETE');self.assertEqual(fresh['raw_calls'],1)
        out=Path(fresh['receipt']['private_output']);self.assertIn('Freshness by encryption',out.read_text())
        before={str(p):p.read_bytes() for p in self.root.rglob('*') if p.is_file() and ('profile-' in str(p) or 'output' in str(p))}
        restored=self.process('RESUME');self.assertEqual(restored['raw_calls'],0)
        self.assertEqual(restored['profile_calls'],0);self.assertEqual(restored['output_calls'],0)
        self.assertEqual(restored['receipt'],fresh['receipt'])
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)

    def test_interruption_after_checkpoint_new_process_completes(self):
        first=self.process('FRESH','after_checkpoint');self.assertIn('failure',first)
        self.assertEqual(first['profile_calls'],0)
        result=self.process('RESUME');self.assertEqual(result['receipt']['status'],'COMPLETE')
        self.assertEqual(result['raw_calls'],0);self.assertEqual(result['profile_calls'],1)
        root=Path(self.cfg['checkpoint_directory'])/self.cfg['workflow_id']
        cp=d.read_json(root/'checkpoint.json');pr=d.read_json(root/'profile.complete.json')
        self.assertEqual(pr['status']['baseline'],cp['summary'])
        self.assertEqual(cp['summary']['scoped_rows'],7)
        v,p=d.modules(SOURCES);offline=SyntheticIO(v,p,True)
        frame=offline.read(root/'evidence')
        _,expected=offline.profile(frame,cp['summary'])
        self.assertEqual(pr['status'],expected)
        self.assertEqual(offline.raw_calls,0)
        saved=decode(json.loads((root/'evidence'/'synthetic-typed.json').read_bytes()))
        self.assertEqual(len(saved['rows']),7)
        self.assertGreater(max(Counter(d.raw_json(encode(r)) for r in saved['rows']).values()),1)

    def test_interruption_before_checkpoint_refuses_resume_and_reacquisition(self):
        self.assertIn('failure',self.process('FRESH','before_checkpoint'))
        retry=self.process('RESUME');self.assertIn('COMPLETE_CHECKPOINT_REQUIRED',retry['failure']);self.assertEqual(retry['raw_calls'],0)
        retry=self.process('FRESH');self.assertIn('failure',retry);self.assertEqual(retry['raw_calls'],0)

    def test_interruption_during_profile_then_restore(self):
        self.assertIn('failure',self.process('FRESH','during_profile'))
        result=self.process('RESUME');self.assertEqual(result['receipt']['status'],'COMPLETE');self.assertEqual(result['raw_calls'],0)

    def test_interruption_during_output_reuses_completed_profile(self):
        self.assertIn('failure',self.process('FRESH','during_output'))
        result=self.process('RESUME');self.assertEqual(result['receipt']['status'],'COMPLETE')
        self.assertEqual(result['raw_calls'],0);self.assertEqual(result['profile_calls'],0)
        root=Path(self.cfg['checkpoint_directory'])/self.cfg['workflow_id']
        self.assertTrue(list(root.glob('*-OUTPUT-FAILED.json')))

    def test_approval_scope_version_schema_hash_and_destinations(self):
        v,p=d.modules(SOURCES);io=SyntheticIO(v,p)
        with self.assertRaisesRegex(d.DeliveryError,'APPROVAL'):d._filesystem_review_only(self.cfg,SOURCES,mode='FRESH',backend=io)
        auth=consent(self.cfg,'FRESH');changed=deepcopy(self.cfg);changed['output_directory']=str(self.root/'other')
        with self.assertRaisesRegex(d.DeliveryError,'APPROVAL'):d._filesystem_review_only(changed,SOURCES,mode='FRESH',approval=auth,backend=io)
        for kind in ('scope','version','schema','code'):
            cfg=deepcopy(self.cfg);sources=dict(SOURCES)
            if kind=='scope':cfg['runtime']['RCAIP_POST_SLUG']='CHANGED'
            if kind=='version':cfg['metadata']['version']=2559
            if kind=='schema':cfg['metadata']['schema']['fields']=[]
            if kind=='code':sources['profile']+='\n# changed'
            with self.assertRaises(Exception):d._filesystem_review_only(cfg,sources,mode='FRESH',approval=auth,backend=io)
        self.assertEqual(io.raw_calls,0)

    def test_corrupt_partial_checkpoint_and_wrong_external_hash(self):
        self.process('FRESH','after_checkpoint')
        root=Path(self.cfg['checkpoint_directory'])/self.cfg['workflow_id'];cp=root/'checkpoint.json'
        v,p=d.modules(SOURCES);io=SyntheticIO(v,p,True)
        with self.assertRaisesRegex(d.DeliveryError,'HASH_MISMATCH'):
            d._filesystem_review_only(self.cfg,SOURCES,mode='RESUME',checkpoint_hash='0'*64,approval=consent(self.cfg,'RESUME','0'*64),backend=io)
        data=root/'evidence'/'synthetic-typed.json';data.write_bytes(data.read_bytes()+b' ')
        result=self.process('RESUME');self.assertIn('CHECKPOINT_FILES_CHANGED',result['failure']);self.assertEqual(result['raw_calls'],0)
        cp.write_bytes(b'{');result=self.process('RESUME');self.assertIn('failure',result);self.assertEqual(result['raw_calls'],0)

    def test_actual_callback_fresh_process_stdlib_transport(self):
        # Actual SparkIO callback, not SyntheticIO.render_detail. Marshal is a
        # local trusted-test transport only, not cloudpickle or a checkpoint format.
        import marshal,types
        class Capture:
            def mapPartitions(self,fn):self.fn=fn;return self
            def saveAsTextFile(self,path):pass
        class Frame:
            schema=SimpleNamespace(fields=[])
            def __init__(self):self.rdd=Capture()
            def select(self,*a):return self
        frame=Frame();py=types.ModuleType('pyspark');sql=types.ModuleType('pyspark.sql');f=types.ModuleType('pyspark.sql.functions')
        sql.functions=f;py.sql=sql
        import re,hashlib
        guide=(ROOT/'tests/INTUNE_RESTART_SAFE_DELIVERY_OPERATOR.md').read_text(encoding='utf-8')
        ns={};exec(re.findall(r'```python\n(.*?)\n```',guide,re.S)[0],ns)
        paths={k:str(ROOT/'notebooks'/name) for k,name in {
            'delivery':'rcaip_intune_delivery.py','validator':'rcaip_intune_validation.py',
            'profile':'rcaip_intune_evidence_profile.py','engine':'RCAIP_Asset_Engine_v3.py',
            'config':'RCAIP_Config_000_Post_Run_Config.py', 'storage':'rcaip_intune_devresults.py',
            'registry':'RCAIP_Config_001_Table_Registry.py', 'writer':'RCAIP_IP_001_Open_Port_Intelligence v2.0.py',
            'audit':'rcaip_ip001_audit.py'}.items()}
        manifest=self.root/'manifest.json'
        manifest.write_text(json.dumps({'notebooks/'+Path(path).name:{'lf_sha256':hashlib.sha256(
            Path(path).read_bytes().replace(b'\r\n',b'\n')).hexdigest()} for path in paths.values()}))
        actual,_=ns['load_delivery'](paths,str(manifest))
        with patch.dict(sys.modules,{'pyspark':py,'pyspark.sql':sql,'pyspark.sql.functions':f}):
            actual.SparkIO(None,None,None).render_detail(frame,self.root/'parts','SYNTHETIC')
        fn=frame.rdd.fn;closure=dict(zip(fn.__code__.co_freevars,[x.cell_contents for x in fn.__closure__]))
        self.assertEqual(set(closure),{'renderer','title'})
        payload=self.root/'callback.bin'
        payload.write_bytes(marshal.dumps((fn.__code__,closure['renderer'].__code__,closure['title'])))
        code="import marshal,types,html,sys; c,r,t=marshal.loads(open(sys.argv[1],'rb').read()); renderer=types.FunctionType(r,{'html':html}); cell=lambda v:(lambda:v).__closure__[0]; values={'renderer':renderer,'title':t}; fn=types.FunctionType(c,{},closure=tuple(cell(values[k]) for k in c.co_freevars)); print(list(fn(iter([{'x':'<escaped>','null':None}]))))"
        result=subprocess.run([sys.executable,'-B','-c',code,str(payload)],cwd=self.root,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('&lt;escaped&gt;',result.stdout);self.assertIn('NULL (unavailable)',result.stdout)

    def test_report_time_basis_and_exact_counter_legend(self):
        result=self.process('FRESH');text=Path(result['receipt']['private_output']).read_text()
        for value in (self.cfg['runtime']['RCAIP_ASSESSMENT_END_DATE_TEXT'],'2558',self.cfg['workflow_id'],
                      'age_state_19','latest_state_3','Future Date Review / NO_ENCRYPTION_VALUE','31-90 = Recent'):
            self.assertIn(value,text)

    def test_synthetic_typed_serializer_and_rendering(self):
        value={'raw':'=PRIVATE<\"\\\n','null':None,'float':-0.0,'decimal':Decimal('0.12345678901234566'),
               'time':datetime(2026,10,23,1,2,3),'repeat':[1,1]}
        back=decode(json.loads(d.raw_json(encode(value))))
        self.assertEqual(encode(back),encode(value))
        rendered=''.join(d.detail_pages([value],'SYNTHETIC'))
        self.assertIn('&lt;',rendered);self.assertIn('NULL (unavailable)',rendered)


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':worker(*sys.argv[2:])
    else:unittest.main(verbosity=2)
