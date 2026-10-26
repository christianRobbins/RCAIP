"""Actual Delta adapter/writer/audit/publisher orchestration over SQLite/SDK doubles.

No Spark/Delta/Department service. SQL page construction is target-runtime only.
Persistent SQLite stores versioned table-shaped synthetic fixtures across processes.
"""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from unittest.mock import patch
import test_intune_delivery as old

ROOT = old.ROOT
spec = importlib.util.spec_from_file_location('dev', ROOT/'notebooks/rcaip_intune_devresults.py')
dev = importlib.util.module_from_spec(spec); spec.loader.exec_module(dev)
SOURCES = dict(old.SOURCES, **{k:(ROOT/'notebooks'/n).read_text(encoding='utf-8') for k,n in {
    'storage':'rcaip_intune_devresults.py', 'registry':'RCAIP_Config_001_Table_Registry.py',
    'writer':'RCAIP_IP_001_Open_Port_Intelligence v2.0.py','audit':'rcaip_ip001_audit.py'}.items()})
sf = old.t.sf


def inputs():
    p = old.t.old.plan()
    return dict(runtime=p.private['runtime'], metadata=p.private['metadata'], workflow_id='synthetic_delta_01',
        generation_utc='2026-10-23 12:00:00.000000', registry={'catalog':'synthetic_catalog',
        'results_namespace':'devresults','reference':'SYNTHETIC REGISTRY ONLY'},
        principal='synthetic@example.invalid', workspace_host='https://approved-workspace.example.invalid', input_authority_reference='SYNTHETIC CERTIFIED FIXTURE')


def approval(cfg, mode, anchor):
    p = old.d.run(cfg, SOURCES)
    return dict(plan_id=p['plan_id'], reference='SYNTHETIC ONLY', operations=dev.OPS if mode=='FRESH' else dev.OPS[1:],
                allow_values=True, allow_writes=True, allow_recomputation=True, anchor=anchor)


class Catalog:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('create table if not exists objects(name text primary key, identity text)')
        self.db.execute('create table if not exists commits(name text, version integer, token text, payload text, primary key(name,version))')
        self.db.execute('create table if not exists files(path text primary key, body blob)'); self.db.commit()
        self.values={}; self.version=None; self.writes=0; self.fail_role=None; self.permission=False
        self.conf=types.SimpleNamespace(isModifiable=lambda k:True,get=lambda k,default=None:self.values.get(k,default),
            set=lambda k,v:self.values.__setitem__(k,v),unset=lambda k:self.values.pop(k,None))
        self.catalog=types.SimpleNamespace(tableExists=self.exists,currentCatalog=lambda:'synthetic_catalog',
            databaseExists=lambda name:name=='synthetic_catalog.devresults');self.read=self
    def exists(self,name):return self.db.execute('select 1 from objects where name=?',(name,)).fetchone() is not None
    def sql(self,sql):
        name=sql.split()[2]
        if sql.startswith('DESCRIBE DETAIL'):
            row=self.db.execute('select identity from objects where name=?',(name,)).fetchone()
            return sf.D([{'id':row[0]}])
        return sf.D([{'version':v,'userMetadata':t} for v,t in self.db.execute(
            'select version,token from commits where name=? order by version desc limit 100',(name,))])
    def option(self,k,v):assert k=='versionAsOf';self.version=v;return self
    def table(self,name):
        row=self.db.execute('select payload from commits where name=? and version=?',(name,self.version)).fetchone()
        if not row:raise RuntimeError('SYNTHETIC VERSION UNAVAILABLE')
        data=old.decode(json.loads(row[0]));return sf.D(data['rows'],data['columns'])
    def createDataFrame(self,rows,schema):
        names=[part.strip().split()[0] for part in schema.split(',')]
        return sf.D([dict(zip(names,r)) for r in rows],names)
    def write(self,frame,name,token,mode):
        if self.permission:raise PermissionError('SYNTHETIC DENIED')
        if self.exists(name) and mode=='errorifexists':raise RuntimeError('SYNTHETIC COLLISION')
        prior=self.db.execute('select max(version) from commits where name=?',(name,)).fetchone()[0]
        version=0 if prior is None else prior+1
        rows=frame.rows
        if prior is not None and mode=='append':
            rows=self.option('versionAsOf',prior).table(name).rows+rows
        self.db.execute('insert or ignore into objects values(?,?)',(name,hashlib.sha256(name.encode()).hexdigest()))
        self.db.execute('insert into commits values(?,?,?,?)',(name,version,token,json.dumps(old.encode({'rows':rows,'columns':frame.columns}))))
        self.db.commit();self.writes+=1
        if self.fail_role and name.endswith('_'+self.fail_role):os._exit(77)


@contextmanager
def table_api(db):
    # Only low-level Spark/SDK transport substitutions; actual DeltaStore methods run.
    class Writer:
        def __init__(self,frame):self.frame=frame;self.token=None;self.mode_value=None
        def option(self,k,v):assert k=='userMetadata';self.token=v;return self
        def mode(self,v):self.mode_value=v;return self
        def format(self,v):assert v=='delta';return self
        def saveAsTable(self,name):
            assert db.values['spark.databricks.delta.commitInfo.userMetadata']==self.token
            db.write(self.frame,name,self.token,self.mode_value)
    def schema(frame):
        value=old.SyntheticIO(None,None).schema(frame)
        fields=[types.SimpleNamespace(name=f['name'],dataType=types.SimpleNamespace(
            simpleString=lambda f=f:f['type'])) for f in value['fields']]
        return types.SimpleNamespace(fields=fields,jsonValue=lambda:value)
    def drop(frame,*names):return sf.D([{k:v for k,v in r.items() if k not in names} for r in frame.rows],
                                     [k for k in frame.columns if k not in names])
    py=types.ModuleType('pyspark');sql=types.ModuleType('pyspark.sql');sql.functions=sf.F;py.sql=sql
    errors=types.ModuleType('databricks.sdk.errors');errors.NotFound=NotFound
    workspace=types.ModuleType('databricks.sdk.service.workspace')
    workspace.ExportFormat=workspace.ImportFormat=types.SimpleNamespace(AUTO='AUTO')
    with patch.dict(sys.modules,{'pyspark':py,'pyspark.sql':sql,'pyspark.sql.functions':sf.F,
           'databricks.sdk.errors':errors,'databricks.sdk.service.workspace':workspace}), \
         patch.object(sf.D,'schema',property(schema),create=True), patch.object(sf.D,'drop',drop), \
         patch.object(sf.D,'write',property(lambda f:Writer(f)),create=True):
        yield


class NotFound(Exception):pass


class Workspace:
    def __init__(self,db):
        self.db=db;self.workspace=self;self.uploads=0;self.corrupt=False;self.interrupt=False
        self.config=types.SimpleNamespace(host=inputs()['workspace_host'])
        principal=inputs()['principal']  # Capture this synthetic identity once; me() is not an analytical preflight.
        self.current_user=types.SimpleNamespace(me=lambda:types.SimpleNamespace(user_name=principal))
    def get_status(self,path):
        if not self.db.db.execute('select 1 from files where path=?',(path,)).fetchone():raise NotFound()
    def mkdirs(self,path):pass
    def upload(self,path,content,format,overwrite):
        assert overwrite is False
        self.db.db.execute('insert into files values(?,?)',(path,content.read()));self.db.db.commit();self.uploads+=1
        if self.interrupt and path.endswith('.html'):raise RuntimeError('SYNTHETIC INTERRUPTION AFTER UPLOAD')
    def download(self,path,format):
        raw=self.db.db.execute('select body from files where path=?',(path,)).fetchone()[0]
        return io.BytesIO(raw+b'CORRUPT' if self.corrupt and path.endswith('.html') else raw)


class Store(dev.DeltaStore):
    def __init__(self,db,plan,plan_id,writer,audit,disabled=False):
        super().__init__(db,plan,plan_id,writer,audit);self.disabled=disabled;self.analytics=None
    def analysis(self,v,p):
        self.analytics=old.SyntheticIO(v,p,self.disabled);return self.analytics
    def proof(self,frame):
        counts=Counter(dev.digest(dev.blob(old.encode(r))) for r in frame.rows)
        return sf.D([{'row_sha256':k,'count':v} for k,v in counts.items()],['row_sha256','count'])
    def pages(self,products):
        rows=[]
        for name,f in products.items():
            for i,page in enumerate(old.d.detail_pages(f.rows,name)):
                rows.append({'product':name,'page':i,'html':page})
        return sf.D(rows,['product','page','html'])


def invoke(root,mode,fault='none',cfg=None):
    cfg=cfg or inputs();plan,pid,_,_,_,writer,audit=dev.prepare(cfg,SOURCES,old.d)
    db=Catalog(root/'tables.sqlite');store=Store(db,plan,pid,writer,audit,mode=='RESUME');ws=Workspace(db)
    if fault.startswith('commit:'):db.fail_role=fault.split(':')[1]
    ws.interrupt=fault=='upload';ws.corrupt=fault=='readback'
    anchorfile=root/'trusted-anchor.json';anchor=json.loads(anchorfile.read_text()) if mode=='RESUME' and anchorfile.exists() else None
    def progress(a):anchorfile.write_text(json.dumps(a))
    def inject(point):
        if point==fault:raise RuntimeError('SYNTHETIC_INTERRUPTION')
    with table_api(db):
        try:
            result=old.d.run(cfg,SOURCES,mode=mode,approval=approval(cfg,mode,anchor),anchor=anchor,
                backend=store,publisher=dev.WorkspacePublisher(ws,plan),progress=progress,fault=inject)
            record={'receipt':result}
        except Exception as exc:record={'failure':str(exc)}
    record.update(raw_calls=store.analytics.raw_calls if store.analytics else 0,
        profile_calls=store.analytics.profile_calls if store.analytics else 0,uploads=ws.uploads)
    db.db.close();return record


class DevresultsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
    def worker(self,mode,fault='none'):
        p=subprocess.run([sys.executable,'-B',__file__,'--worker',str(self.root),mode,fault],capture_output=True,text=True)
        if fault.startswith('commit:'):
            self.assertEqual(p.returncode,77,p.stderr);print('HARD_STOP',fault,p.returncode,flush=True);return
        self.assertEqual(p.returncode,0,p.stderr);result=json.loads(p.stdout)
        print('NEW_PROCESS',mode,fault,json.dumps(result),flush=True);return result
    def test_fresh_and_complete_new_process_resume(self):
        a=self.worker('FRESH');self.assertEqual(a['receipt']['status'],'COMPLETE');self.assertEqual(a['raw_calls'],1)
        db=Catalog(self.root/'tables.sqlite')
        html=db.db.execute("select body from files where path like '%/index.html'").fetchone()[0].decode()
        self.assertIn('pinned devresults Delta products',html);self.assertNotIn('Parquet copies accompany',html)
        db.db.close()
        b=self.worker('RESUME');self.assertEqual(b['receipt'],a['receipt']);self.assertEqual((b['raw_calls'],b['profile_calls'],b['uploads']),(0,0,0))
    def test_new_process_after_evidence_commit(self):
        self.worker('FRESH','commit:evidence');r=self.worker('RESUME')
        self.assertEqual(r['receipt']['status'],'COMPLETE');self.assertEqual(r['raw_calls'],0);self.assertEqual(r['profile_calls'],1)
    def test_incomplete_proof_never_acquires(self):
        self.worker('FRESH','commit:proof');r=self.worker('RESUME')
        self.assertIn('INCOMPLETE_EVIDENCE',r['failure']);self.assertEqual(r['raw_calls'],0)
    def test_checkpoint_commit_before_return(self):
        self.worker('FRESH','commit:checkpoint');r=self.worker('RESUME');self.assertEqual(r['receipt']['status'],'COMPLETE');self.assertEqual(r['raw_calls'],0)
    def test_partial_profile_and_completed_profile_recovery(self):
        self.worker('FRESH','commit:keys');r=self.worker('RESUME')
        self.assertEqual(r['receipt']['status'],'COMPLETE');self.assertEqual(r['raw_calls'],0)
    def test_output_failure_reuses_profile_and_orphan_upload(self):
        a=self.worker('FRESH','upload');self.assertIn('failure',a)
        b=self.worker('RESUME');self.assertEqual(b['receipt']['status'],'COMPLETE');self.assertEqual(b['raw_calls'],0);self.assertEqual(b['profile_calls'],0)
    def test_publisher_mismatch_stops(self):
        a=self.worker('FRESH','readback');self.assertIn('STAGE_PUBLICATION_FAILED_NO_FALLBACK',a['failure'])
        db=Catalog(self.root/'transport.sqlite');ws=Workspace(db);ws.corrupt=True
        plan=old.d.run(inputs(),SOURCES)['private_plan']
        with table_api(db),self.assertRaisesRegex(dev.Stop,'PUBLICATION_READBACK_MISMATCH'):
            dev.WorkspacePublisher(ws,plan).publish('test.html',b'SYNTHETIC')
        db.db.close()
    def test_collision_preserves_commits(self):
        self.worker('FRESH');r=self.worker('FRESH');self.assertIn('WORKFLOW_COLLISION',r['failure']);self.assertEqual(r['raw_calls'],0)
    def test_external_anchor_and_replaced_table(self):
        self.worker('FRESH','after_checkpoint')
        a=self.root/'trusted-anchor.json';old_anchor=a.read_bytes();v=json.loads(old_anchor);v['sha256']='0'*64;a.write_text(json.dumps(v))
        r=self.worker('RESUME');self.assertIn('EXTERNAL_MANIFEST_MISMATCH',r['failure']);self.assertEqual(r['raw_calls'],0)
        a.write_bytes(old_anchor);db=Catalog(self.root/'tables.sqlite');db.db.execute("update objects set identity='CHANGED' where name like '%_evidence'");db.db.commit();db.db.close()
        r=self.worker('RESUME');self.assertIn('CHECKPOINT_MISMATCH',r['failure']);self.assertEqual(r['raw_calls'],0)
    def test_unavailable_version_and_damaged_record(self):
        self.worker('FRESH','after_checkpoint');db=Catalog(self.root/'tables.sqlite')
        db.db.execute("delete from commits where name like '%_evidence'");db.db.commit();db.db.close()
        r=self.worker('RESUME');self.assertIn('failure',r);self.assertEqual(r['raw_calls'],0)
    def test_changed_plan_code_scope_schema_permission(self):
        cfg=inputs();auth=approval(cfg,'FRESH',None)
        for key in ('scope','version','schema','code','destination'):
            c=deepcopy(cfg);s=dict(SOURCES)
            if key=='scope':c['runtime']['RCAIP_POST_SLUG']='other'
            if key=='version':c['metadata']['version']=2559
            if key=='schema':c['metadata']['schema']['fields']=[]
            if key=='code':s['storage']+='\n# changed'
            if key=='destination':c['principal']='other@example.invalid'
            with self.assertRaises(Exception):old.d.run(c,s,mode='FRESH',approval=auth)
        plan,pid,_,_,_,writer,audit=dev.prepare(cfg,SOURCES,old.d);db=Catalog(self.root/'tables.sqlite');db.permission=True
        store=Store(db,plan,pid,writer,audit)
        with table_api(db),self.assertRaises(Exception):
            old.d.run(cfg,SOURCES,mode='FRESH',approval=auth,backend=store,
                publisher=dev.WorkspacePublisher(Workspace(db),plan),progress=lambda a:None)
        self.assertEqual(store.analytics.raw_calls,0);db.db.close()

    def test_same_count_member_change_refused(self):
        self.worker('FRESH','after_checkpoint');db=Catalog(self.root/'tables.sqlite')
        name,payload=db.db.execute("select name,payload from commits where name like '%_evidence'").fetchone()
        data=old.decode(json.loads(payload));data['rows'][0]['asset_key']='CHANGED-SYNTHETIC'
        db.db.execute('update commits set payload=? where name=?',(json.dumps(old.encode(data)),name));db.db.commit();db.db.close()
        r=self.worker('RESUME');self.assertIn('failure',r);self.assertEqual(r['raw_calls'],0)

    def test_damaged_completion_never_complete(self):
        self.worker('FRESH','after_checkpoint');db=Catalog(self.root/'tables.sqlite')
        name,payload=db.db.execute("select name,payload from commits where name like '%_checkpoint'").fetchone()
        data=old.decode(json.loads(payload));data['rows'][0]['payload']='{'
        db.db.execute('update commits set payload=? where name=?',(json.dumps(old.encode(data)),name));db.db.commit();db.db.close()
        r=self.worker('RESUME');self.assertIn('failure',r);self.assertEqual(r['raw_calls'],0)

    def test_manifest_and_publication_commit_interruptions(self):
        self.worker('FRESH','commit:manifest');r=self.worker('RESUME')
        self.assertIn('EXTERNAL_MANIFEST_BINDING_REQUIRED',r['failure']);self.assertEqual(r['raw_calls'],0)
        # Separate explicitly new synthetic workflow/store; preserve the failed one.
        other=self.root/'other';other.mkdir()
        p=subprocess.run([sys.executable,'-B',__file__,'--worker',str(other),'FRESH','commit:publication'],capture_output=True)
        self.assertEqual(p.returncode,77)
        r=invoke(other,'RESUME');self.assertEqual(r['receipt']['status'],'COMPLETE')
        self.assertEqual((r['raw_calls'],r['profile_calls'],r['uploads']),(0,0,0))

    def test_native_run_nulls_duplicates_and_pinned_schema(self):
        self.worker('FRESH','after_checkpoint');db=Catalog(self.root/'tables.sqlite')
        cfg=inputs();plan,pid,_,_,_,writer,audit=dev.prepare(cfg,SOURCES,old.d);s=Store(db,plan,pid,writer,audit,True)
        with table_api(db):
            f=s.read('evidence',s.capture('evidence'));proof=s.read('proof',s.capture('proof'))
            self.assertEqual(len(f.rows),7);self.assertIn('rcaip_run_id',f.columns)
            self.assertTrue(all(r['rcaip_run_id']=='synthetic-run' for r in f.rows))
            self.assertTrue(any(r['intune_last_contact'] is None for r in f.rows))
            self.assertTrue(any(n>1 for n in Counter(dev.blob(old.encode(r)) for r in f.rows).values()))
            self.assertEqual(sum(r['count'] for r in proof.rows),7)
        db.db.close()

    def test_target_launcher_plan_and_raw_read_guard(self):
        import intune_devresults_spark_verification as launcher
        manifest={ 'notebooks/'+name:{'lf_sha256':hashlib.sha256(
            (ROOT/'notebooks'/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()}
            for name in launcher.FILES.values() }
        file=self.root/'manifest.json';file.write_text(json.dumps(manifest))
        result=launcher.verify(ROOT/'notebooks',file,catalog='synthetic_catalog',principal='synthetic@example.invalid',workflow_id='synthetic_guard_01',session_timezone='UTC',workspace_host=inputs()['workspace_host'])
        self.assertEqual(result['status'],'PLAN_ONLY');self.assertFalse(result['values_read'])
        self.assertTrue(all('.devresults.' in n for n in result['private_plan']['tables'].values()))
        db=Catalog(self.root/'guard.sqlite');guard=launcher.GuardedSpark(db,result['private_plan']['tables'].values())
        with self.assertRaisesRegex(AssertionError,'RAW_READER_DISABLED'):guard.read.option('versionAsOf',2558).table('raw.intune')
        with self.assertRaisesRegex(AssertionError,'RAW_READER_DISABLED'):guard.sql('DESCRIBE DETAIL raw.intune')
        db.db.close()

    def test_approval_snapshot_and_namespace_mismatch(self):
        cfg=inputs();auth=approval(cfg,'FRESH',None);plan,pid,_,_,_,writer,audit=dev.prepare(cfg,SOURCES,old.d)
        db=Catalog(self.root/'immutable.sqlite');store=Store(db,plan,pid,writer,audit)
        original=store.analysis
        def analysis(v,p):
            obj=original(v,p)
            def mutate():cfg['principal']='unapproved@example.invalid';auth['reference']='MUTATED';return 'UTC'
            obj.timezone=mutate;return obj
        store.analysis=analysis
        with table_api(db):
            r=old.d.run(cfg,SOURCES,mode='FRESH',approval=auth,backend=store,
                publisher=dev.WorkspacePublisher(Workspace(db),plan),progress=lambda x:None)
        self.assertIn('/Users/synthetic@example.invalid/',r['private_report'])
        db.db.close()


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':print(json.dumps(invoke(Path(sys.argv[2]),sys.argv[3],sys.argv[4])))
    else:unittest.main(verbosity=2)
