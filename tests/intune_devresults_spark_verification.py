"""Opt-in target Spark/Delta + Workspace verification. NOTHING runs on import.

Fresh and resume invocations must be in separate Python contexts. All records
are generated here; the guard forbids reads outside the disclosed test bindings.
No cleanup. No raw acquisition, Config execution or dependency installation.
"""
import hashlib
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from datetime import datetime

FILES = {
    'delivery':'rcaip_intune_delivery.py', 'storage':'rcaip_intune_devresults.py',
    'validator':'rcaip_intune_validation.py', 'profile':'rcaip_intune_evidence_profile.py',
    'engine':'RCAIP_Asset_Engine_v3.py', 'config':'RCAIP_Config_000_Post_Run_Config.py',
    'registry':'RCAIP_Config_001_Table_Registry.py',
    'writer':'RCAIP_IP_001_Open_Port_Intelligence v2.0.py', 'audit':'rcaip_ip001_audit.py'}


def load_release(directory, manifest_path):
    manifest = json.loads(Path(manifest_path).read_text(encoding='utf-8'))
    texts = {}
    for role, name in FILES.items():
        raw = (Path(directory)/name).read_bytes()
        if hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest() != manifest['notebooks/'+name]['lf_sha256']:
            raise ValueError('DEPLOYED_RELEASE_CHANGED')
        texts[role] = raw.decode('utf-8')
    module = ModuleType('verified_target_intune_delivery'); sys.modules[module.__name__] = module
    module.__reviewed_source__ = texts.pop('delivery')
    exec(compile(module.__reviewed_source__, '<verified-delivery>', 'exec'), module.__dict__)
    return module, texts


def synthetic_inputs(catalog, principal, workflow_id, *, session_timezone, workspace_host):
    # This metadata is explicitly synthetic; it is never offered as live authority.
    fields = ['properties_DeviceName', 'properties_LastContact', 'properties_EncryptionStatusString']
    return {'runtime':dict(RCAIP_POST_NAME='SYNTHETIC', RCAIP_POST_SLUG='synthetic',
        RCAIP_RUN_ID='synthetic-'+workflow_id, RCAIP_POST_DEVICE_PREFIX='SYN-',
        RCAIP_ASSESSMENT_END_DATE_TEXT='2026-10-16', RCAIP_POST_TOKENS=[], RCAIP_DNS_TOKENS=[],
        RCAIP_USER_LOCATION_TOKENS=[], RCAIP_MDE_TOKENS=[], certification_reference='SYNTHETIC FIXTURE ONLY'),
        'metadata':dict(source='example_sources.intune_devices_1',
        version=2558, provider='delta', table_id='SYNTHETIC-NOT-A-LIVE-BINDING',
        schema_origin='VERSION_AS_OF_2558', session_timezone=session_timezone,
        schema={'type':'struct','fields':[dict(name=n,type='string',nullable=True,metadata={}) for n in fields]}),
        'workflow_id':workflow_id, 'generation_utc':'2026-10-23 12:00:00.000000',
        'registry':{'catalog':catalog,'results_namespace':'devresults','reference':'EXPLICIT SYNTHETIC TEST REGISTRY'},
        'principal':principal, 'workspace_host':workspace_host, 'input_authority_reference':'SYNTHETIC GENERATOR - NO DEPARTMENT VALUES'}


class GuardedSpark:
    def __init__(self, spark, tables):
        self._spark=spark;self.allowed=set(tables);self.conf=spark.conf
        class Catalog:
            def currentCatalog(inner):return spark.catalog.currentCatalog()
            def databaseExists(inner,name):
                if name not in {'.'.join(t.split('.')[:2]) for t in self.allowed}:raise AssertionError('UNAPPROVED_NAMESPACE')
                return spark.catalog.databaseExists(name)
            def tableExists(inner,name):
                if name not in self.allowed:raise AssertionError('RAW_READER_DISABLED')
                return spark.catalog.tableExists(name)
        self.catalog=Catalog()
    @property
    def read(self):
        reader=self._spark.read;allowed=self.allowed
        class Reader:
            def option(inner,k,v):
                nonlocal reader
                if k!='versionAsOf':raise AssertionError('ONLY_PINNED_READS')
                reader=reader.option(k,v);return inner
            def table(inner,name):
                if name not in allowed:raise AssertionError('RAW_READER_DISABLED')
                return reader.table(name)
        return Reader()
    def sql(self,text):
        match=re.fullmatch(r'DESCRIBE (?:DETAIL|HISTORY) ([A-Za-z_][\w.]*)?(?: LIMIT 100)?',text)
        if not match or match[1] not in self.allowed:raise AssertionError('RAW_READER_DISABLED')
        return self._spark.sql(text)
    def createDataFrame(self,*args,**kwargs):return self._spark.createDataFrame(*args,**kwargs)


def verify(directory, manifest_path, *, catalog, principal, workflow_id, session_timezone, workspace_host, mode='PLAN_ONLY',
           spark=None, client=None, approval=None, anchor=None, progress=None, stop_at='after_evidence_commit'):
    delivery,sources=load_release(directory,manifest_path)
    inputs=synthetic_inputs(catalog,principal,workflow_id, session_timezone=session_timezone, workspace_host=workspace_host)
    disclosed=delivery.run(inputs,sources)
    if mode=='PLAN_ONLY':
        return dict(disclosed, verification='SYNTHETIC ONLY; RAW READER DISABLED',
                    stop_at=stop_at, fresh_context_resume_required=True)
    if mode not in ('FRESH','RESUME'):raise ValueError('MODE_REQUIRED')
    storage=ModuleType('target_verified_storage');exec(compile(sources['storage'],'<storage>','exec'),storage.__dict__)
    plan,pid,acquisition,v,p,writer,audit=storage.prepare(inputs,sources,delivery)
    guard=GuardedSpark(spark,plan['tables'].values())
    counters={'synthetic_acquire':0,'profile':0}
    class Analysis(delivery.SparkIO):
        def acquire(self,*args):
            if mode=='RESUME':raise AssertionError('RAW_READER_DISABLED')
            counters['synthetic_acquire']+=1
            columns='asset_key string,intune_last_contact timestamp,_validation_time_raw string,_intune_encryption_state_raw string,intune_encryption_state string,observation_age_bucket string,intune_device_name string,intune_operating_system string,intune_os_version string,rcaip_run_id string'
            t=datetime(2026,10,15)
            rows=[('SYN-A',t,'2026-10-15','true','REPORTED_ENCRYPTED','Current','SYN-A','Windows','1'),
                  ('SYN-A',t,'2026-10-15','false','REPORTED_NOT_ENCRYPTED','Current','SYN-A','Windows','1'),
                  ('SYN-B',None,'bad',None,'NO_ENCRYPTION_VALUE','Unknown','SYN-B',None,None),
                  ('SYN-C',datetime(2020,1,1),'2020-01-01','weird','UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW','Stale','SYN-C','iOS','1'),
                  (None,None,None,None,'NO_ENCRYPTION_VALUE','Unknown',None,None,None)]
            rows += [rows[0],rows[0]]
            frame=guard.createDataFrame([tuple(r)+(inputs['runtime']['RCAIP_RUN_ID'],) for r in rows],columns)
            return frame,self.summary(frame),{'origin':'SYNTHETIC_GENERATOR','raw_reads':0}
        def profile(self,*args):
            counters['profile']+=1;return super().profile(*args)
    class Store(storage.DeltaStore):
        def analysis(self,v,p):return Analysis(guard,v,p)
    store=Store(guard,plan,pid,writer,audit)
    def fault(point):
        if mode=='FRESH' and point==stop_at:raise RuntimeError('SYNTHETIC_REQUESTED_STOP')
    result=delivery.run(inputs,sources,mode=mode,approval=approval,anchor=anchor,
        backend=store,publisher=storage.WorkspacePublisher(client,plan),progress=progress,fault=fault)
    if mode=='RESUME' and counters['synthetic_acquire']!=0:raise AssertionError('ACQUISITION_ON_RESUME')
    return dict(result, synthetic_counters=counters, raw_reads=0)
