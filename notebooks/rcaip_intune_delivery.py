"""Explicit devresults fresh/checkpoint/resume integration. No calls at import.

The private filesystem implementation is historical review code, not the active
operator route. Original acquisition/profile modules are reused unchanged.
"""
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import html
import json
import os
from pathlib import Path
import re
from types import ModuleType, FunctionType
import uuid

HASHES = {
 'validator':'70ce7c022a0ae39301eceba6c9989efc920517241de57034588af8f7a25b0988',
 'profile':'59c09c974e3e5d08db75651cb979b8bb935fb1eeb54fab5fae23cc2eec07fd1a',
 'engine':'bfefa7412b2cc99b57bc32569e25f9881b82cf119c62a1a9ae15640254964d97',
 'config':'b5739b3387f219da83e52089823c009503341df2f2cb095ce96f543930966cc8'}
# The verified loader supplies the exact bytes it compiles, before execution.
_LOADED_SOURCE_SHA = hashlib.sha256(__reviewed_source__.replace('\r\n','\n').encode('utf-8')).hexdigest()
OPERATIONS = ['ACQUIRE_2558','CHECKPOINT_WRITE_VERIFY','PROFILE_SAVED','OUTPUT_WRITE_VERIFY']


class DeliveryError(ValueError):
    pass


def require(ok, reason):
    if not ok: raise DeliveryError(reason)


def raw_json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_pairs(pairs):
    obj={}
    for k,v in pairs:
        require(k not in obj,'DUPLICATE_MANIFEST_KEY');obj[k]=v
    return obj


def read_json(path):
    return json.loads(path.read_bytes(),object_pairs_hook=strict_pairs)


def exclusive(path, raw):
    with path.open('xb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    require(path.read_bytes()==raw,'WRITE_REOPEN_MISMATCH')


def publish(path, raw):
    """Flush/verify an attempt file, then atomically link without replacing final.

    Requires same-filesystem atomic hard links and exclusive file creation.
    Partial attempts remain evidence, never final completion markers.
    """
    pending=path.with_name(path.name+'.'+uuid.uuid4().hex+'.pending')
    exclusive(pending,raw)
    os.link(pending,path)  # fails if final exists; no overwrite or partial final
    require(path.read_bytes()==raw,'PUBLISHED_REOPEN_MISMATCH')


def modules(sources):
    require(set(sources)==set(HASHES),'EXACT_SOURCE_SET_REQUIRED')
    for name,value in sources.items():
        require(sha(value.replace('\r\n','\n').encode())==HASHES[name],'SOURCE_FINGERPRINT_MISMATCH')
    loaded={}
    for name in ('validator','profile'):
        m=ModuleType('verified_delivery_'+name)
        exec(compile(sources[name],'<verified-'+name+'>','exec'),m.__dict__);loaded[name]=m
    return loaded['validator'],loaded['profile']


def prepare(inputs, sources):
    """Pure explicit input validation; no filesystem or Spark reads."""
    required={'runtime','metadata','checkpoint_directory','output_directory','workflow_id',
              'storage_authorization_reference','input_authority_reference'}
    require(set(inputs)==required,'EXPLICIT_INPUT_CONTRACT_REQUIRED')
    for name in ('storage_authorization_reference','input_authority_reference'):
        require(isinstance(inputs[name],str) and inputs[name].strip(),name.upper()+'_REQUIRED')
    require(bool(re.fullmatch('[a-zA-Z0-9_-]{8,80}',inputs['workflow_id'])),'WORKFLOW_ID_REQUIRED')
    roots=[]
    for name in ('checkpoint_directory','output_directory'):
        root=Path(inputs[name])
        require(root.is_absolute() and '..' not in root.parts,'ABSOLUTE_APPROVED_DIRECTORY_REQUIRED')
        roots.append(str(root))
    require(roots[0]!=roots[1],'SEPARATE_DESTINATIONS_REQUIRED')
    v,p=modules(sources)
    parent=v.prepare(engine_source=sources['engine'],config_source=sources['config'],
                     runtime=inputs['runtime'],metadata=inputs['metadata'])
    acquisition=v.prepare_distributed(parent,engine_source=sources['engine'],config_source=sources['config'])
    payload={'contract':'INTUNE_RESTART_V1','inputs':deepcopy(inputs),'sources':dict(HASHES,delivery=_LOADED_SOURCE_SHA),
             'acquisition':acquisition.private,'acquisition_plan_id':acquisition.plan_id,
             'operations':OPERATIONS,'scan':'FULL_SNAPSHOT_SCAN_POSSIBLE',
             'format':'PARQUET_NATIVE_SCHEMA_V1','origin':'NEW_EXPLICIT_WORKFLOW'}
    return payload,sha(raw_json(payload)),acquisition,v,p


def inventory(directory):
    require(directory.is_dir() and not directory.is_symlink(),'ARTIFACT_DIRECTORY_REQUIRED')
    records={}
    for file in sorted(directory.rglob('*')):
        require(not file.is_symlink(),'SYMLINK_NOT_ALLOWED')
        if file.is_file():
            h=hashlib.sha256()
            with file.open('rb') as stream:
                for chunk in iter(lambda:stream.read(1048576),b''):h.update(chunk)
            records[file.relative_to(directory).as_posix()]={'bytes':file.stat().st_size,'sha256':h.hexdigest()}
            require(len(records)<=100000,'FILE_INVENTORY_LIMIT')
    require(bool(records),'EMPTY_ARTIFACT_DIRECTORY')
    return records


def checked_directory(root, child):
    require(root.is_dir() and not root.is_symlink(),'APPROVED_DIRECTORY_UNAVAILABLE')
    require(root.resolve()==root.absolute(),'ALIASED_DIRECTORY_REFUSED')
    return root/child


def restore_checkpoint(path, expected_hash, plan, io):
    require(isinstance(expected_hash,str) and re.fullmatch('[0-9a-f]{64}',expected_hash),
            'EXTERNAL_CHECKPOINT_HASH_REQUIRED')
    require(path.is_file(),'COMPLETE_CHECKPOINT_REQUIRED_NO_REACQUISITION')
    require(sha(path.read_bytes())==expected_hash,'CHECKPOINT_HASH_MISMATCH')
    m=read_json(path)
    require(m['plan']==plan and m['state']=='COMPLETE','CHECKPOINT_PLAN_MISMATCH')
    evidence=path.parent/'evidence'
    require(inventory(evidence)==m['files'],'CHECKPOINT_FILES_CHANGED')
    frame=io.read(evidence)
    require(io.schema(frame)==m['schema'],'CHECKPOINT_SCHEMA_MISMATCH')
    require(io.summary(frame)==m['summary'],'CHECKPOINT_ACCOUNTING_MISMATCH')
    return frame,m


@dataclass
class SavedProfileContract:
    """New checkpoint-origin contract for pure queries/complete, NOT an old Result."""
    private: dict
    validator: object


class SparkIO:
    """Classic Spark + explicitly approved shared filesystem adapter."""
    def __init__(self,spark,validator,profile):
        self.spark,self.v,self.p=spark,validator,profile

    def timezone(self):return self.spark.conf.get('spark.sql.session.timeZone')

    def schema(self,frame):return frame.schema.jsonValue()

    def write(self,frame,path):frame.write.mode('errorifexists').parquet(str(path))

    def read(self,path):return self.spark.read.parquet(str(path))

    def summary(self,frame):
        from pyspark.sql import functions as F
        rows=self.v.distributed_summary_query(frame,F).limit(2).collect()
        require(len(rows)==1,'SUMMARY_CARDINALITY')
        return self.v.complete_summary(rows[0].asDict(recursive=True))

    def equal(self,left,right):
        require([(f['name'],f['type']) for f in self.schema(left)['fields']] ==
                [(f['name'],f['type']) for f in self.schema(right)['fields']], 'SCHEMA_CHANGED')
        require(left.exceptAll(right).limit(1).count()==0 and
                right.exceptAll(left).limit(1).count()==0,'MEMBER_MULTISET_CHANGED')

    def acquire(self,plan,approval,sources):
        attempt=self.v.DistributedAttempt()
        native=self.v.approve(plan,exact_plan_id=plan.plan_id,allow_values=True,allow_full_snapshot_scan=True)
        result=self.v.execute_distributed(plan,approval=native,attempt=attempt,spark=self.spark,
                    engine_source=sources['engine'],config_source=sources['config'])
        return result.private_relation,result.status(),attempt.status()

    def profile(self,frame,summary):
        from pyspark.sql import functions as F
        fields=self.schema(frame)['fields'];types={f['name']:f['type'] for f in fields}
        require(len(fields)==len(types),'DUPLICATE_RETAINED_COLUMNS')
        variations=[n for n in self.p.VARIATIONS if n in types]
        require(all(types[n]=='string' for n in variations),'VARIATION_SCHEMA_TYPES')
        keys,groups,missing,query,base=self.p.queries(frame,F,self.v,variations)
        rows=query.limit(2).collect();require(len(rows)==1,'PROFILE_CARDINALITY')
        status=self.p.complete(rows[0].asDict(recursive=True),base,
                              SavedProfileContract({'summary':summary},self.v))
        return {'keys':keys,'contact_groups':groups,'missing_identity':missing},status

    def render_detail(self,frame,path,title):
        from pyspark.sql import functions as F
        # Cast temporal/exact numeric values in Spark before Python conversion.
        # Parquet remains native authoritative evidence; HTML is presentation only.
        columns=[F.col(f.name).cast('string').alias(f.name) if f.dataType.typeName() in
                 ('timestamp','timestamp_ntz','date','decimal','double','float') else F.col(f.name)
                 for f in frame.schema.fields]
        # Clone with a minimal globals dictionary: cloudpickle carries code by value,
        # with no import of the driver's dynamically named module on workers.
        renderer=FunctionType(detail_pages.__code__,{'__builtins__':__builtins__,'html':html},'detail_pages')
        frame.select(*columns).rdd.mapPartitions(lambda rows:renderer(rows,title)).saveAsTextFile(str(path))
        for part in path.glob('part-*'):
            target=part.with_name(part.name+'.html')
            require(not target.exists(),'OUTPUT_PART_ALREADY_EXISTS')
            part.rename(target)


def detail_pages(rows,title):
    """Bounded cards per page; executor-side rendering, no driver detail collection."""
    page=[];seen=False
    for row in rows:
        seen=True
        values=row.asDict(recursive=True) if hasattr(row,'asDict') else row
        card='<article><h2>Retained evidence record</h2><dl>'+''.join(
            '<dt>'+html.escape(str(k))+'</dt><dd><pre>'+html.escape('NULL (unavailable)' if v is None
                else repr(v) if isinstance(v,str) else str(v))+'</pre></dd>' for k,v in values.items())+'</dl></article>'
        page.append(card)
        if len(page)==100:
            yield '<!doctype html><meta charset="utf-8"><h1>'+html.escape(title)+'</h1>'+''.join(page);page=[]
    if not seen:yield '<!doctype html><meta charset="utf-8"><h1>'+html.escape(title)+'</h1><p>No retained rows in this branch.</p>'
    if page:yield '<!doctype html><meta charset="utf-8"><h1>'+html.escape(title)+'</h1>'+''.join(page)


def render_summary(status,checkpoint_hash,links,origin,plan):
    def table(headers,rows):
        return '<table border="1"><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr>'+''.join(
            '<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows)+'</table>'
    runtime=plan['inputs']['runtime'];meta=plan['inputs']['metadata']
    provenance=[('Post',runtime['RCAIP_POST_SLUG']),('Run',runtime['RCAIP_RUN_ID']),
        ('Workflow',plan['inputs']['workflow_id']),('Snapshot',meta['version']),
        ('Assessment end date',runtime['RCAIP_ASSESSMENT_END_DATE_TEXT']),
        ('Timezone',meta['session_timezone']),('Delivery source SHA-256',plan['sources']['delivery'])]
    provenance += [('Source binding',meta['source']),('Table identity',meta['table_id'])]
    provenance += [(name+' source SHA-256',value) for name,value in plan['sources'].items() if name!='delivery']
    ages=('Current','Recent','Stale','Unknown','Future Date Review')
    states=('REPORTED_ENCRYPTED','REPORTED_NOT_ENCRYPTED','UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW','NO_ENCRYPTION_VALUE')
    legend=[('age_'+str(i),a) for i,a in enumerate(ages)]
    legend += [(prefix+str(i),s) for prefix in ('state_','latest_state_') for i,s in enumerate(states)]
    legend += [('age_state_'+str(i*4+j),a+' / '+s) for i,a in enumerate(ages) for j,s in enumerate(states)]
    return ('<!doctype html><meta charset="utf-8"><title>Intune Evidence Profile</title>'
       '<style>body{font:16px system-ui;margin:2rem;max-width:1100px;color:#163047}table{border-collapse:collapse;margin:1rem 0}th,td{padding:.55rem;text-align:left;border:1px solid #ccd7df}th{background:#e5eff6}h2{margin-top:2rem}</style>'
       '<h1>Intune Evidence Profile / Internal RCSO Review</h1><p>'+html.escape(origin)+'</p>'
       '<p>Descriptive retained evidence, not a BitLocker/CSA verdict. Analytical keys are not physical devices. '
       'Unknown contact times are ambiguous. Maximum contact is not proven newest event time. '
       'Matrix key counts overlap; no customer routing or original-assessment refresh.</p>'
       '<h2>Verified provenance and time basis</h2>'+table(['Field','Value'],provenance)+
       '<h2>Private counter legend</h2>'+table(['Canonical field','Meaning'],legend)+
       '<p>latest_state counters include every latest-known-contact tie. Age uses calendar-day difference from assessment end date: negative = Future Date Review; 0-30 = Current; 31-90 = Recent; over 90 = Stale; null = Unknown. It is not report generation time.</p>'+
       '<p>Checkpoint SHA-256: '+checkpoint_hash+'</p><h2>Complete accounting</h2>'+
       table(['Metric','Count'],[(k,status['baseline'][k]) for k in ('scoped_rows','distinct_nonnull_asset_keys','missing_identity_rows')])+
       '<h2>Freshness by encryption</h2>'+table(['Age','Encryption','Observations','Analytical keys'],
          [[x[k] for k in ('age','encryption','observations','analytical_keys')] for x in status['freshness_encryption']])+
       '<h2>Same-key/contact variants: explanatory partitions</h2>'+table(['Kind','Time basis','Groups'],
          [[x[k] for k in ('kind','time_basis','groups')] for x in status['variant_groups']])+
       '<h2>Reading the partitions</h2><p>BOTH_REPORTED_STATES means the group includes both reported encryption states. '
       'OTHER_MULTIPLE_CATEGORIES means different mapped categories without that pair. RAW_REPRESENTATIONS_ONLY means '
       'different raw representations in one category. These describe evidence differences, not device verdicts. '
       'UNKNOWN_TIME groups null parsed contact times: they do not prove simultaneous observations. '
       'LATEST_KNOWN_CONTACT retains every observation tied at the maximum parsed contact; OTHER_KNOWN_CONTACT retains earlier known contacts. '
       'OS-name and OS-version representation counts are independent and include null representations.</p>'
       '<h2>Complete private evidence</h2><p>Nulls, repeats, all latest ties and separate OS/version representations retained. '
       'Missing event/identity facts are not inferred. Native typed Parquet copies accompany these escaped presentation cards.</p><ul>'+
       ''.join('<li><a href="'+html.escape(link,quote=True)+'">'+html.escape(label)+'</a></li>' for label,link in links)+'</ul>')


def _filesystem_review_only(inputs, sources, *, mode='PLAN_ONLY', approval=None, checkpoint_hash=None, spark=None,
        backend=None, fault=None):
    """One entry point. backend/fault are explicit local synthetic-test seams only.

    Real operations raise fixed DeliveryError boundaries; arbitrary runtime errors
    must remain private. Receipts never print plan or device identifiers.
    """
    inputs=deepcopy(inputs);sources=deepcopy(sources);approval=deepcopy(approval)
    plan,plan_id,acquisition,v,p=prepare(inputs,sources)
    require(mode in ('PLAN_ONLY','FRESH','RESUME'),'MODE_REQUIRED')
    if mode=='PLAN_ONLY':return {'status':'PLAN_ONLY','private_plan':plan,'plan_id':plan_id,
        'operations':OPERATIONS,'values_read':False,'scan':'FULL_SNAPSHOT_SCAN_POSSIBLE'}
    require(isinstance(approval,dict) and set(approval)=={'plan_id','reference','operations','allow_values',
            'allow_recomputation','allow_writes','checkpoint_hash'},'WORKFLOW_APPROVAL_REQUIRED')
    require(approval['plan_id']==plan_id and isinstance(approval['reference'],str) and approval['reference'].strip()
            and all(approval[x] is True for x in ('allow_values','allow_recomputation','allow_writes')),
            'EXACT_WORKFLOW_APPROVAL_REQUIRED')
    ops=OPERATIONS if mode=='FRESH' else OPERATIONS[2:]
    require(approval['operations']==ops and approval['checkpoint_hash']==checkpoint_hash,'OPERATION_OR_CHECKPOINT_APPROVAL_MISMATCH')
    require(mode!='FRESH' or checkpoint_hash is None,'FRESH_CHECKPOINT_CONFLICT')
    io=backend if backend is not None else SparkIO(spark,v,p)
    require(io.timezone()==plan['acquisition']['parent_plan']['metadata']['session_timezone'], 'SESSION_TIMEZONE_CHANGED')
    root=checked_directory(Path(inputs['checkpoint_directory']),inputs['workflow_id'])
    output_root=checked_directory(Path(inputs['output_directory']),inputs['workflow_id'])
    if mode=='FRESH':
        root.mkdir()  # existing/partial run refuses; never repeat acquisition
        exclusive(root/'workflow.json',raw_json({'plan':plan,'approval':approval}))
    else:
        require(root.is_dir() and (root/'checkpoint.json').is_file(),'COMPLETE_CHECKPOINT_REQUIRED_NO_REACQUISITION')
        require(read_json(root/'workflow.json')['plan']==plan,'WORKFLOW_PLAN_CHANGED')
    attempt=uuid.uuid4().hex
    def event(stage,state,**extra):
        exclusive(root/(attempt+'-'+stage+'-'+state+'.json'),raw_json(dict(stage=stage,state=state,
                    origin=mode,approval=approval,**extra)))
    def point(name):
        if fault:fault(name)
    stage='CHECKPOINT'
    try:
        if mode=='FRESH':
            stage='ACQUISITION'
            event('ACQUISITION','STARTED',native_phase_approval={
                'workflow_plan_id':plan_id,'acquisition_plan_id':acquisition.plan_id,
                'authorization_reference':approval['reference'],
                'allow_values':True,'scan_allowance':v.SCAN})
            frame,summary,native=io.acquire(acquisition,approval,sources)
            event('ACQUISITION','COMPLETE',native=native)
            stage='CHECKPOINT'
            io.write(frame,root/'evidence')
            point('before_checkpoint')
            saved=io.read(root/'evidence');io.equal(frame,saved)
            require(io.summary(saved)==summary,'CHECKPOINT_SUMMARY_CHANGED')
            checkpoint={'state':'COMPLETE','plan':plan,'summary':summary,'schema':io.schema(saved),'original_relation_schema':io.schema(frame),
                        'files':inventory(root/'evidence'),'new_workflow_approval':approval,'native_attempt':native}
            publish(root/'checkpoint.json',raw_json(checkpoint))
            checkpoint_hash=sha((root/'checkpoint.json').read_bytes())
            event('CHECKPOINT','COMPLETE',checkpoint_hash=checkpoint_hash)
            point('after_checkpoint')
        frame,cp=restore_checkpoint(root/'checkpoint.json',checkpoint_hash,plan,io)
        stage='PROFILE'
        profile_receipt=root/'profile.complete.json'
        if profile_receipt.exists():
            pr=read_json(profile_receipt)
            require(pr['checkpoint_hash']==checkpoint_hash,'PROFILE_CHECKPOINT_CHANGED')
            require(bool(re.fullmatch('profile-[0-9a-f]{32}',pr['directory'])),'PROFILE_DIRECTORY_INVALID')
            directory=root/pr['directory']
            require(directory.parent==root and inventory(directory)==pr['files'],'PROFILE_FILES_CHANGED')
            status=pr['status'];require(status['baseline']==cp['summary'],'PROFILE_BASELINE_CHANGED')
            products={name:io.read(directory/name) for name in ('keys','contact_groups','missing_identity')}
        else:
            event(stage,'STARTED')
            products,status=io.profile(frame,cp['summary'])
            point('during_profile')
            directory=root/('profile-'+attempt);directory.mkdir()
            for name,product in products.items():
                io.write(product,directory/name);io.equal(product,io.read(directory/name))
            pr={'checkpoint_hash':checkpoint_hash,'status':status,'directory':directory.name,'files':inventory(directory)}
            publish(profile_receipt,raw_json(pr));event(stage,'COMPLETE')
        stage='OUTPUT'
        receipt=root/'output.complete.json'
        if receipt.exists():
            published=read_json(receipt)
            require(published['checkpoint_hash']==checkpoint_hash,'OUTPUT_CHECKPOINT_CHANGED')
            dest=Path(published['directory'])
            require(dest.parent==output_root and bool(re.fullmatch('[0-9a-f]{32}',dest.name)) and inventory(dest)==published['files'],'OUTPUT_FILES_CHANGED')
        else:
            event(stage,'STARTED');output_root.mkdir(exist_ok=True)
            dest=output_root/attempt;dest.mkdir()
            for name,product in dict(products,observations=frame).items():
                io.render_detail(product,dest/name,name)
                if name=='keys':point('during_output')
            links=[(f.relative_to(dest).as_posix(),f.relative_to(dest).as_posix())
                   for f in sorted(dest.rglob('part-*')) if f.is_file()]
            exclusive(dest/'index.html',render_summary(status,checkpoint_hash,links,
                       'SYNTHETIC ONLY' if backend is not None else 'RESTORED CHECKPOINT EVIDENCE / NEW WORKFLOW',plan).encode())
            published={'checkpoint_hash':checkpoint_hash,'directory':str(dest),'files':inventory(dest)}
            publish(receipt,raw_json(published));event(stage,'COMPLETE')
        return {'status':'COMPLETE','origin':'RESTORED','checkpoint_sha256':checkpoint_hash,
                'profile_manifest_sha256':sha(profile_receipt.read_bytes()),
                'output_manifest_sha256':sha(receipt.read_bytes()),'next_operation':'REVIEW',
                'private_output':str(Path(published['directory'])/'index.html')}
    except BaseException:
        event(stage,'FAILED',checkpoint_complete=(root/'checkpoint.json').exists())
        raise


def run(inputs, sources, *, mode='PLAN_ONLY', approval=None, anchor=None,
        spark=None, client=None, backend=None, publisher=None, progress=None, fault=None):
    """Active devresults route. Old filesystem code is retained for review history only."""
    import sys
    storage = sources.get('storage', '')
    require(sha(storage.replace('\r\n', '\n').encode()) == STORAGE_SHA, 'STORAGE_SOURCE_CHANGED')
    module = ModuleType('verified_intune_devresults')
    sys.modules[module.__name__] = module
    exec(compile(storage, '<verified-devresults>', 'exec'), module.__dict__)
    helpers = ModuleType('verified_intune_delivery_helpers')
    helpers.__dict__.update(globals())
    return module.execute(inputs, sources, helpers, mode=mode, approval=approval, anchor=anchor,
        spark=spark, client=client, backend=backend, publisher=publisher, progress=progress, fault=fault)

STORAGE_SHA = '98929a85bc35311bd0fb8b3cca51b782a3edd398162e623e36923d22a94571f3'
