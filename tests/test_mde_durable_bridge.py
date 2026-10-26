"""Synthetic durable bridge tests; no Spark, enterprise or workbook execution."""
import ast
from copy import deepcopy
from decimal import Decimal
import itertools
from pathlib import Path
import sys
from contextlib import contextmanager
import uuid
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'notebooks'))
import rcaip_mde_durable_bridge as bridge
import test_mde_retained_check as fixture


@contextmanager
def synthetic_directory():
    # Normal inherited directory permissions on Windows; no private-temp ACL.
    path = ROOT / 'outputs' / ('synthetic-bridge-test-' + uuid.uuid4().hex)
    path.mkdir()
    try:
        yield path
    finally:
        for child in path.iterdir():
            assert child.is_file()
            child.unlink()
        path.rmdir()


def setup(shared=False):
    book = fixture.shared_fixture() if shared else fixture.fixture()
    q = fixture.records(book, 0); gates = fixture.records(book, 1)
    sources = fixture.sources()
    derived = {'mde_qualification_cohort', 'mde_qualification_meaning', 'mde_next_gate', 'potential_current_control_review', 'current_identity_review_required'}
    enrich = [{k: v for k, v in r.items() if k not in derived} for r in q]
    search = [{k: r[k] for k in ('asset_key', 'post_slug', 'rcaip_run_id', 'search_eligibility_class', 'search_eligible')} for r in enrich]
    mapping = bridge._projection(sources[11], 'mde_control_context_df')
    control = []
    for row in gates:
        r = {original: row[alias] for alias, original in mapping.items()}
        r.update(post_slug=fixture.POST, rcaip_run_id=fixture.RUN, tool_name='MDE')
        control.append(r)
    # In this synthetic assessment, these are the complete MDE control rows.
    # Production never equates a bounded candidate slice with a full-Post total.
    fixture.cert(book,1,'Current-run MDE Control Assurance rows',len(control))
    fixture.cert(book,1,'Current-run MDE Control Assurance distinct assets',len(control))
    fixture.cert(book,1,'Rediscovery vs Control Assurance ownership/boundary mismatches',
                 sum((r['rediscovery_management_owner'] or '') != (r['ca_management_owner'] or '')
                     or (r['rediscovery_operational_boundary'] or '') != (r['ca_operational_boundary'] or '') for r in gates))
    # Saved summaries are synthetic original outputs, independent expected labels
    # from the accepted fixture. Remove full member displays to model actual gap.
    qsummary = []
    for r in q:
        item = {k:r[k] for k in ('mde_qualification_cohort','mde_qualification_meaning','mde_next_gate')}
        item.update(assets=1, share_of_mde_missing_pct=Decimal('25.00'))
        for prefix in ('resolved','current','stale','historical'):item[prefix+'_mde_entities']=r[prefix+'_mde_entity_count'] or 0
        qsummary.append(item)
    gsummary = [dict(mde_current_analytical_gate=r['mde_current_analytical_gate'], mde_current_gate_meaning=r['mde_current_gate_meaning'], assets=1, share_of_current_unresolved_pct=Decimal('50.00')) for r in gates]
    book['cells'][0]['outputs'][1:] = [fixture.table(qsummary,list(qsummary[0]))]
    book['cells'][1]['outputs'][1:] = [fixture.table(gsummary,list(gsummary[0]))]
    # Preserve the ordinary multi-display layouts from both pinned producers.
    from collections import Counter
    for index, number, rows in ((0,10,q),(1,11,gates)):
        assignments = bridge.check._assignments(sources[number])
        displayed = [n.value.args[0].id for n in ast.parse(sources[number]).body
                     if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call)
                     and isinstance(n.value.func,ast.Name) and n.value.func.id=='display']
        primary = 'mde_qualification_funnel_df' if number==10 else 'mde_current_gate_summary_df'
        for name in displayed:
            if name==primary:continue
            groups = bridge.check._calls(assignments[name], 'groupBy')
            if not groups:continue
            keys = [ast.literal_eval(a) for a in groups[0].args]
            counts = Counter(tuple(row[k] for k in keys) for row in rows)
            context = [dict(zip(keys,key),assets=count) for key,count in counts.items()]
            book['cells'][index]['outputs'].append(fixture.table(context,keys+['assets']))
    raw = fixture.raw(book)
    manifest = dict(post=fixture.POST, run=fixture.RUN, retained_sha256=bridge.digest(raw), inputs={})
    inputs, evidence, trust = {}, {}, {}
    for i,(name,rows) in enumerate(zip(bridge.INPUTS,(search,enrich,control))):
        payload = fixture.table(rows,list(rows[0]))['metadata'][bridge.retained.MIME]
        # Use decoded schema for the snapshot envelope, preserving recorded types.
        payload['schema'] = [{'name':f['name'],'type':bridge.retained._schema_type(f['type'])} for f in payload['schema']]
        b = dict(table='synthetic.product_'+str(i), table_id='SYNTHETIC_DELTA_ID_'+str(i), version=7,
                 schema_sha256=bridge.digest(bridge.encoded(payload['schema'])))
        identity=dict(physical_table=b['table'],table_id=b['table_id'],version=b['version'],schema_sha256=b['schema_sha256'],format='delta',object_type='TABLE')
        records={'registry':dict(logical_product=name,physical_table=b['table']), 'snapshot':identity,
                 'assessment':dict(identity,logical_product=name,post=fixture.POST,run=fixture.RUN,retained_sha256=manifest['retained_sha256'],producer_fingerprints={str(n):h for n,h in bridge.check.FINGERPRINTS.items()})}
        for role,record in records.items():
            ref=name+'/'+role; evidence[ref]=bridge.encoded({'record':record})
            trust[ref]=dict(role='assessment_input_binding' if role=='assessment' else role,sha256=bridge.digest(evidence[ref]))
            b[role]=dict(reference=ref,fields={key:['record',key] for key in record})
        manifest['inputs'][name]=b
        inputs[name]=dict(table=b['table'],table_id=b['table_id'],version=b['version'],payload=payload)
    return manifest,evidence,trust,inputs,raw


def sql_expr(node, row, names):
    """Independent restricted expression oracle; no eval/exec or notebook execution."""
    if isinstance(node,ast.Constant):return node.value
    if isinstance(node,ast.Name):return sql_expr(names[node.id],row,names)
    if isinstance(node,ast.BinOp):
        a,b=sql_expr(node.left,row,names),sql_expr(node.right,row,names)
        if isinstance(node.op,ast.BitAnd):return False if a is False or b is False else None if a is None or b is None else True
        if isinstance(node.op,ast.BitOr):return True if a is True or b is True else None if a is None or b is None else False
    if isinstance(node,ast.UnaryOp):
        a=sql_expr(node.operand,row,names);return None if a is None else not a
    if isinstance(node,ast.Compare):
        a,b=sql_expr(node.left,row,names),sql_expr(node.comparators[0],row,names)
        return None if a is None or b is None else a==b if isinstance(node.ops[0],ast.Eq) else a!=b
    if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
        method=node.func.attr
        if isinstance(node.func.value,ast.Name) and node.func.value.id=='F':
            args=[sql_expr(a,row,names) for a in node.args]
            if method=='col':return row[args[0]]
            if method=='lit':return args[0]
            if method=='coalesce':return next((v for v in args if v is not None),None)
        value=sql_expr(node.func.value,row,names);args=[sql_expr(a,row,names) for a in node.args]
        if method=='isin':return None if value is None else value in args
        if method=='startswith':return None if value is None else value.startswith(args[0])
        if method=='contains':return None if value is None else args[0] in value
    raise AssertionError(ast.dump(node))


def ordered_branches(expression):
    branches=[];fallback=None
    while isinstance(expression,ast.Call) and isinstance(expression.func,ast.Attribute):
        name=expression.func.attr
        if name=='otherwise':fallback=ast.literal_eval(expression.args[0].args[0])
        elif name=='when':branches.append((expression.args[0],ast.literal_eval(expression.args[1].args[0])))
        expression=expression.func.value
    return list(reversed(branches)),fallback


class BridgeTests(unittest.TestCase):
    def test_end_to_end_roundtrip_and_original_preservation(self):
        args=setup(shared=True);original=deepcopy(args)
        result=bridge.reproduce(*args)
        self.assertEqual(args,original)
        self.assertEqual(len(result['qualification']),4)
        self.assertEqual(result['member_map_kind'],'REPRODUCED')
        self.assertEqual(result['original_kind'],'ORIGINAL_RETAINED')
        self.assertEqual(result['qualification'][0]['latest_mde_evidence_timestamp'],'0000-01-01T00:00:00')
        self.assertEqual(result['qualification'][0]['canonical_name'],'=PRIVATE_NAME\x01\\literal')
        with synthetic_directory() as d:
            dest=Path(d)/'synthetic-derived-v1.json'
            meta=bridge.retain_bundle(result,dest,approved_directory=d,trusted_references=args[2],original_raw=args[4])
            self.assertEqual(meta['sha256'],bridge.digest(dest.read_bytes()))
            bridge.validate_bundle(bridge.strict_json(dest.read_bytes()),args[2],args[4])
            with self.assertRaisesRegex(bridge.InputContractError,'destination'):bridge.retain_bundle(result,dest,approved_directory=d,trusted_references=args[2],original_raw=args[4])
            self.assertNotIn(fixture.PRIVATE,str(meta))

    def test_inspection_is_metadata_only_and_no_reproduction(self):
        args=setup();report=bridge.inspect_inputs(*args[:3]);self.assertEqual(report['reproduction'],'NOT_RUN')
        self.assertEqual(report['retention'],'NOT_RUN');self.assertNotIn(fixture.PRIVATE,str(report))
        args[0]['inputs'][bridge.CONTROL]['version']=8
        report=bridge.inspect_inputs(*args[:3]);self.assertEqual(report['state'],'INPUT_PROVENANCE_NOT_ESTABLISHED')
        self.assertNotIn('synthetic.product',str(report))

    def test_manifest_and_witness_failures(self):
        mutations=[lambda a:a[0]['inputs'][bridge.CONTROL].pop('assessment'),
                   lambda a:a[0]['inputs'][bridge.CONTROL].update(version=8),
                   lambda a:a[0]['inputs'][bridge.CONTROL].update(table_id='wrong'),
                   lambda a:a[0]['inputs'][bridge.CONTROL].update(table='synthetic.other'),
                   lambda a:a[0].update(run='synthetic_cc2'),
                   lambda a:a[2].clear(),lambda a:a[1].clear(),
                   lambda a:a[0]['inputs'][bridge.CONTROL].update(verified=True)]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                args=setup();mutate(args)
                with self.assertRaises(bridge.InputContractError):bridge.reproduce(*args)

    def test_wrong_snapshot_and_keys(self):
        for mode in ('version','duplicate','missing','foreign','scope','control_duplicate','tool'):
            with self.subTest(mode=mode):
                args=setup();payload=args[3][bridge.ENRICHMENT]['payload'];cols=[f['name'] for f in payload['schema']]
                if mode=='version':args[3][bridge.ENRICHMENT]['version']=8
                if mode=='duplicate':payload['data'].append(deepcopy(payload['data'][0]))
                if mode=='missing':payload['data'].pop()
                if mode=='foreign':payload['data'][0][cols.index('asset_key')]='OTHER'
                if mode=='scope':payload['data'][0][cols.index('rcaip_run_id')]='other'
                cp=args[3][bridge.CONTROL]['payload'];cc=[f['name'] for f in cp['schema']]
                if mode=='control_duplicate':cp['data'].append(deepcopy(cp['data'][0]))
                if mode=='tool':cp['data'][0][cc.index('tool_name')]='OTHER'
                with self.assertRaises(bridge.InputContractError):bridge.reproduce(*args)

    def test_native_contradictions_and_same_cell_binding(self):
        for mode in ('signal','support','case','summary','source','overflow'):
            with self.subTest(mode=mode):
                args=setup(shared=True)
                book=bridge.strict_json(args[4])
                if mode=='signal':fixture.set_value(book,2,0,'final_resolution_signal','OTHER',output=2)
                if mode=='support':fixture.set_value(book,2,0,'shared_identity_value','ABSENT',output=4)
                if mode=='case':fixture.set_value(book,2,0,'affected_assets',2)
                if mode=='summary':fixture.set_value(book,0,0,'assets',99)
                if mode=='source':book['cells'][0]['source']+='\nchanged = True\n'
                if mode=='overflow':fixture.payload(book,2)['overflow']=True
                # Synthetic witnesses explicitly rebound to this synthetic artifact;
                # reaching native checks must not fail merely at the file hash.
                raw=fixture.raw(book);newhash=bridge.digest(raw);args=list(args);args[4]=raw;args[0]['retained_sha256']=newhash
                for name in bridge.INPUTS:
                    ref=name+'/assessment';doc=bridge.strict_json(args[1][ref]);doc['record']['retained_sha256']=newhash
                    args[1][ref]=bridge.encoded(doc);args[2][ref]['sha256']=bridge.digest(args[1][ref])
                with self.assertRaises(bridge.InputContractError):bridge.reproduce(*args)

    def test_precise_transport_and_mutated_bundle_rejection(self):
        args=setup();p=args[3][bridge.ENRICHMENT]['payload']
        for field,value,kind in [('synthetic_decimal',Decimal('123456789012345678.123456789'),'decimal(30,9)'),('synthetic_zero',Decimal('-0.0'),'double')]:
            p['schema'].append(dict(name=field,type=kind))
            for row in p['data']:row.append(value)
        # Update all synthetic schema witnesses, not a live provenance override.
        h=bridge.digest(bridge.encoded(p['schema']));args[0]['inputs'][bridge.ENRICHMENT]['schema_sha256']=h
        for role in ('snapshot','assessment'):
            ref=bridge.ENRICHMENT+'/'+role;doc=bridge.strict_json(args[1][ref]);doc['record']['schema_sha256']=h
            args[1][ref]=bridge.encoded(doc);args[2][ref]['sha256']=bridge.digest(args[1][ref])
        result=bridge.reproduce(*args);reopened=bridge.strict_json(bridge.encoded(result))
        self.assertEqual(str(reopened['qualification'][0]['synthetic_zero']),'-0.0')
        self.assertEqual(reopened['qualification'][0]['synthetic_decimal'],Decimal('123456789012345678.123456789'))
        bridge.validate_bundle(reopened,args[2],args[4])
        reopened['qualification'][0]['mde_next_gate']='ALTERED'
        with self.assertRaises(bridge.InputContractError):bridge.validate_bundle(reopened,args[2],args[4])

    def test_qualification_source_expression_equivalence(self):
        source=fixture.sources()[10];assign=bridge.check._assignments(source)
        call=next(c for c in bridge.check._calls(assign['mde_qualification_df'],'withColumn') if ast.literal_eval(c.args[0])=='mde_qualification_cohort')
        branches,fallback=ordered_branches(call.args[1])
        for life,vis in itertools.product([None,'CURRENTLY_OBSERVED','NONCURRENT'],[None,'CURRENT_RESOLVED','CURRENT_DURABLE_DEVICE_CHURN','IDENTITY_OR_TEMPORAL_REVIEW_REQUIRED','STALE_REDISCOVERY','HISTORICAL_REDISCOVERY','INSUFFICIENT_IDENTITY_FOR_LOOKBACK','NO_ENTERPRISE_NAME_HIT','OTHER']):
            row=dict(lifecycle_case_state=life,rediscovery_visibility_class=vis)
            expected=next((code for cond,code in branches if sql_expr(cond,row,assign) is True),fallback)
            if expected=='UNMAPPED_REDISCOVERY_STATE':
                with self.assertRaises(bridge.InputContractError):bridge.qualification(row,source)
            else:self.assertEqual(bridge.qualification(row,source)['mde_qualification_cohort'],expected)

    def test_gate_source_expression_equivalence(self):
        source=fixture.sources()[11];assign=bridge.check._assignments(source)
        call=next(c for c in bridge.check._calls(assign['mde_current_gate_df'],'withColumn') if ast.literal_eval(c.args[0])=='mde_current_analytical_gate')
        branches,fallback=ordered_branches(call.args[1])
        for app,identity,conflict,boundary,model,owner,cohort in itertools.product([None,'NOT_APPLICABLE','APPLICABLE','REVIEW_REQUIRED'],[None,'WEAK','STRONG'],[None,False,True],[None,'HIGH'],[None,'SHARED','POST_MANAGED','ENTERPRISE_MANAGED'],[None,'POST','ENTERPRISE'],sorted(bridge.check.GATE_COHORTS)):
            row=dict(ca_mde_applicability_state=app,ca_identity_strength=identity,ca_attribution_conflict_flag=conflict,ca_operational_boundary_confidence=boundary,ca_management_model=model,ca_management_owner=owner,mde_qualification_cohort=cohort)
            expected=next((code for cond,code in branches if sql_expr(cond,row,assign) is True),fallback)
            self.assertEqual(bridge.gate_decision(row),expected)

    def test_empty_transform_and_source_drift(self):
        self.assertEqual(bridge.reproduce_members([],[],fixture.sources())[:2],([],[]))
        s=fixture.sources();s[11]+='\nchanged=True\n'
        with self.assertRaises(bridge.InputContractError):bridge.reproduce_members([],[],s)


class AdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import RCAIP_MDE_Durable_Input_Bridge as entry
        cls.entry=entry

    def test_import_and_inspect_do_not_read_rows_or_write(self):
        args=setup()
        self.assertEqual(self.entry.inspect_bridge(*args[:3],module_folder=ROOT/'notebooks')['reproduction'],'NOT_RUN')

    def test_spark_substitute_actual_entry_path(self):
        args=setup();events=[]
        class Row(dict):
            def asDict(self,recursive=True):return dict(self)
        class Conf:
            def get(self,name):return 'UTC'
        class Frame:
            def __init__(self,payload,stage='source'):
                self.payload=deepcopy(payload);self.stage=stage
                class Schema:
                    def json(inner):return bridge.encoded({'fields':self.payload['schema']}).decode()
                self.schema=Schema()
            def filter(self,expression):events.append(('filter',expression));return self
            def select(self,*columns):events.append(('select',columns));return self
            def join(self,other,**kwargs):events.append(('join',kwargs));return self
            def limit(self,n):events.append(('limit',n));return self
            def selectExpr(self,*expressions):
                events.append(('projection',expressions))
                if expressions[0].startswith('to_json'):return Frame(self.payload,'json')
                if expressions[0].startswith('count('):return Frame(self.payload,'size')
                result=Frame(self.payload,'cast')
                for col,field in enumerate(result.payload['schema']):
                    kind=field['type']
                    if isinstance(kind,str) and (kind in ('float','double') or kind.startswith('decimal(')):
                        for row in result.payload['data']:
                            if row[col] is not None:row[col]=str(row[col])
                return result
            def first(self):
                self_assert=self.stage=='size';assert self_assert
                events.append(('size',))
                return {'rows':len(self.payload['data']),'bytes':len(bridge.encoded(self.payload['data']))}
            def collect(self):
                events.append(('collect',));cols=[f['name'] for f in self.payload['schema']]
                return [Row(zip(cols,r)) for r in self.payload['data']]
        class Reader:
            def format(self,value):self.fmt=value;return self
            def option(self,k,v):self.version=v;return self
            def table(self,name):
                events.append(('pinned_read',name,self.version,self.fmt))
                product=next(p for p in bridge.INPUTS if args[0]['inputs'][p]['table']==name)
                return Frame(args[3][product]['payload'])
        class Spark:
            conf=Conf()
            read=Reader()
            def sql(self,query):
                events.append(('detail',query))
                product=next(p for p in bridge.INPUTS if args[0]['inputs'][p]['table'].replace('.','`.`') in query)
                class Detail:
                    def first(inner):return {'format':'delta','id':args[0]['inputs'][product]['table_id']}
                return Detail()
        with synthetic_directory() as d:
            path=d/'synthetic.ipynb';path.write_bytes(args[4])
            result=self.entry.reproduce_bridge(*args[:3],spark=Spark(),module_folder=ROOT/'notebooks',input_route='local',retained_path=path)
        self.assertEqual(len(result['qualification']),4)
        self.assertEqual(len([e for e in events if e[0]=='collect']),3)
        self.assertTrue(all(e[2:]==(7,'delta') for e in events if e[0]=='pinned_read'))
        self.assertTrue(any(e[0]=='join' and e[1]['how']=='left_semi' for e in events))
        self.assertEqual(len([e for e in events if e[0]=='size']),3)
        # Exercise the actual public DBFS route as well as the explicit local route.
        import io, types
        events.clear()
        class Dbfs:
            def get_status(self,path):return types.SimpleNamespace(is_dir=False,file_size=len(args[4]))
            def download(self,path):return io.BytesIO(args[4])
        dbfs_result=self.entry.reproduce_bridge(*args[:3],spark=Spark(),module_folder=ROOT/'notebooks',input_route='dbfs',source_uri='dbfs:/synthetic/original.ipynb',client=types.SimpleNamespace(dbfs=Dbfs()))
        self.assertEqual(bridge.encoded(dbfs_result),bridge.encoded(result))

    def test_empty_materialized_population_stops(self):
        args=setup()
        for item in args[3].values():item['payload']['data']=[]
        with self.assertRaisesRegex(bridge.InputContractError,'nonempty historical'):bridge.reproduce(*args)

    def test_independent_expected_precedence(self):
        q,g,aliases=bridge.reproduce_members(bridge._records(setup()[3][bridge.ENRICHMENT]['payload']),bridge._records(setup()[3][bridge.CONTROL]['payload']),fixture.sources())
        self.assertEqual([r['mde_qualification_cohort'] for r in q],['CURRENT_MDE_EVIDENCE_CONFIRMED','NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION','CURRENT_IDENTITY_TEMPORAL_REVIEW','CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'])
        self.assertEqual([r['mde_current_analytical_gate'] for r in g],['MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED','CURRENT_APPLICABLE_ENTERPRISE_MDE_CONTROL_REVIEW_CANDIDATE'])

    def test_foreign_alias_and_no_current_residual(self):
        for field,value in [('canonical_serial','CONTRADICTORY_SERIAL'),('current_mde_entity_count',1)]:
            args=setup();p=args[3][bridge.ENRICHMENT]['payload'];cols=[f['name'] for f in p['schema']]
            p['data'][3][cols.index(field)]=value
            with self.assertRaisesRegex(bridge.InputContractError,'inherited evidence|residual membership'):bridge.reproduce(*args)


    def test_byte_bound_precedes_collection(self):
        class Size:
            def first(self):return {'rows':1,'bytes':bridge.MAX_BYTES+1}
        class Frame:
            def selectExpr(self,*expr):return Size() if expr[0].startswith('count(') else self
            def limit(self,n):return self
            def collect(self):raise AssertionError('Collection must not run')
        with self.assertRaisesRegex(bridge.InputContractError,'byte bound'):self.entry._collect(Frame(),[{'name':'asset_key','type':'string'}],bridge)

    def test_alternate_post_run(self):
        args=setup();oldpost,oldrun=fixture.POST,fixture.RUN
        newpost,newrun='another_synthetic_post','another_synthetic_run'
        args=list(args);book=bridge.strict_json(args[4])
        for cell in book['cells']:
            cell['outputs'][0]['text']=cell['outputs'][0]['text'].replace('Post: '+oldpost,'Post: '+newpost).replace('Run ID: '+oldrun,'Run ID: '+newrun)
        args[4]=fixture.raw(book);args[0].update(post=newpost,run=newrun,retained_sha256=bridge.digest(args[4]))
        for name in bridge.INPUTS:
            p=args[3][name]['payload'];cols=[f['name'] for f in p['schema']]
            for row in p['data']:row[cols.index('post_slug')]=newpost;row[cols.index('rcaip_run_id')]=newrun
            ref=name+'/assessment';doc=bridge.strict_json(args[1][ref]);doc['record'].update(post=newpost,run=newrun,retained_sha256=args[0]['retained_sha256'])
            args[1][ref]=bridge.encoded(doc);args[2][ref]['sha256']=bridge.digest(args[1][ref])
        self.assertEqual(bridge.reproduce(*args)['manifest']['post'],newpost)

    def test_unapproved_retention_and_no_partial_file(self):
        args=setup();bundle=bridge.reproduce(*args);bundle['manifest']['inputs'][bridge.CONTROL]['version']=99
        with synthetic_directory() as d:
            dest=d/'must-not-exist.json'
            with self.assertRaises(bridge.InputContractError):bridge.retain_bundle(bundle,dest,approved_directory=d,trusted_references=args[2],original_raw=args[4])
            self.assertFalse(dest.exists())


    def test_regional_native_timestamp_stops_before_collection(self):
        args=setup();m=args[0];inputs=args[3]
        fields=inputs[bridge.ENRICHMENT]['payload']['schema']
        next(f for f in fields if f['name']=='latest_mde_evidence_timestamp')['type']='timestamp'
        m['inputs'][bridge.ENRICHMENT]['schema_sha256']=bridge.digest(bridge.encoded(fields))
        class Frame:
            def __init__(self,schema):
                class Schema:
                    def json(inner):return bridge.encoded({'fields':schema}).decode()
                self.schema=Schema()
            def collect(self):raise AssertionError('No timestamp collection authorized')
        class Read:
            def format(self,*a):return self
            def option(self,*a):return self
            def table(self,name):return Frame(inputs[next(n for n in bridge.INPUTS if m['inputs'][n]['table']==name)]['payload']['schema'])
        class Spark:
            read=Read()
            class conf:
                @staticmethod
                def get(name):return 'Europe/London'
            def sql(self,query):
                name=next(n for n in bridge.INPUTS if m['inputs'][n]['table'].replace('.','`.`') in query)
                class Detail:
                    def first(inner):return {'format':'delta','id':m['inputs'][name]['table_id']}
                return Detail()
        with self.assertRaisesRegex(bridge.InputContractError,'supported UTC'):self.entry._frames(Spark(),m,bridge)


def rebind_original(args, book):
    args=list(args);args[4]=fixture.raw(book);h=bridge.digest(args[4]);args[0]['retained_sha256']=h
    for name in bridge.INPUTS:
        ref=name+'/assessment';doc=bridge.strict_json(args[1][ref]);doc['record']['retained_sha256']=h
        args[1][ref]=bridge.encoded(doc);args[2][ref]['sha256']=bridge.digest(args[1][ref])
    return args


class NavigatorCorrections(unittest.TestCase):
    def test_multidisplay_reordered_and_primary_ambiguity(self):
        for mode in ('reorder','missing','duplicate','conflicting'):
            for cell in (0,1):
                with self.subTest(mode=mode,cell=cell):
                    args=setup();book=bridge.strict_json(args[4]);outputs=book['cells'][cell]['outputs']
                    self.assertGreater(len(outputs),2)
                    if mode=='reorder':outputs.reverse()
                    elif mode=='missing':outputs.pop(1)
                    else:
                        duplicate=deepcopy(outputs[1])
                        if mode=='conflicting':duplicate['metadata'][bridge.retained.MIME]['data'][0][0]='CONTRADICTORY'
                        outputs.append(duplicate)
                    args=rebind_original(args,book)
                    if mode=='reorder':bridge.reproduce(*args)
                    else:
                        with self.assertRaisesRegex(bridge.InputContractError,'summary missing/ambiguous'):bridge.reproduce(*args)

    def test_required_context_before_pure_gate(self):
        args=setup();p=args[3][bridge.CONTROL]['payload'];cols=[f['name'] for f in p['schema']]
        p['data'][0][cols.index('applicability_state')]=None
        with self.assertRaisesRegex(bridge.InputContractError,'required MDE Control Assurance context'):bridge.reproduce(*args)
        # Keep the independently tested expression's native null semantics intact.
        row=fixture.records(fixture.fixture(),1)[0];row['ca_mde_applicability_state']=None
        self.assertEqual(bridge.gate_decision(row),'MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED')
        args=setup();args[3][bridge.CONTROL]['payload']['data'].pop(0)
        with self.assertRaisesRegex(bridge.InputContractError,'required MDE Control Assurance context'):bridge.reproduce(*args)
        args=setup();book=bridge.strict_json(args[4]);fixture.cert(book,1,'Missing MDE Control Assurance context',1)
        with self.assertRaisesRegex(bridge.InputContractError,'Certification mismatch'):bridge.reproduce(*rebind_original(args,book))

    def test_saved_bundle_external_anchor_and_joint_mutation(self):
        args=setup();bundle=bridge.reproduce(*args);original=bridge.encoded(bundle);anchor=bridge.digest(original)
        bridge.load_bundle_bytes(original,expected_sha256=anchor,trusted_references=args[2],original_raw=args[4])
        changed=bridge.strict_json(original);p=changed['inputs'][bridge.ENRICHMENT]['payload'];cols=[f['name'] for f in p['schema']]
        p['data'][0][cols.index('canonical_name')]='CHANGED_NONRESIDUAL'
        changed['qualification'][0]['canonical_name']='CHANGED_NONRESIDUAL'
        changed['input_payload_sha256'][bridge.ENRICHMENT]=bridge.digest(bridge.encoded(p))
        # Internal coherence alone is deliberately not a saved-file identity claim.
        bridge.validate_bundle(changed,args[2],args[4])
        for add_digest in (False,True):
            if add_digest:changed['sha256']=bridge.digest(bridge.encoded(changed))
            with self.assertRaisesRegex(bridge.InputContractError,'external SHA-256 mismatch'):
                bridge.load_bundle_bytes(bridge.encoded(changed),expected_sha256=anchor,trusted_references=args[2],original_raw=args[4])
        with self.assertRaisesRegex(bridge.InputContractError,'external SHA-256 mismatch'):
            bridge.load_bundle_bytes(b'NOT JSON',expected_sha256=anchor,trusted_references=args[2],original_raw=args[4])

    def test_verified_helper_loading_before_any_execution(self):
        import RCAIP_MDE_Durable_Input_Bridge as entry
        import subprocess
        for folder in (None,{},123,'dbfs:/not-a-local-folder'):
            with self.assertRaises(entry.BridgeOperationError):entry.inspect_bridge(*setup()[:3],module_folder=folder)
        with synthetic_directory() as d:
            for name in entry.EXPECTED_MODULES:
                (d/(name+'.py')).write_bytes((ROOT/'notebooks'/(name+'.py')).read_bytes())
            (d/'rcaip_mde_durable_bridge.py').write_text("raise RuntimeError('UNVERIFIED_PRIVATE_SENTINEL')\n")
            with self.assertRaisesRegex(entry.BridgeOperationError,'reviewed hash mismatch') as cm:
                entry.inspect_bridge(*setup()[:3],module_folder=d)
            self.assertNotIn('UNVERIFIED_PRIVATE_SENTINEL',str(cm.exception))
            process=subprocess.run([sys.executable,'-B','-c',"import runpy;runpy.run_path("+repr(str(ROOT/'notebooks/RCAIP_MDE_Durable_Input_Bridge.py'))+")"],cwd=d,text=True,capture_output=True)
            self.assertEqual(process.returncode,0);self.assertNotIn('UNVERIFIED_PRIVATE_SENTINEL',process.stderr)
            (d/'rcaip_cell24_retained.py').unlink()
            with self.assertRaisesRegex(entry.BridgeOperationError,'required module missing'):entry._load_helpers(d)

    def test_public_errors_fatal_and_private_values_hidden(self):
        import RCAIP_MDE_Durable_Input_Bridge as entry
        import traceback
        secret='PRIVATE_SYNTHETIC_QUERY_AND_RECORD_VALUE'
        for failure in (RuntimeError,MemoryError,RecursionError,KeyboardInterrupt,SystemExit):
            class FaultSpark:
                def sql(self,query):raise failure(secret)
            try:entry.inspect_bridge(*setup()[:3],module_folder=ROOT/'notebooks',spark=FaultSpark())
            except BaseException as exc:
                self.assertNotIn(secret,''.join(traceback.format_exception(exc)))
                self.assertIsInstance(exc,(entry.BridgeOperationError,MemoryError,KeyboardInterrupt,SystemExit))
            else:self.fail('Upstream failure became success')
        args=setup();bundle=bridge.reproduce(*args)
        with synthetic_directory() as d:
            path=d/'synthetic.ipynb';path.write_bytes(args[4])
            for operation in (
                lambda:entry.reproduce_bridge(*args[:3],module_folder=ROOT/'notebooks',spark=type('Bad',(),{'sql':lambda self,q:(_ for _ in ()).throw(RuntimeError(secret))})(),input_route='local',retained_path=path),
                lambda:entry.retain_bridge(bundle,d/'absent'/secret,approved_directory=d,reviewed_trust=args[2],module_folder=ROOT/'notebooks',input_route='local',retained_path=path),
                lambda:entry.reopen_bridge(d/secret,expected_bundle_sha256='0'*64,module_folder=ROOT/'notebooks',reviewed_trust=args[2],expected_original_sha256=bridge.digest(args[4]),input_route='local',retained_path=path)):
                with self.assertRaises(entry.BridgeOperationError) as cm:operation()
                self.assertNotIn(secret,str(cm.exception))

    def test_dbfs_single_file_pagination_hash_and_bounds(self):
        import RCAIP_MDE_Durable_Input_Bridge as entry
        import io,types
        raw=b'synthetic-only-'*(160000);requests=[];paths=[]
        class Stream(io.BytesIO):
            def read(self,n):requests.append(n);self.assert_bound(n);return super().read(n)
            @staticmethod
            def assert_bound(n):assert 0<n<=1048576
        class Dbfs:
            size=len(raw)
            def get_status(self,path):paths.append(path);return types.SimpleNamespace(is_dir=False,file_size=self.size)
            def download(self,path):paths.append(path);return Stream(raw)
        dbfs=Dbfs();client=types.SimpleNamespace(dbfs=dbfs)
        self.assertEqual(entry._download_retained(client,'dbfs:/synthetic/file.ipynb',bridge.digest(raw)),raw)
        self.assertGreater(len(requests),2);self.assertEqual(set(paths),{'/synthetic/file.ipynb'})
        for size,sha in ((len(raw)+1,bridge.digest(raw)),(len(raw),'0'*64),(bridge.MAX_BYTES+1,bridge.digest(raw))):
            dbfs.size=size
            with self.assertRaises(RuntimeError):entry._download_retained(client,'dbfs:/synthetic/file.ipynb',sha)
        with self.assertRaises(ValueError):entry._download_retained(client,'/not-dbfs','0'*64)
        with self.assertRaises(entry.BridgeOperationError):entry._retained_bytes(bridge,'0'*64,input_route='dbfs',source_uri='dbfs:/synthetic/file.ipynb',retained_path='conflict',client=client)

    def test_public_saved_file_roundtrip(self):
        import RCAIP_MDE_Durable_Input_Bridge as entry
        args=setup();bundle=bridge.reproduce(*args)
        with synthetic_directory() as d:
            original=d/'synthetic.ipynb';original.write_bytes(args[4]);dest=d/'derived.json'
            receipt=entry.retain_bridge(bundle,dest,module_folder=ROOT/'notebooks',approved_directory=d,reviewed_trust=args[2],input_route='local',retained_path=original)
            result=entry.reopen_bridge(dest,expected_bundle_sha256=receipt['sha256'],module_folder=ROOT/'notebooks',reviewed_trust=args[2],expected_original_sha256=bridge.digest(args[4]),input_route='local',retained_path=original)
            self.assertEqual(bridge.encoded(result),bridge.encoded(bundle))


if __name__=='__main__':unittest.main()
