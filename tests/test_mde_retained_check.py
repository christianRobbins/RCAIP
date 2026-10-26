"""Synthetic notebooks only. No caller, Spark, SDK service or producer execution."""
import ast
import copy
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'notebooks'))
import rcaip_mde_retained_check as check

POST, RUN = 'synthetic_post', 'synthetic_run'
PRIVATE = 'PRIVATE_SYNTHETIC_IDENTIFIER_NOT_FOR_REPORT'


def sources():
    text = (ROOT / 'notebooks/RCAIP_DIAG_001_Asset_Visibility_Funnel.py').read_text(encoding='utf-8')
    return {n: next(c for c in text.split('# COMMAND ----------') if f'# Cell {n} -' in c) for n in check.PRODUCTS}


def table(rows, columns):
    def kind(c):
        values = [r[c] for r in rows if r[c] is not None]
        value = values[0] if values else None
        if isinstance(value, list): return {'type': 'array', 'elementType': 'string', 'containsNull': False}
        if type(value) is bool: return 'boolean'
        if type(value) is int: return 'long'
        if isinstance(value, Decimal): return 'double'
        return 'string'
    return {'output_type': 'display_data', 'metadata': {check.retained.MIME: {
        'schema': [{'name': c, 'type': json.dumps(kind(c))} for c in columns],
        'data': [[r[c] for c in columns] for r in rows], 'overflow': False}}}


def fixture():
    src = sources(); cols = check.contracts(src)
    qname, gname = check.PRODUCTS[10][0], check.PRODUCTS[11][0]
    codes = ['CURRENT_MDE_EVIDENCE_CONFIRMED', 'NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION',
             'CURRENT_IDENTITY_TEMPORAL_REVIEW', 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY']
    q = []
    for i, code in enumerate(codes):
        r = dict.fromkeys(cols[qname])
        r.update(asset_key=f'{PRIVATE}-{i}', post_slug=POST, rcaip_run_id=RUN,
                 original_mde_assurance_state='MISSING', mde_qualification_cohort=code,
                 lifecycle_case_state='NONCURRENT' if i == 1 else 'CURRENTLY_OBSERVED',
                 rediscovery_visibility_class=['CURRENT_RESOLVED','HISTORICAL_REDISCOVERY','IDENTITY_OR_TEMPORAL_REVIEW_REQUIRED','STALE_REDISCOVERY'][i],
                 canonical_name='=PRIVATE_NAME\x01\\literal', canonical_serial=f'PRIVATE_SERIAL-{i}', canonical_ip='198.51.100.5',
                 enterprise_mde_name_evidence_found=True, resolved_mde_entity_count=1,
                 current_mde_entity_count=1 if i == 0 else 0, stale_mde_entity_count=1 if i == 3 else 0,
                 historical_mde_entity_count=1 if i in (1,2) else 0,
                 potential_current_control_review=i == 3, current_identity_review_required=i == 2,
                 management_owner='ENTERPRISE', operational_boundary='ENTERPRISE',
                 final_resolution_signal='REDISCOVERED_STALE_EXACT_SINGLE_ENTITY',
                 latest_mde_evidence_timestamp='0000-01-01T00:00:00',
                 latest_rediscovered_mde_machine_group='PRIVATE_TAG_NOT_LOCATION',
                 latest_rediscovered_mde_recency='STALE', durable_identity_class='SINGLE_ENTITY',
                 original_classification_preserved=True)
        for f in ('assurance_case_changed','routing_changed','priority_changed','fuzzy_matching_allowed','ip_only_matching_allowed'): r[f] = False
        for f in ('mde_qualification_meaning','mde_next_gate'):
            r[f] = check._decision_pair(src[10], qname, 'mde_qualification_cohort', f)[code]
        q.append(r)
    gates = []
    for i in (2,3):
        r = {c: q[i].get(c) for c in cols[gname]}
        for f in ('canonical_name','canonical_serial','canonical_ip','management_owner','operational_boundary'):
            r['rediscovery_'+f] = q[i][f]
        r.update(ca_device_scope='ENDPOINT', ca_management_model='ENTERPRISE_MANAGED', ca_management_owner='ENTERPRISE',
                 ca_identity_strength='STRONG', ca_attribution_conflict_flag=False, ca_operational_boundary_confidence='HIGH',
                 ca_mde_applicability_state='APPLICABLE', ca_source_mask_active='INTUNE',
                 ca_post_visibility_state='CURRENT', ca_attribution_confidence='HIGH',
                 mde_current_analytical_gate='MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED' if i == 2 else check.RESIDUAL_GATE)
        r['mde_current_gate_meaning'] = check._decision_pair(src[11],gname,'mde_current_analytical_gate','mde_current_gate_meaning')[r['mde_current_analytical_gate']]
        gates.append(r)
    cname, mname, sname, rname, aname = check.PRODUCTS[14]
    case = dict.fromkeys(cols[cname]); member = dict.fromkeys(cols[mname])
    member.update({k:v for k,v in q[3].items() if k in member})
    member.update({k:v for k,v in gates[1].items() if k in member})
    common = dict(case_id='PRIVATE_CASE', case_rule_key='MDE_STALE_SINGLE_ENTITY_NO_CURRENT_VISIBILITY',
                  case_family='MDE_CURRENT_VISIBILITY_RECONCILIATION', primary_customer='ENTERPRISE_SERVICE_OWNER',
                  relationship_outcome='STALE_SINGLE_ENTITY_MDE_HISTORY_NO_CURRENT_MDE',
                  recommended_treatment='Validate current Defender enrollment or reporting for the enterprise-managed endpoint and reconcile the remaining stale Defender identity record.',
                  residual_concern='Current applicable enterprise-managed endpoint has only stale single-entity MDE evidence and no current MDE representation.',
                  priority_basis='Current applicable enterprise endpoint with no current MDE after governed enterprise rediscovery.',
                  shared_identity_types='', shared_identity_values='', maximum_related_assets=0)
    member.update(common)
    member = {c:member[c] for c in cols[mname]}
    case.update({k:v for k,v in common.items() if k in case})
    case.update(case_class='ENTERPRISE_RECONCILIATION_REFERRAL', affected_assets=1, distinct_serials=1,
                resolved_mde_entities=1, stale_mde_entities=1, historical_mde_entities=0,
                case_member_asset_keys=[q[3]['asset_key']], ca_device_scope='ENDPOINT', ca_mde_applicability_state='APPLICABLE',
                rediscovery_recency='STALE',active_source_patterns='INTUNE',visibility_states='CURRENT',
                attribution_confidence_states='HIGH',max_assets_related_by_shared_identity=0)
    definitions = check._assignments(src[14])['compression_rows'].elts
    accounting = [dict(stage_order=i, stage=ast.literal_eval(d.elts[1]),count=[4,1,1,1,1,1,1,0,0][i-1],
                       interpretation=ast.literal_eval(d.elts[3])) for i,d in enumerate(definitions,1)]
    products = {qname:q,gname:gates,cname:[case],mname:[member],sname:[],rname:[],aname:accounting}
    certs = {n:{} for n in src}
    for n,s in src.items():
        for node in ast.parse(s).body:
            if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id=='print':
                args=node.value.args
                if len(args)>1 and isinstance(args[0],ast.Constant) and isinstance(args[0].value,str) and args[0].value.endswith(':'):
                    certs[n][args[0].value[:-1]]=0
        certs[n].update(Status='PASS',Post=POST,**{'Run ID':RUN})
    certs[10].update({'Durable MDE enrichment rows':4,'Qualified rows':4,'Qualified distinct asset keys':4,
        'Qualification cohorts observed':4,'Current MDE evidence confirmed':1,'Current identity/evidence review required':1,
        'Potential current control-review candidates':1,'Non-current lifecycle/evidence-limitation assets':1})
    certs[11].update({'Current unresolved rediscovery assets':2,'Current unresolved distinct asset keys':2,
        'Joined analytical-gate rows':2,'Joined analytical-gate distinct assets':2,
        'MDE identity resolution still required':1,'Current applicable enterprise MDE control-review candidates':1})
    certs[14].update({'Starting MDE MISSING population':4,'Current MDE evidence confirmed':1,
        'Non-current lifecycle/evidence-limitation':1,'Current identity/temporal review outside control-case set':1,
        'Owner-qualified enterprise residual candidates':1,'Residual case members formed':1,
        'Deterministic enterprise assurance cases':1,'Enterprise service-owner cases':1,'Case formation readiness':'READY',
        'Compression reconciliation':'4 / 4','Residual member reconciliation':'1 / 1'})
    book={'nbformat':4,'cells':[]}
    for n in src:
        stream=check._marker(n)+'\n'+'\n'.join(k+': '+str(v) for k,v in certs[n].items())+'\n'
        outputs=[{'output_type':'stream','name':'stdout','text':stream}]
        outputs += [table(products[p],cols[p]) for p in check.PRODUCTS[n] if p not in check.OPTIONAL]
        book['cells'].append({'cell_type':'code','source':src[n],'outputs':outputs})
    return book


def raw(book): return check.retained._json(book).encode('utf-8')


def inspect(book):
    value=raw(book)
    return check.inspect_retained_bytes(value,expected_input_sha256=hashlib.sha256(value).hexdigest(),expected_post=POST,expected_run=RUN)


def payload(book, cell, output=1): return book['cells'][cell]['outputs'][output]['metadata'][check.retained.MIME]


def set_value(book, cell, row, column, value, output=1):
    p=payload(book,cell,output); cols=[f['name'] for f in p['schema']]
    p['data'][row][cols.index(column)]=value


# Publication copy: internal review attribution omitted.
def records(book,cell,output=1):
 p=payload(book,cell,output);return [dict(zip([f['name'] for f in p['schema']],r)) for r in p['data']]

def put(book,cell,output,rows):
 cols=[f['name'] for f in payload(book,cell,output)['schema']]
 book['cells'][cell]['outputs'][output]=table(rows,cols)

def cert(book,cell,label,value):
 text=book['cells'][cell]['outputs'][0]['text']
 import re
 pattern=r'(?m)^'+re.escape(label)+r':[^\n]*$'
 assert len(re.findall(pattern,text))==1,label
 book['cells'][cell]['outputs'][0]['text']=re.sub(pattern,label+': '+str(value),text)

def shared_fixture():
 b=fixture();q=records(b,0);g=records(b,1);cases=records(b,2);m=records(b,2,2)
 token='SYNTHETIC_SHARED_HARDWARE_TOKEN'
 for i in (1,3):q[i]['hardware_uuid_values']=[token]
 put(b,0,1,q)
 a=check._assignments(sources()[14])['case_member_df']
 for field in ('case_family','relationship_outcome','recommended_treatment','residual_concern','priority_basis'):
  call=next(c for c in check._calls(a,'withColumn') if ast.literal_eval(c.args[0])==field)
  candidates=[c for c in ast.walk(call.args[1]) if isinstance(c,ast.Call) and isinstance(c.func,ast.Attribute) and c.func.attr=='when' and 'startswith' in ast.unparse(c.args[0])]
  assert len(candidates)==1,field
  expr=candidates[0].args[1]
  value='CROSS_ASSET_DURABLE_IDENTITY_REUSE:HARDWARE_UUID' if field=='relationship_outcome' else ast.literal_eval(expr.args[0])
  cases[0][field]=value
  if field in m[0]:m[0][field]=value
 for row in (cases[0],m[0]):
  row['case_rule_key']='MDE_CROSS_ASSET_IDENTITY_REUSE::HARDWARE_UUID'
  row['shared_identity_types']='HARDWARE_UUID'
 m[0]['maximum_related_assets']=2;m[0]['shared_identity_values']='HARDWARE_UUID:'+token
 cases[0]['max_assets_related_by_shared_identity']=2
 put(b,2,1,cases);put(b,2,2,m)
 support=[{'case_id':cases[0]['case_id'],'case_member_asset_key':m[0]['asset_key'],'identity_type':'HARDWARE_UUID','shared_identity_value':token,'related_asset_count':2,'related_asset_keys':[q[1]['asset_key'],q[3]['asset_key']]}]
 cols=check.contracts(sources())['case_shared_identity_support_df']
 b['cells'][2]['outputs'].append(table(support,cols))
 cert(b,2,'Shared durable identity tokens in current rediscovery population',1)
 cert(b,2,'Shared-identity supporting evidence rows touching formed cases',1)
 return b

def limitation_fixture():
 b=fixture();q=records(b,0);r=copy.deepcopy(q[2]);r['asset_key']='SYNTHETIC_LIMITATION'
 r.update(mde_qualification_cohort='CURRENT_IDENTITY_EVIDENCE_LIMITATION',rediscovery_visibility_class='NO_ENTERPRISE_NAME_HIT',enterprise_mde_name_evidence_found=False,resolved_mde_entity_count=0,current_mde_entity_count=0,stale_mde_entity_count=0,historical_mde_entity_count=0)
 for field in ('mde_qualification_meaning','mde_next_gate'):
  r[field]=check._decision_pair(sources()[10],check.PRODUCTS[10][0],'mde_qualification_cohort',field)[r['mde_qualification_cohort']]
 q.append(r);put(b,0,1,q)
 for label in ('Durable MDE enrichment rows','Qualified rows','Qualified distinct asset keys','Qualification cohorts observed'):cert(b,0,label,5)
 cert(b,0,'Current identity/evidence review required',2)
 acc=records(b,2,3);acc[0]['count']=5;acc[3]['count']=2;put(b,2,3,acc)
 cert(b,2,'Starting MDE MISSING population',5);cert(b,2,'Current identity/temporal review outside control-case set',2);cert(b,2,'Compression reconciliation','5 / 5')
 return b


class NavigatorCorrectionTests(unittest.TestCase):
    def test_reproduction_1_current_confirmation_precedence(self):
        b=fixture();set_value(b,0,1,'rediscovery_visibility_class','CURRENT_RESOLVED')
        set_value(b,0,1,'current_mde_entity_count',1)
        with self.assertRaisesRegex(check.InputContractError,'current-confirmation precedence'):inspect(b)

    def test_reproduction_2_current_entity_in_residual(self):
        b=fixture();set_value(b,0,3,'current_mde_entity_count',1);set_value(b,1,1,'current_mde_entity_count',1)
        with self.assertRaisesRegex(check.InputContractError,'Native residual evidence contradiction'):inspect(b)

    def test_reproduction_3_signal_and_case_rule(self):
        b=fixture();set_value(b,0,3,'final_resolution_signal','REDISCOVERED_HISTORICAL_EXACT_SINGLE_ENTITY')
        set_value(b,2,0,'final_resolution_signal','REDISCOVERED_HISTORICAL_EXACT_SINGLE_ENTITY',output=2)
        with self.assertRaisesRegex(check.InputContractError,'case rule/source signal'):inspect(b)

    def test_reproduction_4_unknown_limitation_visibility(self):
        b=limitation_fixture();set_value(b,0,4,'rediscovery_visibility_class','UNRECOGNIZED_SYNTHETIC_STATE')
        with self.assertRaisesRegex(check.InputContractError,'Qualification visibility'):inspect(b)

    def test_reproduction_5_unsupported_support_type(self):
        b=shared_fixture();set_value(b,2,0,'identity_type','UNSUPPORTED_SYNTHETIC_TYPE',output=4)
        with self.assertRaisesRegex(check.InputContractError,'unsupported identity type'):inspect(b)

    def test_reproduction_6_uncorroborated_support_token(self):
        b=shared_fixture();set_value(b,2,0,'shared_identity_value','NONEXISTENT_SYNTHETIC_TOKEN',output=4)
        with self.assertRaisesRegex(check.InputContractError,'not corroborated'):inspect(b)

    def test_reproduction_7_inconsistent_member_summary(self):
        b=shared_fixture();set_value(b,2,0,'shared_identity_values','HARDWARE_UUID:DIFFERENT_SYNTHETIC_TOKEN',output=2)
        with self.assertRaisesRegex(check.InputContractError,'identity-value summary'):inspect(b)

    def test_positive_shared_support_and_both_limitation_states(self):
        for b in (shared_fixture(),limitation_fixture()):
            result=inspect(b)
            self.assertEqual(result['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
            for private in ('SYNTHETIC_SHARED_HARDWARE_TOKEN',PRIVATE,'PRIVATE_CASE','PRIVATE_TAG'):
                self.assertNotIn(private,json.dumps(result))
        b=limitation_fixture();set_value(b,0,4,'rediscovery_visibility_class','INSUFFICIENT_IDENTITY_FOR_LOOKBACK')
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')

    def test_positive_multiple_types_and_source_token_normalization(self):
        b=shared_fixture();q=records(b,0);m=records(b,2,2);cases=records(b,2)
        token='SYNTHETIC_SHARED_HARDWARE_TOKEN'
        q[1]['hardware_uuid_values']=['  '+token.lower()+'  ',None,' ']
        for i in (1,3):q[i]['mde_entity_ids']=['shared_mde_token']
        put(b,0,1,q)
        suffix='HARDWARE_UUID,MDE_ENTITY_ID'
        for row in (m[0],cases[0]):
            row['shared_identity_types']=suffix
            row['case_rule_key']='MDE_CROSS_ASSET_IDENTITY_REUSE::'+suffix
            row['relationship_outcome']='CROSS_ASSET_DURABLE_IDENTITY_REUSE:'+suffix
        m[0]['shared_identity_values']='HARDWARE_UUID:'+token+' || MDE_ENTITY_ID:shared_mde_token'
        put(b,2,1,cases);put(b,2,2,m)
        support=records(b,2,4)
        support.append({**support[0],'identity_type':'MDE_ENTITY_ID','shared_identity_value':'shared_mde_token'})
        put(b,2,4,support)
        cert(b,2,'Shared durable identity tokens in current rediscovery population',2)
        cert(b,2,'Shared-identity supporting evidence rows touching formed cases',2)
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')

    def test_support_unavailable_duplicate_and_summary_maximum(self):
        b=shared_fixture();set_value(b,0,1,'hardware_uuid_values',None)
        with self.assertRaisesRegex(check.InputContractError,'not corroborated'):inspect(b)
        b=shared_fixture();set_value(b,2,0,'maximum_related_assets',3,output=2)
        set_value(b,2,0,'max_assets_related_by_shared_identity',3)
        with self.assertRaisesRegex(check.InputContractError,'related-count summary'):inspect(b)
        b=shared_fixture();support=records(b,2,4);support.append(copy.deepcopy(support[0]));put(b,2,4,support)
        with self.assertRaisesRegex(check.InputContractError,'duplicate supporting'):inspect(b)
        b=fixture();set_value(b,2,0,'shared_identity_values','invented',output=2)
        with self.assertRaisesRegex(check.InputContractError,'identity-value summary'):inspect(b)

    def test_native_full_lineage_and_historical_rule_branches(self):
        for rule,signal in [('MDE_STALE_FULL_DURABLE_LINEAGE','REDISCOVERED_STALE_FULL_DURABLE_LINEAGE'),
                            ('MDE_HISTORICAL_SINGLE_ENTITY_NO_CURRENT_VISIBILITY','REDISCOVERED_HISTORICAL_EXACT_SINGLE_ENTITY')]:
            b=fixture();q=records(b,0);g=records(b,1);m=records(b,2,2);cases=records(b,2)
            q[3]['final_resolution_signal']=m[0]['final_resolution_signal']=signal
            for row in (m[0],cases[0]):row['case_rule_key']=rule
            node=check._assignments(sources()[14])['case_member_df']
            for field in ('case_family','relationship_outcome','recommended_treatment','residual_concern','priority_basis'):
                expr=next(c.args[1] for c in check._calls(node,'withColumn') if ast.literal_eval(c.args[0])==field)
                branches=[]
                while isinstance(expr,ast.Call) and isinstance(expr.func,ast.Attribute):
                    if expr.func.attr=='when':branches.append(expr.args)
                    expr=expr.func.value
                for cond,value in reversed(branches):
                    text=ast.unparse(cond)
                    if rule in text or '!=' in text:
                        v=ast.literal_eval(value.args[0]);cases[0][field]=v;m[0][field]=v;break
            if 'HISTORICAL' in rule:
                for row in (q[3],g[1]):row['rediscovery_visibility_class']='HISTORICAL_REDISCOVERY'
                for row in (q[3],g[1],m[0]):
                    row.update(latest_rediscovered_mde_recency='HISTORICAL',stale_mde_entity_count=0,historical_mde_entity_count=1)
                cases[0].update(rediscovery_recency='HISTORICAL',stale_mde_entities=0,historical_mde_entities=1)
            else:
                for row in (q[3],g[1],m[0]):row['resolved_mde_entity_count']=2
                cases[0]['resolved_mde_entities']=2
            put(b,0,1,q);put(b,1,1,g);put(b,2,1,cases);put(b,2,2,m)
            self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')

    def test_source_null_semantics(self):
        b=fixture();set_value(b,0,3,'current_mde_entity_count',None);set_value(b,1,1,'current_mde_entity_count',None)
        set_value(b,0,0,'routing_changed',None)
        set_value(b,1,0,'ca_mde_applicability_state',None)
        set_value(b,1,1,'ca_attribution_conflict_flag',None)
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
        b=fixture();set_value(b,0,1,'lifecycle_case_state',None)
        with self.assertRaisesRegex(check.InputContractError,'Noncurrent lifecycle'):inspect(b)

    def test_cell14_predicate_not_imposed_on_all_cell11_rows(self):
        b=fixture();set_value(b,0,3,'current_mde_entity_count',1);set_value(b,1,1,'current_mde_entity_count',1)
        # Existing gate decision remains valid; this record is NOT in a supplied
        # Cell 14 residual. Adjust only synthetic saved accounting/certification.
        put(b,2,1,[]);put(b,2,2,[])
        acc=records(b,2,3)
        for row in acc:
            if row['stage_order']==4:row['count']=2
            if row['stage_order'] in (5,6,7):row['count']=0
        put(b,2,3,acc)
        cert(b,2,'Current identity/temporal review outside control-case set',2)
        for label in ('Owner-qualified enterprise residual candidates','Residual case members formed',
                      'Deterministic enterprise assurance cases','Enterprise service-owner cases'):cert(b,2,label,0)
        cert(b,2,'Residual member reconciliation','0 / 0')
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')


class RetainedCheckTests(unittest.TestCase):
    def test_complete_map_and_no_disclosure(self):
        result=inspect(fixture())
        self.assertEqual(result['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
        text=json.dumps(result)
        for secret in (PRIVATE,'PRIVATE_CASE','PRIVATE_NAME','PRIVATE_TAG','198.51.100','synthetic_run','0000-01-01'):
            self.assertNotIn(secret,text)
        self.assertNotIn('rows',result)
        self.assertEqual(result['inventory_comparison']['status'],'NOT_ESTABLISHED_BY_REVIEWED_CONTRACTS')

    def test_aggregate_only_and_residual_only(self):
        for mode in ('aggregate','residual'):
            b=fixture()
            for ci in (0,1):
                b['cells'][ci]['outputs']=b['cells'][ci]['outputs'][:1]
            if mode=='aggregate':
                b['cells'][0]['outputs'].append(table([{'assets':4,'mde_qualification_cohort':'CURRENT_MDE_EVIDENCE_CONFIRMED'}],['assets','mde_qualification_cohort']))
            result=inspect(b)
            self.assertEqual(result['state'],'CORE_ASSOCIATIONS_NOT_ESTABLISHED')
            self.assertFalse(result['products'][0]['candidate_structure_found'])
            self.assertIn('asset_key',result['products'][0]['missing_required_fields'])

    def test_missing_empty_and_ambiguous_cells(self):
        self.assertEqual(inspect({'nbformat':4,'cells':[]})['state'],'CORE_ASSOCIATIONS_NOT_ESTABLISHED')
        b=fixture(); b['cells'].append(copy.deepcopy(b['cells'][0]))
        with self.assertRaisesRegex(check.InputContractError,'Ambiguous'): inspect(b)

    def test_membership_and_alias_rejections(self):
        scenarios=[(0,1,'asset_key',PRIVATE+'-0','duplicate'),(0,0,'rcaip_run_id','foreign','scope'),
                   (1,0,'asset_key','foreign','membership'),(1,0,'rediscovery_canonical_name','changed','inherited'),
                   (1,0,'latest_mde_evidence_timestamp','0001-01-01','inherited'),
                   (2,0,'asset_key','foreign','membership')]
        for cell,row,col,value,reason in scenarios:
            with self.subTest(col=col,cell=cell):
                b=fixture();set_value(b,cell,row,col,value,output=2 if cell==2 else 1)
                with self.assertRaisesRegex(check.InputContractError,reason): inspect(b)
        b=fixture();payload(b,0)['data'].pop()
        with self.assertRaisesRegex(check.InputContractError,'Certification mismatch'): inspect(b)

    def test_decisions_scope_certification_source(self):
        for cell,col,value,reason in [(0,'mde_next_gate','bogus','Contradictory'),(0,'original_classification_preserved',False,'preserved'),
                                     (1,'mde_current_analytical_gate','bogus','Unsupported'),(2,'primary_customer','POST_DT','Case/member')]:
            b=fixture();set_value(b,cell,0,col,value)
            with self.assertRaisesRegex(check.InputContractError,reason):inspect(b)
        for change,reason in [('Status: PASS','certification'),('Run ID: '+RUN,'scope')]:
            b=fixture();b['cells'][0]['outputs'][0]['text']=b['cells'][0]['outputs'][0]['text'].replace(change,change+ '_CHANGED')
            with self.assertRaisesRegex(check.InputContractError,reason):inspect(b)
        b=fixture();b['cells'][0]['source']+='\nsynthetic_changed_executable = True\n'
        with self.assertRaisesRegex(check.InputContractError,'executable source'):inspect(b)

    def test_overflow_schema_duplicate_and_missing(self):
        for overflow in (True,None):
            b=fixture();payload(b,0)['overflow']=overflow
            with self.assertRaisesRegex(check.InputContractError,'overflow'):inspect(b)
        b=fixture();p=payload(b,0);p['schema'].append(p['schema'][0])
        with self.assertRaisesRegex(check.InputContractError,'Duplicate structured'):inspect(b)
        b=fixture();b['cells'][0]['outputs'].append(copy.deepcopy(b['cells'][0]['outputs'][1]))
        with self.assertRaisesRegex(check.InputContractError,'ambiguous candidate'):inspect(b)
        b=fixture();payload(b,0)['schema'][0]['type']='{"type":"array","type":"array","elementType":"string"}'
        with self.assertRaisesRegex(check.InputContractError,'Duplicate JSON'):inspect(b)

    def test_parser_compatibility_and_exact_evidence(self):
        b=fixture()
        # Extra evidence values are kept exact internally and never returned.
        for ci in (0,1):
            p=payload(b,ci)
            p['schema'].append({'name':'synthetic_numeric_evidence','type':'"double"'})
            for row in p['data']:row.append(Decimal('-0.0'))
        value=raw(b).replace(b'"output_type":"stream"',b'"output_type":"stream","output_type":"stream"')
        result=check.inspect_retained_bytes(value,expected_input_sha256=hashlib.sha256(value).hexdigest(),expected_post=POST,expected_run=RUN)
        self.assertEqual(result['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
        self.assertIn(b'-0.0',value)
        payload(b,1)['data'][0][-1]=Decimal('0.0')
        with self.assertRaisesRegex(check.InputContractError,'inherited'):inspect(b)
        b=fixture();value=raw(b).replace(b'"overflow":false',b'"overflow":false,"overflow":false',1)
        with self.assertRaisesRegex(check.InputContractError,'Duplicate JSON'):
            check.inspect_retained_bytes(value,expected_input_sha256=hashlib.sha256(value).hexdigest(),expected_post=POST,expected_run=RUN)

    def test_hash_and_size(self):
        b=raw(fixture())
        with self.assertRaisesRegex(check.InputContractError,'hash mismatch'):
            check.inspect_retained_bytes(b,expected_input_sha256='0'*64,expected_post=POST,expected_run=RUN)
        for value in (b'',b' '*(check.MAX_BYTES+1)):
            with self.assertRaisesRegex(check.InputContractError,'byte bound'):
                check.inspect_retained_bytes(value,expected_input_sha256='0'*64,expected_post=POST,expected_run=RUN)

    def test_optional_zero_and_nonzero_absence(self):
        b=fixture();result=inspect(b)
        self.assertEqual([p['status'] for p in result['products'] if p['logical_product'] in check.OPTIONAL],
                         ['CERTIFIED_ZERO_NOT_DISPLAYED']*2)
        b['cells'][2]['outputs'][0]['text']=b['cells'][2]['outputs'][0]['text'].replace(
            'Shared-identity supporting evidence rows touching formed cases: 0',
            'Shared-identity supporting evidence rows touching formed cases: 1')
        self.assertEqual(inspect(b)['state'],'CORE_ASSOCIATIONS_NOT_ESTABLISHED')

    def test_wrong_decision_combination_and_accounting(self):
        b=fixture();code='MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED'
        set_value(b,1,1,'mde_current_analytical_gate',code)
        set_value(b,1,1,'mde_current_gate_meaning',check._decision_pair(sources()[11],check.PRODUCTS[11][0],
                  'mde_current_analytical_gate','mde_current_gate_meaning')[code])
        with self.assertRaisesRegex(check.InputContractError,'identity/temporal contradiction'):inspect(b)
        b=fixture();set_value(b,2,0,'stage','UNKNOWN_STAGE',output=3)
        with self.assertRaisesRegex(check.InputContractError,'stage meaning'):inspect(b)
        b=fixture();set_value(b,2,0,'recommended_treatment','invented treatment')
        with self.assertRaisesRegex(check.InputContractError,'Native case decision'):inspect(b)

    def test_reordered_columns_and_alternate_post(self):
        b=fixture()
        for c in b['cells']:
            for o in c['outputs'][1:]:
                p=o['metadata'][check.retained.MIME]
                p['schema'].reverse()
                for r in p['data']:r.reverse()
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
        value=raw(fixture()).replace(POST.encode(),b'paris_synthetic').replace(RUN.encode(),b'paris_synthetic_run')
        result=check.inspect_retained_bytes(value,expected_input_sha256=hashlib.sha256(value).hexdigest(),
                expected_post='paris_synthetic',expected_run='paris_synthetic_run')
        self.assertEqual(result['state'],'RETAINED_ASSOCIATIONS_VALIDATED')

    def test_exact_precision_and_private_error_safety(self):
        b=fixture()
        for ci in (0,1):
            p=payload(b,ci);p['schema'].append({'name':'synthetic_precision','type':'"double"'})
            for r in p['data']:r.append(Decimal('0.12345678901234566'))
        self.assertEqual(inspect(b)['state'],'RETAINED_ASSOCIATIONS_VALIDATED')
        payload(b,1)['data'][0][-1]=Decimal('0.1234567890123457')
        with self.assertRaisesRegex(check.InputContractError,'inherited') as caught:inspect(b)
        self.assertNotIn(PRIVATE,str(caught.exception))
        value=b'{"nbformat":4,"PRIVATE_SECRET":'
        with self.assertRaisesRegex(check.InputContractError,'Invalid retained JSON') as caught:
            check.inspect_retained_bytes(value,expected_input_sha256=hashlib.sha256(value).hexdigest(),expected_post=POST,expected_run=RUN)
        self.assertNotIn('PRIVATE_SECRET',str(caught.exception))


def extracted_transport():
    path=ROOT/'notebooks/RCAIP_MDE_Retained_Input_Check.py'
    tree=ast.parse(path.read_text())
    funcs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_download_retained']
    namespace={'hashlib':hashlib,'re':__import__('re')}
    exec(compile(ast.Module(body=funcs,type_ignores=[]),str(path),'exec'),namespace)
    return namespace['_download_retained']


class TransportTests(unittest.TestCase):
    def run_transport(self, data, size=None, digest=None, is_dir=False):
        class Stream(io.BytesIO):
            requests=[]
            def read(self,n=-1):
                self.requests.append(n)
                if not 0<n<=1024*1024:raise AssertionError('Oversized request')
                return super().read(n)
        stream=Stream(data)
        dbfs=types.SimpleNamespace(get_status=lambda p:types.SimpleNamespace(file_size=len(data) if size is None else size,is_dir=is_dir),download=lambda p:stream)
        result=extracted_transport()(types.SimpleNamespace(dbfs=dbfs),'dbfs:/synthetic.ipynb',digest or hashlib.sha256(data).hexdigest())
        return result,stream.requests

    def test_pagination_exact_bytes(self):
        data=b'SYNTHETIC_ONLY\x00'*(180000)
        result,requests=self.run_transport(data)
        self.assertEqual(data,result);self.assertGreater(len(requests),2)

    def test_transport_rejects_wrong_length_hash_bounds(self):
        for kw in ({'size':20},{'digest':'0'*64},{'size':64*1024*1024+1},{'is_dir':True}):
            with self.subTest(kw=kw),self.assertRaises(RuntimeError):self.run_transport(b'synthetic',**kw)

    def test_no_automatic_caller_and_pinned_modules(self):
        tree=ast.parse((ROOT/'notebooks/RCAIP_MDE_Retained_Input_Check.py').read_text())
        self.assertFalse(any(isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) for n in tree.body))
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='EXPECTED_MODULES')
        for name,wanted in ast.literal_eval(assignment.value).items():
            data=(ROOT/'notebooks'/f'{name}.py').read_bytes().replace(b'\r\n',b'\n')
            self.assertEqual(hashlib.sha256(data).hexdigest(),wanted)


if __name__=='__main__': unittest.main(verbosity=2)
