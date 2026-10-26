"""Synthetic expression/DataFrame adapters, NOT real Spark or Department data."""
from copy import deepcopy
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import test_intune_distributed as dist
import test_intune_validation as old
import ip001_synthetic_frame as sf

spec = importlib.util.spec_from_file_location('profile_consumer', old.ROOT/'notebooks/rcaip_intune_evidence_profile.py')
p = importlib.util.module_from_spec(spec); spec.loader.exec_module(p)
SOURCE = (old.ROOT/'notebooks/rcaip_intune_validation.py').read_text(encoding='utf-8')


def row(key='PRIVATE-A', contact='2026-09-20', raw='true', state=p.STATES[0], age='Current', **kw):
    return dict(asset_key=key, intune_last_contact=contact, _validation_time_raw=contact,
                _intune_encryption_state_raw=raw, intune_encryption_state=state,
                observation_age_bucket=age, intune_device_name='PRIVATE-NAME',
                intune_serial_number='PRIVATE-SERIAL', intune_os_version='PRIVATE-OS', **kw)


def fixture():
    rows = [row(), row(raw='false', state=p.STATES[1]), row(raw=None, state=p.STATES[3]),
            row(key='PRIVATE-B', contact='2026-09-01', age='Recent'),
            row(key='PRIVATE-B', contact='2026-09-01', age='Recent', raw='TRUE'),
            row(key='PRIVATE-B', contact='2026-09-21'),
            row(key='PRIVATE-C', contact=None, age='Unknown'),
            row(key='PRIVATE-C', contact=None, age='Unknown', raw=None, state=p.STATES[3]),
            row(key=None, contact=None, age='Unknown', raw='odd', state=p.STATES[2]),
            row(key='PRIVATE-STALE', contact='2026-01-01', age='Stale'),
            row(key='PRIVATE-FUTURE', contact='2099-01-01', age='Future Date Review')]
    rows[1]['intune_device_name']='PRIVATE-NAME-2'
    rows[2]['intune_serial_number']=None
    rows[4]['intune_os_version']='PRIVATE-OS-2'
    rows[6]['_validation_time_raw']='malformed'
    rows[7]['_validation_time_raw']='  '
    return rows


@contextmanager
def adapter(spark, **kw):
    with dist.distributed_adapter(spark, **kw) as f:
        with patch.object(sf.D, 'schema', property(lambda frame: frame.__dict__['schema'])), \
             patch.object(f, 'min', lambda c:sf.Agg(lambda rs:min(
                (sf.col(c).v(r) for r in rs if sf.col(c).v(r) is not None), default=None)), create=True):
            yield f


def context(rows, post='SYNTHETIC'):
    parent=old.plan()
    parent.private['runtime']['RCAIP_POST_SLUG']=post
    parent.plan_id=old.v.digest(parent.private)
    plan=old.v.prepare_distributed(parent,engine_source=old.ENGINE,config_source=old.CONFIG)
    columns=list(row())
    frame=sf.D(rows,columns)
    frame.schema=SimpleNamespace(jsonValue=lambda:{'type':'struct','fields':[
        {'name':n,'type':'timestamp' if n=='intune_last_contact' else 'string','nullable':True,'metadata':{}}
        for n in columns]})
    spark=old.Spark()
    frame.sparkSession=spark
    with adapter(spark) as f:
        summary=old.v.complete_summary(old.v.distributed_summary_query(frame,f).limit(2).collect()[0].asDict())
    spark.actions=0
    result=old.v.DistributedResult(frame,plan,summary)
    attempt=old.v.DistributedAttempt()
    attempt.started=True;attempt.stage='COMPLETE';attempt.private_relation=frame;attempt.private_result=result
    return {p.DP+'MODULE':old.v,p.DP+'PLAN':plan,p.DP+'RESULT':result,p.DP+'ATTEMPT':attempt,
            p.DP+'APPROVAL':dict(old.consent(plan),authorization_reference='SYNTHETIC-ACQUISITION'),
            'RCAIP_INTUNE_PLAN':parent,'RCAIP_INTUNE_ENGINE_SOURCE':old.ENGINE,
            'RCAIP_INTUNE_CONFIG_SOURCE':old.CONFIG,
            'RCAIP_INTUNE_VALUE_ATTEMPT':{'execute_dispatched':True,'authorization_reference':'SYNTHETIC-OLD'}},spark


def execute(ns, spark, **overrides):
    p.run(ns,validator_source=SOURCE)
    options=dict(validator_source=SOURCE,mode='PROFILE',exact_plan_id=ns[p.PREFIX+'PLAN'].plan_id,
                 allow_retained_values=True,allow_possible_recomputation=True,
                 authorization_reference='SYNTHETIC-PROFILE')
    options.update(overrides)
    with adapter(spark):return p.run(ns,**options)


class ProfileTests(unittest.TestCase):
    def test_complete_profile_and_unchanged_original(self):
        rows=fixture();ns,s=context(rows);original=dict(ns);snapshot=deepcopy(ns[p.DP+'RESULT'].status())
        out=execute(ns,s)
        self.assertEqual(out['status'],'COMPLETE',ns.get(p.PREFIX+'EXCEPTION'))
        self.assertEqual(s.actions,1)
        self.assertEqual(sum(x['observations'] for x in out['freshness_encryption']),11)
        self.assertEqual(out['baseline']['distinct_nonnull_asset_keys'],5)
        self.assertEqual(out['baseline']['missing_identity_rows'],1)
        parts={(x['kind'],x['time_basis']):x['groups'] for x in out['variant_groups']}
        self.assertEqual(sum(parts.values()),3)
        self.assertEqual(parts[(p.KINDS[0],p.TIMES[1])],1)
        self.assertEqual(parts[(p.KINDS[1],p.TIMES[0])],1)
        self.assertEqual(parts[(p.KINDS[2],p.TIMES[2])],1)
        result=ns[p.PREFIX+'RESULT'];keys={r['asset_key']:r for r in result.private_keys.rows}
        self.assertEqual(len(keys),5)
        self.assertEqual(keys['PRIVATE-A']['latest_known_tied_rows'],3)
        self.assertEqual(keys['PRIVATE-A']['latest_state_3'],1)
        self.assertEqual(keys['PRIVATE-A']['intune_device_name_representations_including_null'],2)
        self.assertEqual(keys['PRIVATE-A']['intune_serial_number_representations_including_null'],2)
        self.assertEqual(keys['PRIVATE-B']['intune_os_version_representations_including_null'],2)
        self.assertEqual(keys['PRIVATE-C']['latest_known_tied_rows'],0)
        self.assertEqual(keys['PRIVATE-C']['unknown_time_rows'],2)
        self.assertIsNone(keys['PRIVATE-C']['earliest_known_contact'])
        self.assertEqual(result.private_observations.rows,rows)
        self.assertEqual(len(result.private_missing_identity.rows),1)
        for k,v in original.items():self.assertIs(ns[k],v)
        self.assertEqual(snapshot,ns[p.DP+'RESULT'].status())
        self.assertNotIn('PRIVATE-',json.dumps(out))
        out['baseline']['scoped_rows']=0
        self.assertEqual(result.status()['baseline']['scoped_rows'],11)

    def test_plan_only_no_actions_and_private_provenance(self):
        ns,s=context(fixture());out=p.run(ns,validator_source=SOURCE)
        self.assertEqual(out['phase'],'PLAN_ONLY');self.assertEqual(s.actions,0)
        plan=ns[p.PREFIX+'PLAN']
        self.assertEqual(plan.private['acquisition_plan'],ns[p.DP+'PLAN'].private)
        self.assertEqual(p.run(ns,validator_source=SOURCE),out)
        self.assertIs(ns[p.PREFIX+'PLAN'],plan)

    def test_empty_and_missing_identity_only(self):
        for rows in ([],[row(key=None),row(key=None,contact=None,age='Unknown')]):
            ns,s=context(rows);out=execute(ns,s)
            self.assertEqual(out['status'],'COMPLETE',ns.get(p.PREFIX+'EXCEPTION'))
            self.assertEqual(out['baseline']['distinct_nonnull_asset_keys'],0)
            self.assertEqual(out['baseline']['missing_identity_rows'],len(rows))
            self.assertEqual(ns[p.PREFIX+'RESULT'].private_keys.rows,[])

    def test_supplied_age_boundaries_and_varied_posts(self):
        ages=['Current','Current','Recent','Recent','Stale','Future Date Review','Unknown']
        for post in ('SYNTHETIC-ONE','SYNTHETIC-TWO'):
            rows=[row(age=a,contact=None if a=='Unknown' else str(2020+i)) for i,a in enumerate(ages)]
            # These are supplied Engine categories for ages 0/30/31/90/91/future/unknown;
            # the consumer must not recalculate age from the arbitrary synthetic times.
            ns,s=context(rows,post);out=execute(ns,s)
            self.assertEqual(out['status'],'COMPLETE')
            totals={a:sum(x['observations'] for x in out['freshness_encryption'] if x['age']==a) for a in p.AGES}
            self.assertEqual(totals,dict(zip(p.AGES,[2,2,1,1,1])))

    def test_identical_repeats_not_variants(self):
        ns,s=context([row(),row()]);out=execute(ns,s)
        self.assertEqual(out['baseline']['equal_time_groups'],1)
        self.assertEqual(sum(x['groups'] for x in out['variant_groups']),0)
        self.assertEqual(ns[p.PREFIX+'RESULT'].private_keys.rows[0]['latest_known_tied_rows'],2)

    def test_unexpected_categories_refused(self):
        ns,s=context([row()]);ns[p.DP+'RESULT'].private_relation.rows[0]['observation_age_bucket']='SECRET-UNKNOWN'
        out=execute(ns,s);self.assertEqual(out['error_code'],'UNEXPECTED_AGE_CATEGORY')
        self.assertNotIn('SECRET',json.dumps(out));self.assertNotIn(p.PREFIX+'RESULT',ns)
        ns,s=context([row()]);ns[p.DP+'RESULT'].private_relation.rows[0]['intune_encryption_state']='SECRET-STATE'
        self.assertEqual(execute(ns,s)['status'],'STOP')

    def test_missing_and_changed_context(self):
        for key in (p.DP+'RESULT',p.DP+'ATTEMPT','RCAIP_INTUNE_ENGINE_SOURCE'):
            ns,s=context([row()]);del ns[key]
            self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'RETAINED_CONTEXT_REQUIRED_NO_RELOAD')
            self.assertEqual(s.actions,0)
        ns,s=context([row()]);p.run(ns,validator_source=SOURCE)
        ns[p.DP+'RESULT']._status['scoped_rows']+=1
        self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'PROFILE_CONTEXT_CHANGED')
        ns,s=context([row()]);ns[p.DP+'ATTEMPT'].stage='FAILED'
        self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'COMPLETED_RESULT_REQUIRED')

    def test_hash_schema_and_plan_refusals(self):
        ns,s=context([row()])
        self.assertEqual(p.run(ns,validator_source=SOURCE+'# changed')['error_code'],'VALIDATOR_SOURCE_MISMATCH')
        ns[p.DP+'RESULT'].private_relation.schema=SimpleNamespace(jsonValue=lambda:{'fields':[]})
        self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'RETAINED_SCHEMA_REQUIRED')
        ns,s=context([row()]);ns[p.DP+'RESULT'].private_plan['parent_plan']['version']=1
        self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'RESULT_PLAN_CHANGED')

    def test_approval_and_repeat_guards(self):
        for options in ({'allow_retained_values':False},{'allow_possible_recomputation':False},
                        {'exact_plan_id':'wrong'},{'authorization_reference':'SYNTHETIC-ACQUISITION'},
                        {'authorization_reference':'SYNTHETIC-OLD'}):
            ns,s=context([row()]);self.assertEqual(execute(ns,s,**options)['status'],'STOP')
            self.assertEqual(s.actions,0)
            self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'PROFILE_ATTEMPT_ALREADY_RETAINED')
        ns,s=context([row()]);self.assertEqual(execute(ns,s)['status'],'COMPLETE')
        self.assertEqual(p.run(ns,validator_source=SOURCE,mode='PROFILE')['status'],'STOP')
        self.assertEqual(s.actions,1)

    def test_bounded_summary_and_private_failure(self):
        ns,s=context([row()]);p.run(ns,validator_source=SOURCE)
        with adapter(s),patch.object(sf.D,'collect',return_value=[]):
            out=p.run(ns,validator_source=SOURCE,mode='PROFILE',exact_plan_id=ns[p.PREFIX+'PLAN'].plan_id,
                      allow_retained_values=True,allow_possible_recomputation=True,authorization_reference='NEW')
        self.assertEqual(out['error_code'],'PROFILE_SUMMARY_CARDINALITY')
        self.assertNotIn(p.PREFIX+'RESULT',ns)
        ns,s=context([row()]);p.run(ns,validator_source=SOURCE)
        with adapter(s),patch.object(sf.D,'collect',side_effect=RuntimeError('PRIVATE-DETAIL')):
            out=p.run(ns,validator_source=SOURCE,mode='PROFILE',exact_plan_id=ns[p.PREFIX+'PLAN'].plan_id,
                      allow_retained_values=True,allow_possible_recomputation=True,authorization_reference='NEW')
        self.assertNotIn('PRIVATE-DETAIL',json.dumps(out))
        self.assertIn(p.PREFIX+'EXCEPTION',ns)

    def test_baseline_mismatch_no_complete_profile(self):
        ns,s=context([row()]);ns[p.DP+'RESULT'].private_relation.rows.append(row())
        self.assertEqual(execute(ns,s)['error_code'],'RETAINED_SUMMARY_CHANGED')
        self.assertNotIn(p.PREFIX+'RESULT',ns)

    def test_original_variant_partition_latest_and_historical(self):
        rows=[row(contact='2020-01-01'),row(contact='2020-01-01',raw='false',state=p.STATES[1]),
              row(contact='2021-01-01'),row(contact='2021-01-01',raw='TRUE'),
              row(contact=None,age='Unknown'),row(contact=None,age='Unknown',raw='TRUE')]
        ns,s=context(rows);out=execute(ns,s)
        self.assertEqual(out['status'],'COMPLETE')
        parts={(x['kind'],x['time_basis']):x['groups'] for x in out['variant_groups']}
        self.assertEqual(parts[(p.KINDS[0],p.TIMES[2])],1)
        self.assertEqual(parts[(p.KINDS[2],p.TIMES[1])],1)
        self.assertEqual(parts[(p.KINDS[2],p.TIMES[0])],1)
        key=ns[p.PREFIX+'RESULT'].private_keys.rows[0]
        self.assertEqual(sum(key['age_state_'+str(i)] for i in range(20)),6)
        self.assertEqual(key['latest_known_tied_rows'],2)
        self.assertEqual(key['latest_state_1'],0)

    def test_summary_shape_partition_and_count_guards(self):
        ns,s=context(fixture());plan=p.prepare(ns,validator_source=SOURCE)
        with adapter(s) as f:
            _,_,_,query,base=p.queries(plan.relation,f,plan.validator,plan.private['variation_fields'])
            valid=query.limit(2).collect()[0].asDict()
        for name,value in [('unexpected_age',1),('profile_keys',999),('profile_rows',999),
                           ('matrix_keys_0',999),('matrix_rows_0',999),('variant_0',999)]:
            altered=dict(valid);altered[name]=value
            with self.assertRaises(p.ProfileError):p.complete(altered,base,plan)
        altered=dict(valid);altered['new_unbounded_field']=1
        with self.assertRaisesRegex(p.ProfileError,'SHAPE'):p.complete(altered,base,plan)

    def test_source_has_no_acquisition_or_unbounded_detail_collection(self):
        import ast
        tree=ast.parse((old.ROOT/'notebooks/rcaip_intune_evidence_profile.py').read_text(encoding='utf-8'))
        forbidden={'table','sql','toPandas','toLocalIterator','collect_set','collect_list',
                   'dropDuplicates','row_number','first','persist','unpersist','save','saveAsTable'}
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)]
        self.assertFalse(forbidden & {n.func.attr for n in calls})
        collections=[n for n in calls if n.func.attr=='collect']
        self.assertEqual(len(collections),1)
        self.assertEqual(ast.unparse(collections[0]),'query.limit(2).collect()')

    def test_missing_optional_representations_and_existing_profile_not_overwritten(self):
        ns,s=context([row()]);frame=ns[p.DP+'RESULT'].private_relation
        schema=frame.schema.jsonValue();schema['fields']=[f for f in schema['fields'] if f['name'] not in p.VARIATIONS]
        frame.schema=SimpleNamespace(jsonValue=lambda:schema)
        out=execute(ns,s);self.assertEqual(out['status'],'COMPLETE')
        self.assertEqual(ns[p.PREFIX+'RESULT'].private_plan['variation_fields'],[])
        ns,s=context([row()]);sentinel=object();ns[p.PREFIX+'RESULT']=sentinel
        self.assertEqual(p.run(ns,validator_source=SOURCE)['error_code'],'PROFILE_RESULT_ALREADY_RETAINED')
        self.assertIs(ns[p.PREFIX+'RESULT'],sentinel)


class OperatingSystemTests(unittest.TestCase):
    def with_os(self, values, field_type='string'):
        rows=[row(intune_operating_system=value) for value in values]
        ns,s=context(rows)
        frame=ns[p.DP+'RESULT'].private_relation
        schema=deepcopy(frame.schema.jsonValue())
        schema['fields'].append({'name':'intune_operating_system','type':field_type,
                                 'nullable':True,'metadata':{}})
        frame.schema=SimpleNamespace(jsonValue=lambda:deepcopy(schema))
        frame.columns.append('intune_operating_system')
        return ns,s,rows

    def test_different_os_same_version_private_only(self):
        ns,s,rows=self.with_os(['SYNTHETIC-Windows','SYNTHETIC-Android'])
        original=ns[p.DP+'RESULT'];before=deepcopy(original.status())
        out=execute(ns,s)
        self.assertEqual(out['status'],'COMPLETE')
        result=ns[p.PREFIX+'RESULT'];key=result.private_keys.rows[0]
        self.assertEqual(key['intune_operating_system_representations_including_null'],2)
        self.assertEqual(key['intune_os_version_representations_including_null'],1)
        self.assertIn('intune_operating_system',result.private_plan['variation_fields'])
        self.assertEqual(key['observations'],2)
        self.assertEqual(key['latest_known_tied_rows'],2)
        self.assertIs(result.private_observations,original.private_relation)
        self.assertEqual(original.private_relation.rows,rows)
        self.assertEqual(original.status(),before)
        self.assertNotIn('SYNTHETIC-Windows',json.dumps(out))
        self.assertNotIn('SYNTHETIC-Android',json.dumps(out))
        control,cs=context([row(),row()])
        self.assertEqual(out,execute(control,cs))

    def test_os_populated_null_and_all_null(self):
        for values,expected in [(['SYNTHETIC-OS',None],2),([None,None],1)]:
            with self.subTest(values=values):
                ns,s,rows=self.with_os(values)
                self.assertEqual(execute(ns,s)['status'],'COMPLETE')
                key=ns[p.PREFIX+'RESULT'].private_keys.rows[0]
                self.assertEqual(key['intune_operating_system_representations_including_null'],expected)
                self.assertEqual(key['intune_os_version_representations_including_null'],1)
                self.assertEqual(ns[p.DP+'RESULT'].private_relation.rows,rows)

    def test_os_absent_remains_unestablished(self):
        ns,s=context([row()]);out=execute(ns,s)
        self.assertEqual(out['status'],'COMPLETE')
        result=ns[p.PREFIX+'RESULT']
        self.assertNotIn('intune_operating_system',result.private_plan['variation_fields'])
        self.assertNotIn('intune_operating_system_representations_including_null',result.private_keys.columns)
        self.assertNotIn('intune_operating_system',result.private_observations.rows[0])
        self.assertEqual(result.private_keys.rows[0]['intune_os_version_representations_including_null'],1)

    def test_os_incompatible_type_refused_before_action(self):
        ns,s,rows=self.with_os([7],field_type='long')
        out=p.run(ns,validator_source=SOURCE)
        self.assertEqual(out['error_code'],'VARIATION_SCHEMA_TYPES')
        self.assertEqual(s.actions,0)
        self.assertNotIn(p.PREFIX+'PLAN',ns)
        self.assertNotIn(p.PREFIX+'RESULT',ns)
        self.assertEqual(ns[p.DP+'RESULT'].private_relation.rows,rows)


if __name__=='__main__':unittest.main(verbosity=2)
