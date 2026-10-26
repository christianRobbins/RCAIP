"""Focused distributed-mode tests using synthetic Spark adapters, NOT Spark."""
from contextlib import contextmanager
from copy import deepcopy
from collections import defaultdict
import json
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch
import test_intune_validation as old
import ip001_synthetic_frame as sf

v = old.v
spec = importlib.util.spec_from_file_location('distributed_operator',old.ROOT/'notebooks/RCAIP_Intune_Distributed_Validation.py')
operator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operator)
SOURCE = (old.ROOT/'notebooks/rcaip_intune_validation.py').read_text(encoding='utf-8')


@contextmanager
def distributed_adapter(spark, fail_collect=False):
    with old.synthetic_spark(spark):
        f = sys.modules['pyspark.sql'].functions
        sys.modules['pyspark'].StorageLevel = SimpleNamespace(MEMORY_AND_DISK='synthetic-memory-disk')
        original_coalesce = f.coalesce
        f.sum = lambda c:sf.Agg(lambda rows:sum(sf.col(c).v(r) for r in rows if sf.col(c).v(r) is not None) if rows else None)
        f.count = lambda c:sf.Agg(lambda rows:sum(sf.col(c).v(r) is not None for r in rows))
        f.countDistinct = lambda c:sf.Agg(lambda rows:len({json.dumps(sf.col(c).v(r),sort_keys=True,default=str) for r in rows if sf.col(c).v(r) is not None}))
        f.coalesce = lambda *cs:sf.Agg(lambda rows:next((c.v(rows) for c in cs if c.v(rows) is not None),None)) if any(isinstance(c,sf.Agg) for c in cs) else original_coalesce(*cs)
        def agg(frame, *expressions):
            return sf.D([{e.name:e.v(frame.rows) for e in expressions}])
        def grouped(frame,*keys):
            groups=defaultdict(list)
            for r in frame.rows:groups[tuple(r[k] for k in keys)].append(r)
            return SimpleNamespace(agg=lambda *es:sf.D([
                dict(zip(keys,k),**{e.name:e.v(rs) for e in es}) for k,rs in groups.items()],list(keys)+[e.name for e in es]))
        def collect(frame):
            if fail_collect:raise RuntimeError('SYNTHETIC failed aggregate')
            if frame.rows and 'scoped_rows' not in frame.rows[0]:
                raise AssertionError('detail collected into driver')
            if len(frame.rows)>2:raise AssertionError('unbounded summary')
            spark.actions+=1
            return [SimpleNamespace(asDict=lambda recursive=True,r=r:deepcopy(r)) for r in frame.rows]
        def persist(frame, level):
            spark.persisted=frame
            return frame
        with patch.object(sf.D,'agg',agg,create=True), \
             patch.object(sf.D,'groupBy',grouped,create=True), \
             patch.object(sf.D,'crossJoin',lambda a,b:sf.D([dict(x,**y) for x in a.rows for y in b.rows]),create=True), \
             patch.object(sf.D,'persist',persist,create=True), \
             patch.object(sf.D,'collect',collect):
            yield f


def new_plan():
    return v.prepare_distributed(old.plan(),engine_source=old.ENGINE,config_source=old.CONFIG)


def summary_row(i=0):
    return dict(asset_key='SYN-'+str(i%7), intune_last_contact=None if i%3==0 else '2026-10-15',
                _validation_time_raw='bad' if i%3==0 else '2026-10-15',
                _intune_encryption_state_raw=('true','false','weird',None)[i%4],
                intune_encryption_state=v.ENCRYPTION[i%4], observation_age_bucket='Unknown' if i%3==0 else 'Current')


class Distributed(unittest.TestCase):
    def test_more_than_both_driver_capacities(self):
        for count in (10001,100001):
            rows=[summary_row(i) for i in range(count)]
            frame=sf.D(rows);spark=old.Spark()
            with distributed_adapter(spark) as f:
                query=v.distributed_summary_query(frame,f)
                result=v.complete_summary(query.limit(2).collect()[0].asDict())
            expected=v.summarize(rows)
            for k,value in expected.items():self.assertEqual(result[k],value,k)
            self.assertEqual(result['scoped_rows'],count)
            self.assertEqual(sum(result['encryption_categories'].values()),count)
            self.assertEqual(result['distinct_nonnull_asset_keys'],7)
            self.assertEqual(len(frame.rows),count)
            self.assertEqual(spark.actions,1)
            self.assertNotIn('SYN-',json.dumps(result))

    def test_execute_retains_all_typed_rows_and_null_conflicts(self):
        rows=[old.row(encryption=' True '),old.row(encryption='false'),
              old.row('SYN-B',time='bad',encryption='unknown'),
              old.row('SYN-B',time=None,encryption=None),old.row('OTHER')]
        p=new_plan();attempt=v.DistributedAttempt();s=old.Spark(rows)
        with distributed_adapter(s):
            result=v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=s,
                                         engine_source=old.ENGINE,config_source=old.CONFIG)
        expected=v.summarize(s.persisted.rows)
        for k,value in expected.items():self.assertEqual(result.status()[k],value,k)
        self.assertEqual(result.status()['unknown_time_encryption_conflict_groups'],1)
        self.assertEqual(len(result.private_relation.rows),4)
        self.assertIs(result.private_relation,s.persisted)
        self.assertIn(' True ',result.private_relation.rows[0].values())
        self.assertEqual(attempt.status()['status'],'COMPLETE')
        self.assertEqual(s.options,[('versionAsOf',2558)])
        before=s.actions;result.status();self.assertEqual(s.actions,before)

    def test_empty_scope(self):
        p=new_plan();s=old.Spark([old.row('OTHER')])
        with distributed_adapter(s):
            result=v.execute_distributed(p,approval=old.consent(p),attempt=v.DistributedAttempt(),spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertEqual(result.status()['state'],'NO_SCOPED_MATCHES')
        self.assertEqual(result.status()['source_empty'],'UNVERIFIED')

    def test_new_plan_and_old_approval_refused(self):
        parent=old.plan();before=deepcopy(parent.private)
        p=v.prepare_distributed(parent,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertEqual(before,parent.private);self.assertNotEqual(parent.plan_id,p.plan_id)
        for approval in (None,old.consent(parent),{'plan_id':p.plan_id,'allow_values':True}):
            with self.assertRaises(v.ValidationError):
                v.execute_distributed(p,approval=approval,attempt=v.DistributedAttempt(),spark=None,engine_source=old.ENGINE,config_source=old.CONFIG)
        p.private['parent_plan']['runtime']['RCAIP_POST_SLUG']='changed'
        with self.assertRaisesRegex(v.ValidationError,'CHANGED_PLAN'):
            v.execute_distributed(p,approval=old.consent(parent),attempt=v.DistributedAttempt(),spark=None,engine_source=old.ENGINE,config_source=old.CONFIG)

    def test_failed_action_retained_not_complete_and_no_retry(self):
        p=new_plan();s=old.Spark([old.row()]);attempt=v.DistributedAttempt()
        with distributed_adapter(s,fail_collect=True),self.assertRaises(RuntimeError):
            v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertIsNotNone(attempt.private_exception);self.assertIsNotNone(attempt.private_relation)
        self.assertIsNone(attempt.private_result);self.assertEqual(attempt.status()['status'],'STOP')
        with self.assertRaisesRegex(v.ValidationError,'ATTEMPT_ALREADY_USED'):
            v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=None,engine_source=old.ENGINE,config_source=old.CONFIG)

    def test_post_read_identity_change(self):
        p=new_plan();s=old.Spark([old.row()]);attempt=v.DistributedAttempt();calls=[]
        def sql(query):
            calls.append(query)
            return SimpleNamespace(first=lambda:{'id':'SYNTHETIC-ID' if len(calls)==1 else 'OTHER','format':'delta'})
        s.sql=sql
        with distributed_adapter(s),self.assertRaisesRegex(v.ValidationError,'IDENTITY_CHANGED'):
            v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertEqual(s.actions,1);self.assertIsNone(attempt.private_result)

    def test_partial_and_invalid_summary_rejected(self):
        with distributed_adapter(old.Spark()) as f:
            row=v.distributed_summary_query(sf.D([summary_row()]),f).collect()[0].asDict()
        for key in ('enc_0','grouped_identity_rows','scoped_rows'):
            broken=dict(row);broken[key]+=1
            with self.assertRaises(v.ValidationError):v.complete_summary(broken)
        broken=dict(row);broken.pop('scoped_rows')
        with self.assertRaisesRegex(v.ValidationError,'SHAPE'):v.complete_summary(broken)

    def test_pre_read_guards(self):
        for field,bad in [('version',2559),('source','other')]:
            parent=old.plan();parent.private[field]=bad;parent.plan_id=v.digest(parent.private)
            with self.assertRaises(v.ValidationError):v.prepare_distributed(parent,engine_source=old.ENGINE,config_source=old.CONFIG)
        p=new_plan()
        with self.assertRaisesRegex(v.ValidationError,'ENGINE_FINGERPRINT'):
            v.execute_distributed(p,approval=old.consent(p),attempt=v.DistributedAttempt(),spark=None,engine_source=old.ENGINE+'# edit',config_source=old.CONFIG)
        s=old.Spark();s.conf.get=lambda _: 'Europe/London'
        with self.assertRaisesRegex(v.ValidationError,'TIMEZONE_CHANGED'):
            v.execute_distributed(p,approval=old.consent(p),attempt=v.DistributedAttempt(),spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)

    def test_missing_identity_whitespace_and_all_categories(self):
        rows=[summary_row(i) for i in range(8)]
        rows[0]['asset_key']=None
        rows[1]['_validation_time_raw']='\u0085\u001c';rows[1]['intune_last_contact']=None
        rows[2]['observation_age_bucket']='Future Date Review'
        rows[3]['observation_age_bucket']='Stale'
        with distributed_adapter(old.Spark()) as f:
            actual=v.complete_summary(v.distributed_summary_query(sf.D(rows),f).collect()[0].asDict())
        for k,value in v.summarize(rows).items():self.assertEqual(actual[k],value,k)

    def test_schema_change_before_action_and_partial_cardinality(self):
        p=new_plan();s=old.Spark();attempt=v.DistributedAttempt()
        with distributed_adapter(s),patch.object(sf.D,'schema',property(lambda _:SimpleNamespace(jsonValue=lambda:{}))):
            with self.assertRaisesRegex(v.ValidationError,'SCHEMA_CHANGED'):
                v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertEqual(s.actions,0)
        s=old.Spark([old.row()]);attempt=v.DistributedAttempt()
        fake=SimpleNamespace(limit=lambda n:SimpleNamespace(collect=lambda:[]))
        with distributed_adapter(s),patch.object(v,'distributed_summary_query',return_value=fake):
            with self.assertRaisesRegex(v.ValidationError,'SUMMARY_CARDINALITY'):
                v.execute_distributed(p,approval=old.consent(p),attempt=attempt,spark=s,engine_source=old.ENGINE,config_source=old.CONFIG)
        self.assertIsNone(attempt.private_result)

    def operator_namespace(self):
        return {'RCAIP_INTUNE_PLAN':old.plan(),'RCAIP_INTUNE_ENGINE_SOURCE':old.ENGINE,
                'RCAIP_INTUNE_CONFIG_SOURCE':old.CONFIG,
                'RCAIP_INTUNE_VALUE_ATTEMPT':{'execute_dispatched':True,'authorization_reference':'OLD-APPROVAL'},
                'RCAIP_INTUNE_VALUE_APPROVAL':{'retained':'original'},
                '_RCAIP_INTUNE_VALUE_EXCEPTION':RuntimeError('private old failure')}

    def test_operator_plan_default_preserves_prior_and_new_approval(self):
        ns=self.operator_namespace();before=dict(ns)
        self.assertEqual(operator.run(ns,validator_source=SOURCE)['phase'],'PLAN_ONLY')
        for k,value in before.items():self.assertIs(ns[k],value)
        p=ns[operator.PREFIX+'PLAN'];ns['spark']=old.Spark([old.row()])
        with distributed_adapter(ns['spark']):
            result=operator.run(ns,validator_source=SOURCE,mode='EXECUTE',exact_plan_id=p.plan_id,
                    allow_values=True,allow_full_snapshot_scan=True,authorization_reference='NEW-APPROVAL')
        self.assertEqual(result['status'],'COMPLETE')
        count=ns['spark'].actions
        self.assertEqual(operator.run(ns,validator_source=SOURCE,mode='EXECUTE')['status'],'STOP')
        self.assertEqual(ns['spark'].actions,count)
        for k,value in before.items():self.assertIs(ns[k],value)

    def test_operator_failed_action_private_safe_output(self):
        ns=self.operator_namespace();operator.run(ns,validator_source=SOURCE)
        p=ns[operator.PREFIX+'PLAN'];ns['spark']=old.Spark([old.row()])
        with distributed_adapter(ns['spark'],fail_collect=True):
            result=operator.run(ns,validator_source=SOURCE,mode='EXECUTE',exact_plan_id=p.plan_id,
                    allow_values=True,allow_full_snapshot_scan=True,authorization_reference='NEW-APPROVAL')
        self.assertEqual(result['status'],'STOP');self.assertFalse(result['complete_result_retained'])
        self.assertNotIn('SYNTHETIC',json.dumps(result))
        self.assertIsNotNone(ns[operator.PREFIX+'EXCEPTION'])
        prior=ns[operator.PREFIX+'EXCEPTION']
        operator.run(ns,validator_source=SOURCE)
        self.assertIs(ns[operator.PREFIX+'EXCEPTION'],prior)


if __name__=='__main__':
    unittest.main(verbosity=2)
