"""Intune Evidence Profile / Internal RCSO Review. Inert, session-only consumer.

No acquisition, writes, winner selection, routing or assurance decisions.
Only run(..., mode='PROFILE') after separate retained-lineage access approval.
"""
from copy import deepcopy
import hashlib
import json
from types import ModuleType

VALIDATOR_SHA = '70ce7c022a0ae39301eceba6c9989efc920517241de57034588af8f7a25b0988'
AGES = ('Current', 'Recent', 'Stale', 'Unknown', 'Future Date Review')
STATES = ('REPORTED_ENCRYPTED', 'REPORTED_NOT_ENCRYPTED',
          'UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW', 'NO_ENCRYPTION_VALUE')
VARIATIONS = ('intune_device_name', 'intune_serial_number', 'intune_os_version',
              'intune_operating_system')
KINDS = ('BOTH_REPORTED_STATES', 'OTHER_MULTIPLE_CATEGORIES', 'RAW_REPRESENTATIONS_ONLY')
TIMES = ('UNKNOWN_TIME', 'LATEST_KNOWN_CONTACT', 'OTHER_KNOWN_CONTACT')
PREFIX = 'RCAIP_INTUNE_PROFILE_'
DP = 'RCAIP_INTUNE_DISTRIBUTED_'


class ProfileError(ValueError):
    pass


def require(ok, code):
    if not ok:
        raise ProfileError(code)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


class Plan:
    def __init__(self, payload, result, attempt, validator):
        self.private = deepcopy(payload)
        self.plan_id = digest(payload)
        self.result, self.attempt, self.validator = result, attempt, validator
        self.relation = result.private_relation

    def status(self):
        return {'phase': 'PLAN_ONLY', 'product': 'Intune Evidence Profile / Internal RCSO Review',
                'values_evaluated': False, 'retention': 'SESSION_ONLY_NOT_DURABLE',
                'possible_bound_source_recomputation': True, 'summary_driver_rows': 1}


def prepare(namespace, *, validator_source):
    """Pure retained-object/schema checks. No Spark action or metadata query."""
    require(hashlib.sha256(validator_source.replace('\r\n', '\n').encode()).hexdigest()
            == VALIDATOR_SHA, 'VALIDATOR_SOURCE_MISMATCH')
    names = [DP + x for x in ('MODULE', 'PLAN', 'RESULT', 'ATTEMPT', 'APPROVAL')]
    names += ['RCAIP_INTUNE_PLAN', 'RCAIP_INTUNE_ENGINE_SOURCE', 'RCAIP_INTUNE_CONFIG_SOURCE',
              'RCAIP_INTUNE_VALUE_ATTEMPT']
    require(all(n in namespace for n in names), 'RETAINED_CONTEXT_REQUIRED_NO_RELOAD')
    module, plan, result, attempt, approval = [namespace[DP + x] for x in
                                             ('MODULE', 'PLAN', 'RESULT', 'ATTEMPT', 'APPROVAL')]
    require(isinstance(result, module.DistributedResult) and
            isinstance(attempt, module.DistributedAttempt), 'RESULT_TYPE')
    require(attempt.started is True and attempt.stage == 'COMPLETE' and
            attempt.private_result is result and attempt.private_relation is result.private_relation
            and attempt.private_exception is None, 'COMPLETED_RESULT_REQUIRED')
    v = ModuleType('verified_intune_profile_contract')
    exec(compile(validator_source, '<verified-validator-contract>', 'exec'), v.__dict__)
    rebuilt = v.prepare_distributed(namespace['RCAIP_INTUNE_PLAN'],
                engine_source=namespace['RCAIP_INTUNE_ENGINE_SOURCE'],
                config_source=namespace['RCAIP_INTUNE_CONFIG_SOURCE'])
    require(plan.plan_id == v.digest(plan.private) == rebuilt.plan_id and
            result.private_plan == plan.private, 'RESULT_PLAN_CHANGED')
    require(isinstance(approval, dict) and approval.get('plan_id') == plan.plan_id and
            approval.get('allow_values') is True and approval.get('scan_allowance') == v.SCAN
            and isinstance(approval.get('authorization_reference'), str)
            and approval['authorization_reference'].strip(), 'ACQUISITION_APPROVAL_REQUIRED')
    prior = namespace['RCAIP_INTUNE_VALUE_ATTEMPT']
    require(isinstance(prior, dict) and prior.get('execute_dispatched') is True,
            'PRIOR_ATTEMPT_REQUIRED')
    summary = result.status()
    require(summary.get('result_mode') == v.DISTRIBUTED_MODE and
            summary.get('retention') == 'SESSION_ONLY_NOT_DURABLE' and
            summary.get('assessment_refresh') is False and
            summary.get('deduplication') == 'NOT_PERFORMED', 'RESULT_SUMMARY_CONTRACT')
    for name in ('scoped_rows', 'distinct_nonnull_asset_keys', 'missing_identity_rows',
                 'conflicting_encryption_equal_time_groups'):
        require(type(summary.get(name)) is int and summary[name] >= 0, 'RESULT_COUNTS')
    require(summary['distinct_nonnull_asset_keys'] + summary['missing_identity_rows'] <=
            summary['scoped_rows'], 'RESULT_ACCOUNTING')
    schema = result.private_relation.schema.jsonValue()
    fields = schema.get('fields', [])
    types = {f['name']: f['type'] for f in fields}
    require(len(types) == len(fields), 'DUPLICATE_RETAINED_COLUMNS')
    required = ('asset_key', 'intune_last_contact', '_validation_time_raw',
                '_intune_encryption_state_raw', 'intune_encryption_state', 'observation_age_bucket')
    require(all(n in types for n in required), 'RETAINED_SCHEMA_REQUIRED')
    require(all(types[n] == 'string' for n in required if n != 'intune_last_contact') and
            types['intune_last_contact'] in ('timestamp', 'timestamp_ntz'), 'RETAINED_SCHEMA_TYPES')
    require(all(types[n] == 'string' for n in VARIATIONS if n in types), 'VARIATION_SCHEMA_TYPES')
    return Plan({'contract': 'RETAINED_INTUNE_PROFILE_V1', 'source_sha': VALIDATOR_SHA,
                 'acquisition_plan': deepcopy(plan.private), 'acquisition_plan_id': plan.plan_id,
                 'acquisition_approval': deepcopy(approval), 'prior_attempt': deepcopy(prior),
                 'summary': summary, 'schema': schema,
                 'variation_fields': [n for n in VARIATIONS if n in types]}, result, attempt, v)


def queries(relation, F, validator, variation_fields):
    """Distributed detail; fixed one-row aggregate. Never collect keys or times."""
    def tally(c):
        return F.coalesce(F.sum(F.when(c, F.lit(1)).otherwise(F.lit(0))), F.lit(0))
    def total(c):
        return F.coalesce(F.sum(F.col(c)), F.lit(0))
    key, time, state = [F.col(n) for n in ('asset_key', 'intune_last_contact', 'intune_encryption_state')]
    age = F.col('observation_age_bucket')
    keyed = relation.filter(key.isNotNull())
    missing = relation.filter(key.isNull())
    profiles = keyed.groupBy('asset_key').agg(
        F.count(F.lit(1)).alias('observations'), tally(time.isNull()).alias('unknown_time_rows'),
        tally(time.isNotNull()).alias('known_time_rows'),
        F.min(time).alias('earliest_known_contact'), F.max(time).alias('latest_known_contact'),
        *[tally(age == a).alias('age_' + str(i)) for i, a in enumerate(AGES)],
        *[tally(state == s).alias('state_' + str(i)) for i, s in enumerate(STATES)],
        *[tally((age == a) & (state == s)).alias('age_state_' + str(i*4+j))
          for i, a in enumerate(AGES) for j, s in enumerate(STATES)],
        *[F.countDistinct(F.struct(F.col(n))).alias(n + '_representations_including_null')
          for n in variation_fields])
    latest = profiles.select('asset_key', 'latest_known_contact')
    groups = keyed.groupBy('asset_key', 'intune_last_contact').agg(
        F.count(F.lit(1)).alias('observations'),
        F.countDistinct(F.struct(F.col('_intune_encryption_state_raw'), state)).alias('variants'),
        F.countDistinct(state).alias('categories'),
        *[tally(state == s).alias('state_' + str(i)) for i, s in enumerate(STATES)])
    groups = groups.join(latest, 'asset_key', 'inner')
    groups = groups.withColumn('time_basis', F.when(time.isNull(), F.lit(TIMES[0]))
              .when(time == F.col('latest_known_contact'), F.lit(TIMES[1])).otherwise(F.lit(TIMES[2])))
    flagged = (F.col('observations') > 1) & (F.col('variants') > 1)
    groups = groups.withColumn('variant_kind', F.when(flagged,
        F.when((F.col('state_0') > 0) & (F.col('state_1') > 0), F.lit(KINDS[0]))
        .when(F.col('categories') > 1, F.lit(KINDS[1])).otherwise(F.lit(KINDS[2]))))
    latest_counts = groups.groupBy('asset_key').agg(
        F.coalesce(F.sum(F.when(F.col('time_basis') == TIMES[1], F.col('observations'))
                        .otherwise(F.lit(0))), F.lit(0)).alias('latest_known_tied_rows'),
        *[F.coalesce(F.sum(F.when(F.col('time_basis') == TIMES[1], F.col('state_' + str(i)))
                              .otherwise(F.lit(0))), F.lit(0)).alias('latest_state_' + str(i))
          for i in range(4)])
    profiles = profiles.join(latest_counts, 'asset_key', 'inner')
    matrix = relation.agg(
        tally(age.isNull() | ~age.isin(*AGES)).alias('unexpected_age'),
        *[tally((age == a) & (state == s)).alias('matrix_rows_' + str(i*4+j))
          for i, a in enumerate(AGES) for j, s in enumerate(STATES)],
        *[F.countDistinct(F.when((age == a) & (state == s), key)).alias('matrix_keys_' + str(i*4+j))
          for i, a in enumerate(AGES) for j, s in enumerate(STATES)])
    partitions = groups.agg(*[
        tally((F.col('variant_kind') == kind) & (F.col('time_basis') == basis)).alias('variant_' + str(i*3+j))
        for i, kind in enumerate(KINDS) for j, basis in enumerate(TIMES)])
    accounting = profiles.agg(F.count(F.lit(1)).alias('profile_keys'), total('observations').alias('profile_rows'))
    baseline = validator.distributed_summary_query(relation, F)
    summary = baseline.crossJoin(matrix).crossJoin(partitions).crossJoin(accounting)
    return profiles, groups, missing, summary, list(baseline.columns)


def complete(row, baseline_fields, plan):
    expected_extra = {'unexpected_age', 'profile_keys', 'profile_rows'}
    expected_extra |= {p + str(i) for p in ('matrix_rows_', 'matrix_keys_') for i in range(20)}
    expected_extra |= {'variant_' + str(i) for i in range(9)}
    require(set(row) == set(baseline_fields) | expected_extra, 'PROFILE_SUMMARY_SHAPE')
    require(all(type(x) is int and x >= 0 for x in row.values()), 'PROFILE_SUMMARY_COUNTS')
    old = plan.validator.complete_summary({k: row[k] for k in baseline_fields})
    require(old == plan.private['summary'], 'RETAINED_SUMMARY_CHANGED')
    require(row['unexpected_age'] == 0, 'UNEXPECTED_AGE_CATEGORY')
    require(sum(row['matrix_rows_' + str(i)] for i in range(20)) == old['scoped_rows'], 'MATRIX_ACCOUNTING')
    require(row['profile_keys'] == old['distinct_nonnull_asset_keys'] and
            row['profile_rows'] + old['missing_identity_rows'] == old['scoped_rows'], 'PROFILE_ACCOUNTING')
    require(sum(row['variant_' + str(i)] for i in range(9)) ==
            old['conflicting_encryption_equal_time_groups'], 'VARIANT_ACCOUNTING')
    require(sum(row['variant_' + str(i*3)] for i in range(3)) ==
            old['unknown_time_encryption_conflict_groups'], 'UNKNOWN_VARIANT_ACCOUNTING')
    require(all(row['matrix_keys_' + str(i)] <= min(row['matrix_rows_' + str(i)], row['profile_keys'])
                for i in range(20)), 'MATRIX_KEY_ACCOUNTING')
    return {'status': 'COMPLETE', 'product': 'Intune Evidence Profile / Internal RCSO Review',
            'baseline': old,
            'freshness_encryption': [{'age': a, 'encryption': s,
                'observations': row['matrix_rows_' + str(i*4+j)],
                'analytical_keys': row['matrix_keys_' + str(i*4+j)]}
                for i, a in enumerate(AGES) for j, s in enumerate(STATES)],
            'variant_groups': [{'kind': k, 'time_basis': t, 'groups': row['variant_' + str(i*3+j)]}
                for i, k in enumerate(KINDS) for j, t in enumerate(TIMES)],
            'key_counts_overlap_across_cells': True, 'retention': 'SESSION_ONLY_NOT_DURABLE',
            'assessment_refresh': False, 'maximum_contact_is_not_newest_source_event': True,
            'source_event_identity_and_category_not_established': True}


class Profile:
    def __init__(self, plan, products, status):
        self.private_plan = deepcopy(plan.private)
        self.private_original_result = plan.result
        self.private_observations = plan.relation
        self.private_keys, self.private_contact_groups, self.private_missing_identity = products
        self._status = deepcopy(status)

    def status(self):
        return deepcopy(self._status)


def run(namespace, *, validator_source, mode='PLAN_ONLY', exact_plan_id=None,
        allow_retained_values=False, allow_possible_recomputation=False, authorization_reference=None):
    """Public operator. STOP codes are fixed; arbitrary exception details stay private."""
    try:
        require(mode in ('PLAN_ONLY', 'PROFILE'), 'MODE_REQUIRED')
        require(PREFIX + 'ATTEMPT' not in namespace, 'PROFILE_ATTEMPT_ALREADY_RETAINED')
        require(PREFIX + 'RESULT' not in namespace, 'PROFILE_RESULT_ALREADY_RETAINED')
        rebuilt = prepare(namespace, validator_source=validator_source)
        if PREFIX + 'PLAN' not in namespace:
            namespace[PREFIX + 'PLAN'] = rebuilt
        plan = namespace[PREFIX + 'PLAN']
        require(plan.plan_id == digest(plan.private) == rebuilt.plan_id and
                plan.result is rebuilt.result and plan.relation is rebuilt.relation and
                plan.attempt is rebuilt.attempt, 'PROFILE_CONTEXT_CHANGED')
        if mode == 'PLAN_ONLY':
            return plan.status()
        attempt = {'started': False, 'complete': False}
        namespace[PREFIX + 'ATTEMPT'] = attempt
        require(exact_plan_id == plan.plan_id, 'EXACT_PROFILE_PLAN_REQUIRED')
        require(allow_retained_values is True, 'PROFILE_VALUE_APPROVAL_REQUIRED')
        require(allow_possible_recomputation is True, 'RECOMPUTATION_APPROVAL_REQUIRED')
        require(isinstance(authorization_reference, str) and authorization_reference.strip()
                and authorization_reference not in (
                    plan.private['acquisition_approval']['authorization_reference'],
                    plan.private['prior_attempt'].get('authorization_reference')), 'NEW_PROFILE_APPROVAL_REQUIRED')
        attempt['approval'] = {'plan_id': plan.plan_id, 'authorization_reference': authorization_reference,
                              'allow_retained_values': True, 'allow_possible_recomputation': True}
        # Only after approval inspect current session timezone, without changing it.
        require(plan.relation.sparkSession.conf.get('spark.sql.session.timeZone') ==
                plan.private['acquisition_plan']['parent_plan']['metadata']['session_timezone'],
                'SESSION_TIMEZONE_CHANGED')
        from pyspark.sql import functions as F
        attempt['started'] = True
        keys, groups, missing, query, base_fields = queries(plan.relation, F, plan.validator,
                                                           plan.private['variation_fields'])
        attempt['private_products'] = (keys, groups, missing)
        records = query.limit(2).collect()
        require(len(records) == 1, 'PROFILE_SUMMARY_CARDINALITY')
        status = complete(records[0].asDict(recursive=True), base_fields, plan)
        # Refuse context replacement/mutation across the evaluation as well.
        after = prepare(namespace, validator_source=validator_source)
        require(after.plan_id == plan.plan_id and after.relation is plan.relation and
                after.result is plan.result and after.attempt is plan.attempt, 'PROFILE_CONTEXT_CHANGED')
        namespace[PREFIX + 'RESULT'] = Profile(plan, (keys, groups, missing), status)
        attempt['complete'] = True
        return status
    except (Exception, KeyboardInterrupt) as error:
        namespace.setdefault(PREFIX + 'EXCEPTION', error)
        return {'status': 'STOP', 'error_code': str(error) if type(error) is ProfileError else
                'PRIVATE_PROFILE_FAILURE_RETAINED', 'retry_authorized': False}
