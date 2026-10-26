"""Explicit two-phase Intune validation. Import performs no I/O or Spark actions.

Private plans/results contain Department context. Only status() is shareable,
subject to Department approval. No writes, deduplication or assessment refresh.
"""
import ast
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date
import hashlib
import json
import re

SOURCE = 'example_sources.intune_devices_1'
VERSION = 0  # Fictional example; original snapshot binding is not published.
ENGINE_SHA = 'bfefa7412b2cc99b57bc32569e25f9881b82cf119c62a1a9ae15640254964d97'
CONFIG_SHA = 'b5739b3387f219da83e52089823c009503341df2f2cb095ce96f543930966cc8'
SCAN = 'FULL_SNAPSHOT_SCAN_POSSIBLE'
RUNTIME = ('RCAIP_POST_NAME', 'RCAIP_POST_SLUG', 'RCAIP_RUN_ID',
           'RCAIP_POST_DEVICE_PREFIX', 'RCAIP_ASSESSMENT_END_DATE_TEXT',
           'RCAIP_POST_TOKENS', 'RCAIP_DNS_TOKENS',
           'RCAIP_USER_LOCATION_TOKENS', 'RCAIP_MDE_TOKENS')
HELPERS = ('clean_column_name', 'clean_dataframe_columns', 'first_existing_column',
           'blank_to_null', 'text_expression', 'timestamp_expression',
           'normalized_host_key', 'normalized_serial', 'build_token_regex',
           'add_lifecycle_columns')


class ValidationError(ValueError):
    pass


def require(ok, reason):
    if not ok:
        raise ValidationError(reason)


def source_metadata(spark, *, expected_table_id):
    """Explicit metadata-only probe, never called by import or prepare.

    No history discovery, version selection or device-value action.
    """
    require(isinstance(expected_table_id, str) and expected_table_id, 'TABLE_ID_REQUIRED')
    try:
        detail = spark.sql('DESCRIBE DETAIL ' + SOURCE).first()
        require(detail['id'] == expected_table_id and detail['format'].lower() == 'delta',
                'SOURCE_IDENTITY_CHANGED')
        schema = spark.read.format('delta').option('versionAsOf', VERSION).table(SOURCE).schema.jsonValue()
    except Exception as error:
        getter = getattr(error, 'getCondition', None) or getattr(error, 'getErrorClass', None)
        condition = getter() if getter else None
        if condition in ('TABLE_OR_VIEW_NOT_FOUND', 'DELTA_TABLE_NOT_FOUND',
                         'INSUFFICIENT_PERMISSIONS', 'PERMISSION_DENIED'):
            raise ValidationError('SOURCE_UNAVAILABLE:' + condition) from error
        raise  # Programming/resource/version failures are not ordinary absence.
    return {'source': SOURCE, 'version': VERSION, 'provider': 'delta',
            'table_id': expected_table_id, 'schema': schema,
            'schema_origin': 'VERSION_AS_OF_2558',
            'session_timezone': spark.conf.get('spark.sql.session.timeZone')}


def fingerprint(source):
    return hashlib.sha256(source.replace('\r\n', '\n').encode('utf-8')).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def expressions(engine, config):
    require(fingerprint(engine) == ENGINE_SHA, 'ENGINE_FINGERPRINT_MISMATCH')
    require(fingerprint(config) == CONFIG_SHA, 'CONFIG_FINGERPRINT_MISMATCH')
    tree = ast.parse(engine)
    helpers = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in HELPERS]
    require({n.name for n in helpers} == set(HELPERS), 'HELPER_SELECTION')
    stages = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and
              any(isinstance(t, ast.Name) and t.id == 'intune_stage' for t in n.targets)]
    require(len(stages) == 1, 'INTUNE_STAGE_SELECTION')
    # The complete accepted withColumn/filter expression, not a copied classifier.
    return helpers, stages[0]


def namespace(engine, config, runtime, functions=None):
    helpers, stage = expressions(engine, config)
    ns = {'re': re, 'F': functions, **deepcopy(runtime), 'intune_source_table': SOURCE}
    exec(compile(ast.Module(helpers, type_ignores=[]), '<reviewed-helpers>', 'exec'), ns)
    ns['post_token_regex'] = ns['build_token_regex']([
        token for key in RUNTIME[-4:] for token in runtime[key]])
    ns['post_device_prefix_upper'] = runtime['RCAIP_POST_DEVICE_PREFIX'].strip().upper()
    return ns, stage


def selection(engine, config, runtime, schema):
    ns, stage = namespace(engine, config, runtime)
    names = [f['name'] for f in schema['fields']]
    require(len(names) == len(set(names)), 'DUPLICATE_SOURCE_COLUMNS')
    clean, used = [], set()
    for name in names:
        base = ns['clean_column_name'](name)
        item, suffix = base, 2
        while item in used:
            item, suffix = base + '_' + str(suffix), suffix + 1
        used.add(item)
        clean.append(item)
    mappings = {}
    for node in ast.walk(stage):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'withColumn':
            target, expr = node.args
            if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name) and expr.func.id in ('text_expression', 'timestamp_expression'):
                candidates = ast.literal_eval(expr.args[1])
                mappings[ast.literal_eval(target)] = next((c for c in candidates if c in used), None)
    needed = sorted({c for c in mappings.values() if c})
    types = dict(zip(clean, [f['type'] for f in schema['fields']]))
    require(all(types[c] in ('string', 'timestamp', 'timestamp_ntz', 'date', 'long',
                            'integer', 'short', 'byte', 'boolean', 'double', 'float')
                or isinstance(types[c], str) and types[c].startswith('decimal(')
                for c in needed), 'UNSUPPORTED_SELECTED_TYPE')
    require((mappings.get('intune_device_name') or mappings.get('intune_serial_number'))
            and (mappings.get('intune_device_name') or mappings.get('intune_user_name'))
            and mappings.get('intune_last_contact') and mappings.get('_intune_encryption_state_raw'),
            'SCHEMA_INCOMPATIBLE_REQUIRED_FIELDS')
    return {'clean_names': clean, 'columns': needed, 'mappings': mappings,
            'optional_missing': sorted(k for k, v in mappings.items() if not v)}


class Plan:
    def __init__(self, payload):
        self.private = deepcopy(payload)
        self.plan_id = digest(payload)

    def status(self):
        return {'phase': 'PLAN_ONLY', 'version': VERSION, 'scan': SCAN,
                'hard_scan_cap': None, 'values_read': False,
                'optional_fields_missing': len(self.private['selection']['optional_missing'])}


def prepare(*, engine_source, config_source, runtime, metadata, max_rows=10000):
    """Pure plan from explicitly copied certified context and schema metadata.

    metadata schema must be from version 2558, not current DESCRIBE DETAIL.
    Certification_reference identifies existing evidence; it is not a new cert.
    """
    expressions(engine_source, config_source)
    require(set(runtime) == set(RUNTIME) | {'certification_reference'}, 'RUNTIME_CONTRACT')
    require(all(isinstance(runtime[k], str) and runtime[k].strip() for k in
                ('RCAIP_POST_NAME', 'RCAIP_POST_SLUG', 'RCAIP_RUN_ID',
                 'RCAIP_ASSESSMENT_END_DATE_TEXT', 'certification_reference')), 'RUNTIME_AUTHORITY_REQUIRED')
    date.fromisoformat(runtime['RCAIP_ASSESSMENT_END_DATE_TEXT'])
    require(isinstance(runtime['RCAIP_POST_DEVICE_PREFIX'], str), 'PREFIX_TYPE')
    require(all(isinstance(runtime[k], list) and all(isinstance(t, str) for t in runtime[k])
                for k in RUNTIME[-4:]), 'TOKEN_TYPES')
    require(runtime['RCAIP_POST_DEVICE_PREFIX'].strip() or
            any(t.strip() for k in RUNTIME[-4:] for t in runtime[k]), 'EMPTY_SCOPE')
    require(metadata['source'] == SOURCE and metadata['version'] == VERSION and
            metadata['provider'].lower() == 'delta' and metadata['table_id'], 'SNAPSHOT_BINDING')
    require(metadata.get('schema_origin') == 'VERSION_AS_OF_2558', 'PINNED_SCHEMA_REQUIRED')
    require(metadata.get('session_timezone') in ('UTC', 'Etc/UTC', '+00:00'), 'UTC_TRANSPORT_REQUIRED')
    require(type(max_rows) is int and 0 < max_rows <= 100000, 'PRIVATE_ROW_LIMIT')
    chosen = selection(engine_source, config_source, runtime, metadata['schema'])
    payload = {'source': SOURCE, 'version': VERSION, 'engine_sha': ENGINE_SHA,
               'config_sha': CONFIG_SHA, 'runtime': deepcopy(runtime),
               'metadata': deepcopy(metadata), 'selection': chosen,
               'scan': SCAN, 'hard_scan_cap': None, 'max_rows': max_rows}
    return Plan(payload)


def approve(plan, *, exact_plan_id, allow_values=False, allow_full_snapshot_scan=False):
    """Call only after separate approval; creates no approval by default."""
    require(exact_plan_id == plan.plan_id == digest(plan.private), 'CHANGED_PLAN')
    require(allow_values is True, 'VALUE_APPROVAL_REQUIRED')
    require(allow_full_snapshot_scan is True, 'SCAN_APPROVAL_REQUIRED')
    return {'plan_id': exact_plan_id, 'allow_values': True, 'scan_allowance': SCAN}


class Result:
    def __init__(self, rows, plan):
        self.private_rows = rows
        self.private_plan = deepcopy(plan.private)
        self._status = summarize(rows)

    def status(self):
        return deepcopy(self._status)


def summarize(rows):
    encryption = Counter(r['intune_encryption_state'] for r in rows)
    allowed = {'REPORTED_ENCRYPTED', 'REPORTED_NOT_ENCRYPTED',
               'UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW', 'NO_ENCRYPTION_VALUE'}
    require(set(encryption) <= allowed, 'UNEXPECTED_ENCRYPTION_DECISION')
    groups, times = defaultdict(list), defaultdict(list)
    for row in rows:
        if row['asset_key'] is not None:
            groups[row['asset_key']].append(row)
            times[(row['asset_key'], row['intune_last_contact'])].append(row)
    ties = [rs for rs in times.values() if len(rs) > 1]
    conflicts = [rs for rs in ties if len({(r['_intune_encryption_state_raw'],
                                          r['intune_encryption_state']) for r in rs}) > 1]
    usable = sum(r['asset_key'] is not None and r['intune_last_contact'] is not None for r in rows)
    return {'state': ('NO_SCOPED_MATCHES' if not rows else
                      'USABLE_SCOPED_RECORDS' if usable else 'SCOPED_EVIDENCE_UNUSABLE'),
            'source_empty': 'UNVERIFIED' if not rows else False,
            'scoped_rows': len(rows), 'usable_identity_and_time_rows': usable,
            'missing_identity_rows': sum(r['asset_key'] is None for r in rows),
            'null_or_blank_timestamp_rows': sum(r['_validation_time_raw'] is None or
                not r['_validation_time_raw'].strip() for r in rows),
            'timestamp_parse_failure_rows': sum(bool((r['_validation_time_raw'] or '').strip())
                and r['intune_last_contact'] is None for r in rows),
            'future_rows': sum(r['observation_age_bucket'] == 'Future Date Review' for r in rows),
            'stale_rows': sum(r['observation_age_bucket'] == 'Stale' for r in rows),
            'duplicate_identity_excess_rows': sum(len(rs)-1 for rs in groups.values()),
            'equal_time_groups': len(ties), 'conflicting_encryption_equal_time_groups': len(conflicts),
            'encryption_categories': dict(sorted(encryption.items())),
            'deduplication': 'NOT_PERFORMED', 'assessment_refresh': False}


def execute(plan, *, approval=None, spark, engine_source, config_source):
    """One projected acquisition, max_rows+1 overflow guard, no source census.

    The row guard bounds returned records, NOT bytes scanned. Full scan approval
    is mandatory. Exceeding it returns no misleading partial summary.
    """
    require(plan.plan_id == digest(plan.private), 'CHANGED_PLAN')
    require(isinstance(approval, dict) and approval.get('plan_id') == plan.plan_id and
            approval.get('allow_values') is True, 'VALUE_APPROVAL_REQUIRED')
    require(approval.get('scan_allowance') == SCAN, 'SCAN_APPROVAL_REQUIRED')
    p = plan.private
    rebuilt = prepare(engine_source=engine_source, config_source=config_source,
                      runtime=p['runtime'], metadata=p['metadata'], max_rows=p['max_rows'])
    require(rebuilt.plan_id == plan.plan_id, 'PLAN_CONTRACT_CHANGED')
    # Only now touch Spark, including metadata. No change to session settings.
    require(spark.conf.get('spark.sql.session.timeZone') == p['metadata']['session_timezone'],
            'SESSION_TIMEZONE_CHANGED')
    def identity():
        row = spark.sql('DESCRIBE DETAIL ' + SOURCE).first()
        require(row['id'] == p['metadata']['table_id'] and row['format'].lower() == 'delta',
                'SOURCE_IDENTITY_CHANGED')
    identity()
    raw = spark.read.format('delta').option('versionAsOf', VERSION).table(SOURCE)
    require(raw.schema.jsonValue() == p['metadata']['schema'], 'PINNED_SCHEMA_CHANGED')
    from pyspark.sql import functions as F
    ns, stage = namespace(engine_source, config_source, p['runtime'], F)
    raw = ns['clean_dataframe_columns'](raw).select(*p['selection']['columns'])
    for i, col in enumerate(p['selection']['columns']):
        raw = raw.withColumn('_validation_raw_' + str(i), F.col(col).cast('string'))
    time_col = p['selection']['mappings']['intune_last_contact']
    raw = raw.withColumn('_validation_time_raw', F.col(time_col).cast('string'))
    ns['intune_raw'] = raw
    exec(compile(ast.Module([stage], type_ignores=[]), '<reviewed-intune-stage>', 'exec'), ns)
    scoped = ns['add_lifecycle_columns'](ns['intune_stage'], 'intune_last_contact')
    # Avoid Python datetime range conversion; schema remains bound in the plan.
    columns = [F.col(f.name).cast('string').alias(f.name)
               if f.dataType.typeName() in ('timestamp', 'timestamp_ntz', 'date', 'decimal', 'double', 'float')
               else F.col(f.name) for f in scoped.schema.fields]
    rows = [row.asDict(recursive=True) for row in scoped.select(*columns).limit(p['max_rows'] + 1).collect()]
    require(len(rows) <= p['max_rows'], 'PRIVATE_ROW_LIMIT_EXCEEDED_NO_COMPLETE_RESULT')
    identity()
    return Result(rows, plan)


# Explicit full-population mode; the preceding accepted small-result path is
# unchanged. No automatic fallback from an executed/overflowed small plan.
DISTRIBUTED_MODE = 'DISTRIBUTED_SESSION_V1'
ENCRYPTION = ('REPORTED_ENCRYPTED', 'REPORTED_NOT_ENCRYPTED',
              'UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW', 'NO_ENCRYPTION_VALUE')


class DistributedPlan(Plan):
    def status(self):
        return {'phase': 'PLAN_ONLY', 'result_mode': DISTRIBUTED_MODE,
                'version': VERSION, 'scan': SCAN, 'hard_scan_cap': None,
                'values_read': False, 'summary_driver_rows': 1,
                'retention': 'SESSION_ONLY_NOT_DURABLE', 'retry_authorized': False}


def prepare_distributed(previous_plan, *, engine_source, config_source):
    """New plan copied from retained authority. Does not edit the executed plan."""
    require(previous_plan.plan_id == digest(previous_plan.private), 'CHANGED_PARENT_PLAN')
    old = previous_plan.private
    rebuilt = prepare(engine_source=engine_source, config_source=config_source,
                      runtime=old['runtime'], metadata=old['metadata'], max_rows=old['max_rows'])
    require(rebuilt.plan_id == previous_plan.plan_id, 'PARENT_CONTRACT_CHANGED')
    return DistributedPlan({'parent_plan': deepcopy(old), 'parent_plan_id': previous_plan.plan_id,
                            'result_mode': DISTRIBUTED_MODE, 'summary_driver_rows': 1,
                            'retention': 'SESSION_MEMORY_AND_DISK_NOT_DURABLE', 'scan': SCAN})


class DistributedAttempt:
    """Private one-use ledger; never clear/reuse an attempted object to retry."""
    def __init__(self):
        self.started = False
        self.stage = 'NOT_DISPATCHED'
        self.private_relation = None
        self.private_exception = None
        self.private_result = None

    def status(self):
        return {'status': 'COMPLETE' if self.private_result is not None else 'STOP',
                'stage': self.stage, 'execute_dispatched': self.started,
                'complete_result_retained': self.private_result is not None,
                'retention': 'SESSION_ONLY_NOT_DURABLE', 'retry_authorized': False,
                'delta_writes': 'NONE', 'assessment_refresh': False}


class DistributedResult:
    def __init__(self, relation, plan, summary):
        self.private_relation = relation
        self.private_plan = deepcopy(plan.private)
        self._status = deepcopy(summary)

    def status(self):
        # Fixed scalars/categories only. Never executes Spark or exposes keys/raws.
        return deepcopy(self._status)


def distributed_summary_query(relation, F):
    """One row, fixed columns. All per-identity grouping remains distributed."""
    def tally(condition):
        return F.coalesce(F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))), F.lit(0))
    key, time = F.col('asset_key'), F.col('intune_last_contact')
    # Python str.strip (accepted small summary) recognizes more whitespace than
    # Spark trim. Explicit equivalent character class, not a normalization change.
    whitespace = r'^[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]*$'
    blank = F.col('_validation_time_raw').isNull() | F.col('_validation_time_raw').rlike(whitespace)
    state = F.col('intune_encryption_state')
    base = relation.agg(
        F.count(F.lit(1)).alias('scoped_rows'),
        F.countDistinct(key).alias('distinct_nonnull_asset_keys'),
        tally(key.isNotNull()).alias('nonnull_identity_rows'),
        tally(key.isNull()).alias('missing_identity_rows'),
        tally(key.isNotNull() & time.isNotNull()).alias('usable_identity_and_time_rows'),
        tally(blank).alias('null_or_blank_timestamp_rows'),
        tally((~blank) & time.isNull()).alias('timestamp_parse_failure_rows'),
        tally(F.col('observation_age_bucket') == 'Future Date Review').alias('future_rows'),
        tally(F.col('observation_age_bucket') == 'Stale').alias('stale_rows'),
        tally(state.isNull() | ~state.isin(*ENCRYPTION)).alias('unexpected_encryption_rows'),
        *[tally(state == value).alias('enc_' + str(i)) for i, value in enumerate(ENCRYPTION)])
    groups = relation.filter(key.isNotNull()).groupBy('asset_key', 'intune_last_contact').agg(
        F.count(F.lit(1)).alias('group_rows'),
        # Struct retains null raw values as a distinct pair, like Python tuples.
        F.countDistinct(F.struct(F.col('_intune_encryption_state_raw'), state)).alias('variants'))
    tied = F.col('group_rows') > 1
    conflict = tied & (F.col('variants') > 1)
    grouped = groups.agg(
        F.coalesce(F.sum(F.col('group_rows')), F.lit(0)).alias('grouped_identity_rows'),
        tally(tied).alias('equal_time_groups'),
        tally(conflict).alias('conflicting_encryption_equal_time_groups'),
        tally(tied & time.isNull()).alias('unknown_time_ambiguity_groups'),
        tally(conflict & time.isNull()).alias('unknown_time_encryption_conflict_groups'))
    return base.crossJoin(grouped)


def complete_summary(row):
    base_fields = ('scoped_rows', 'distinct_nonnull_asset_keys', 'nonnull_identity_rows',
                   'missing_identity_rows', 'usable_identity_and_time_rows',
                   'null_or_blank_timestamp_rows', 'timestamp_parse_failure_rows',
                   'future_rows', 'stale_rows', 'unexpected_encryption_rows',
                   'grouped_identity_rows', 'equal_time_groups',
                   'conflicting_encryption_equal_time_groups', 'unknown_time_ambiguity_groups',
                   'unknown_time_encryption_conflict_groups')
    require(set(row) == set(base_fields) | {'enc_' + str(i) for i in range(4)}, 'SUMMARY_SHAPE')
    require(all(type(x) is int and x >= 0 for x in row.values()), 'SUMMARY_COUNTS')
    n = row['scoped_rows']
    require(row['unexpected_encryption_rows'] == 0, 'UNEXPECTED_ENCRYPTION_DECISION')
    require(sum(row['enc_' + str(i)] for i in range(4)) == n, 'ENCRYPTION_ACCOUNTING')
    require(row['nonnull_identity_rows'] + row['missing_identity_rows'] == n and
            row['grouped_identity_rows'] == row['nonnull_identity_rows'] and
            row['distinct_nonnull_asset_keys'] <= row['nonnull_identity_rows'], 'IDENTITY_ACCOUNTING')
    require(row['usable_identity_and_time_rows'] <= row['nonnull_identity_rows'] and
            row['null_or_blank_timestamp_rows'] + row['timestamp_parse_failure_rows'] <= n and
            row['future_rows'] + row['stale_rows'] <= n and
            row['conflicting_encryption_equal_time_groups'] <= row['equal_time_groups'] <= row['nonnull_identity_rows']//2 and
            row['unknown_time_ambiguity_groups'] <= row['equal_time_groups'] and
            row['unknown_time_encryption_conflict_groups'] <= min(row['unknown_time_ambiguity_groups'], row['conflicting_encryption_equal_time_groups']),
            'QUALITY_ACCOUNTING')
    result = {k: row[k] for k in base_fields if k not in
              ('nonnull_identity_rows', 'grouped_identity_rows', 'unexpected_encryption_rows')}
    result.update(state='NO_SCOPED_MATCHES' if not n else 'USABLE_SCOPED_RECORDS'
                  if row['usable_identity_and_time_rows'] else 'SCOPED_EVIDENCE_UNUSABLE',
                  source_empty='UNVERIFIED' if not n else False,
                  duplicate_identity_excess_rows=row['nonnull_identity_rows']-row['distinct_nonnull_asset_keys'],
                  encryption_categories={value: row['enc_'+str(i)] for i,value in enumerate(ENCRYPTION) if row['enc_'+str(i)]},
                  deduplication='NOT_PERFORMED', assessment_refresh=False,
                  result_mode=DISTRIBUTED_MODE, retention='SESSION_ONLY_NOT_DURABLE',
                  identity_count_unit='NONNULL_ANALYTICAL_ASSET_KEYS_NOT_PHYSICAL_DEVICES',
                  null_time_caveat='UNKNOWN_TIME_GROUPS_ARE_AMBIGUITY_NOT_PROVEN_SIMULTANEITY')
    return result


def execute_distributed(plan, *, approval=None, attempt, spark, engine_source, config_source):
    """Separate approved attempt. Full relation stays in Spark; one summary row.

    Persist covers only projected, Post-scoped pre-dedup evidence. Actions can
    recompute evicted/failed partitions. No single-scan or durability guarantee.
    """
    require(isinstance(attempt, DistributedAttempt) and not attempt.started, 'ATTEMPT_ALREADY_USED')
    require(plan.plan_id == digest(plan.private), 'CHANGED_PLAN')
    previous = Plan(plan.private['parent_plan'])
    require(previous.plan_id == plan.private['parent_plan_id'], 'CHANGED_PARENT_PLAN')
    rebuilt = prepare_distributed(previous, engine_source=engine_source, config_source=config_source)
    require(rebuilt.plan_id == plan.plan_id, 'PLAN_CONTRACT_CHANGED')
    require(isinstance(approval, dict) and approval.get('plan_id') == plan.plan_id and
            approval.get('allow_values') is True, 'VALUE_APPROVAL_REQUIRED')
    require(approval.get('scan_allowance') == SCAN, 'SCAN_APPROVAL_REQUIRED')
    attempt.started, attempt.stage = True, 'PRE_READ'
    try:
        p = previous.private
        require(spark.conf.get('spark.sql.session.timeZone') == p['metadata']['session_timezone'], 'SESSION_TIMEZONE_CHANGED')
        def identity():
            detail = spark.sql('DESCRIBE DETAIL ' + SOURCE).first()
            require(detail['id'] == p['metadata']['table_id'] and detail['format'].lower() == 'delta', 'SOURCE_IDENTITY_CHANGED')
        identity()
        raw = spark.read.format('delta').option('versionAsOf', VERSION).table(SOURCE)
        require(raw.schema.jsonValue() == p['metadata']['schema'], 'PINNED_SCHEMA_CHANGED')
        from pyspark.sql import functions as F
        from pyspark import StorageLevel
        ns, stage = namespace(engine_source, config_source, p['runtime'], F)
        raw = ns['clean_dataframe_columns'](raw).select(*p['selection']['columns'])
        for i, col in enumerate(p['selection']['columns']):
            raw = raw.withColumn('_validation_raw_' + str(i), F.col(col).cast('string'))
        raw = raw.withColumn('_validation_time_raw', F.col(p['selection']['mappings']['intune_last_contact']).cast('string'))
        ns['intune_raw'] = raw
        exec(compile(ast.Module([stage], type_ignores=[]), '<reviewed-intune-stage>', 'exec'), ns)
        scoped = ns['add_lifecycle_columns'](ns['intune_stage'], 'intune_last_contact')
        # Retain native typed evidence; no dedup/filter beyond the accepted stage.
        attempt.private_relation = scoped
        scoped.persist(StorageLevel.MEMORY_AND_DISK)
        attempt.stage = 'FULL_POPULATION_AGGREGATION'
        query = distributed_summary_query(scoped, F)
        # Algebra has exactly one row. Additional guard never collects group detail.
        records = query.limit(2).collect()
        require(len(records) == 1, 'SUMMARY_CARDINALITY')
        summary = complete_summary(records[0].asDict(recursive=True))
        attempt.stage = 'POST_ACQUISITION_IDENTITY'
        identity()
        result = DistributedResult(scoped, plan, summary)
        attempt.private_result, attempt.stage = result, 'COMPLETE'
        return result
    except BaseException as error:
        # Preserve private failure/lineage, never call it a complete result. Caller
        # must not print the exception, clear the attempt or retry automatically.
        attempt.private_exception = error
        raise
