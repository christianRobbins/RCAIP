"""Operator preparation only. No bridge imports, writes, product rows or auto-call.

Source basis: Config 000 registry bindings/rcaip_table; Config 001 three IP006
registrations. DESCRIBE DETAIL/HISTORY and versionAsOf schema inspection reuse
established repository patterns WITHOUT REPORT 002's latest-product selection.
Call only after enterprise metadata authorization. Display result.status() only.
Private evidence remains in the enterprise process; no witness is manufactured.
"""
import hashlib
from datetime import datetime, timezone
import json
import re

STEMS = {
    'IP006_MDE_REDISCOVERY_SEARCH_INPUT': 'ip006_mde_rediscovery_search_input',
    'IP006_MDE_REDISCOVERY_ENRICHMENT': 'ip006_mde_rediscovery_enrichment',
    'IP006_CONTROL_ASSURANCE_EVALUATION': 'ip006_control_assurance_evaluation',
}
RUN_REGISTRY = 'devresults.rcaip_assessment_run_registry'
PROFILE_REGISTRY = 'devresults.rcaip_post_profile_registry'
MAX_HISTORY = 100


class MetadataError(RuntimeError):
    """Sanitized fatal metadata/preparation failure."""


class MetadataEvidence:
    """Explicit private evidence container; repr never renders records or origins."""
    def __init__(self, evidence, report):
        self._enterprise_evidence = evidence
        self._report = report

    def __repr__(self):
        return '<MDE metadata evidence: enterprise-private; share status() only>'

    def status(self):
        return json.loads(json.dumps(self._report))


def _require(value, category):
    if not value: raise MetadataError(category)


def _name(value):
    _require(isinstance(value,str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,2}',value), 'INVALID_TABLE_BINDING')
    return '.'.join('`'+part+'`' for part in value.split('.'))


def _literal(value):
    return "CAST(unhex('"+value.encode('utf-8').hex()+"') AS STRING)"


def _step(stage, call):
    try: return call()
    except MetadataError: raise
    except (MemoryError,RecursionError): raise MemoryError('Metadata resource failure: '+stage) from None
    except KeyboardInterrupt: raise KeyboardInterrupt('Metadata interrupted: '+stage) from None
    except SystemExit: raise SystemExit('Metadata terminated: '+stage) from None
    except Exception: raise MetadataError('METADATA_OPERATION_FAILED: '+stage) from None


def _detail(spark, table):
    row=spark.sql('DESCRIBE DETAIL '+_name(table)).first()
    _require(row is not None,'TABLE_DETAIL_MISSING')
    data=row.asDict(recursive=True)
    _require(data.get('format')=='delta' and isinstance(data.get('id'),str) and data['id'],'DELTA_TABLE_REQUIRED')
    return data


def _registry(spark, table, column, value, fields):
    detail=_detail(spark,table)
    history=spark.sql('DESCRIBE HISTORY '+_name(table)+' LIMIT 1').first()
    _require(history is not None,'REGISTRY_HISTORY_MISSING')
    version=history['version']
    _require(type(version) is int and version>=0,'REGISTRY_VERSION_INVALID')
    # Pin CURRENT registry metadata acquisition; never call it historical input proof.
    frame=spark.read.format('delta').option('versionAsOf',version).table(table)
    _require(set(fields)<=set(frame.columns),'REGISTRY_SCHEMA_UNSUPPORTED')
    rows=frame.filter(column+' = '+_literal(value)).select(*fields).limit(2).collect()
    _require(len(rows)==1,'EXACT_REGISTRY_RECORD_MISSING_OR_AMBIGUOUS')
    record=rows[0].asDict(recursive=True)
    _require(record[column]==value,'REGISTRY_RECORD_SCOPE_MISMATCH')
    _require(_detail(spark,table)['id']==detail['id'],'REGISTRY_IDENTITY_CHANGED')
    return record,{'origin':table,'predicate_field':column,'predicate_value':value,
                   'metadata_snapshot_version':version,'detail':detail,
                   'history':history.asDict(recursive=True),'record':record,
                   'meaning':'CURRENT_REGISTRY_RECORD_NOT_HISTORICAL_PRODUCT_SNAPSHOT_CERTIFICATE'}


def _collect(spark, post, run, versions, expected_results_namespace):
    _require(isinstance(post,str) and re.fullmatch('[a-z0-9][a-z0-9_]*',post),'INVALID_POST')
    _require(isinstance(run,str) and bool(run.strip()),'INVALID_RUN')
    _require(isinstance(versions,dict) and set(versions)==set(STEMS),'EXPLICIT_THREE_PRODUCT_VERSION_REQUEST_REQUIRED')
    _require(all(v is None or type(v) is int and v>=0 for v in versions.values()),'INVALID_EXPLICIT_VERSION')
    run_row,run_evidence=_step('EXACT_RUN_REGISTRY',lambda:_registry(spark,RUN_REGISTRY,'run_id',run,
        ['run_id','post_profile_key','profile_hash','results_namespace','run_status','active']))
    key=run_row['post_profile_key'];namespace=run_row['results_namespace']
    _require(isinstance(expected_results_namespace,str) and namespace==expected_results_namespace,'UNAPPROVED_RESULTS_NAMESPACE')
    _require(isinstance(key,str) and bool(key) and isinstance(namespace,str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?',namespace),'REGISTRY_BINDING_FIELDS_INVALID')
    profile,profile_evidence=_step('EXACT_PROFILE_REGISTRY',lambda:_registry(spark,PROFILE_REGISTRY,'post_profile_key',key,
        ['post_profile_key','post_slug','profile_hash','profile_status','active']))
    _require(profile['post_slug']==post,'RUN_PROFILE_BINDING_MISMATCH')
    profile_relation = 'MATCH_NOT_HISTORICAL_INPUT_PROOF' if profile['profile_hash']==run_row['profile_hash'] else 'MISMATCH_REVIEW_REQUIRED'
    evidence={'registry_profile_hash_relation':profile_relation,'collected_at_utc':datetime.now(timezone.utc).isoformat(),'run_registry':run_evidence,'profile_registry':profile_evidence,'products':{}}
    statuses={}
    for logical,stem in STEMS.items():
        table=namespace+'.'+post+'_'+stem
        detail=_step('PRODUCT_DETAIL',lambda:_detail(spark,table))
        rows=_step('PRODUCT_HISTORY',lambda:spark.sql('DESCRIBE HISTORY '+_name(table)+' LIMIT '+str(MAX_HISTORY+1)).collect())
        _require(len(rows)<=MAX_HISTORY+1,'HISTORY_BOUND_VIOLATION')
        history=[row.asDict(recursive=True) for row in rows]
        seen=[r.get('version') for r in history]
        _require(all(type(v) is int and v>=0 for v in seen) and len(set(seen))==len(seen),'HISTORY_VERSION_INVALID')
        requested=versions[logical]
        record={'origin':table,'binding_basis':'CONFIG001_STEM_PLUS_CURRENT_EXACT_RUN_PROFILE',
                'detail':detail,'history':history,'history_query_not_truncated':len(rows)<=MAX_HISTORY,
                'requested_version':requested,'historical_assessment_association':'NOT_ESTABLISHED'}
        state='VERSION_NOT_SELECTED'
        if requested is not None:
            # Do not require history retention to inspect the explicitly requested
            # version's schema; history omission does not prove snapshot absence.
            schema=_step('EXPLICIT_VERSION_SCHEMA',lambda:spark.read.format('delta').option('versionAsOf',requested).table(table).schema.json())
            parsed=json.loads(schema)
            _require(isinstance(parsed,dict) and parsed.get('type')=='struct' and isinstance(parsed.get('fields'),list),'PRODUCT_SCHEMA_UNSUPPORTED')
            fields_json=json.dumps(parsed['fields'],ensure_ascii=True,separators=(',',':'))
            record.update(schema_json=schema,schema_fields_sha256=hashlib.sha256(fields_json.encode()).hexdigest(),
                          version_seen_in_history_window=requested in seen)
            state='EXPLICIT_SCHEMA_OBSERVED_NOT_DATA_READABILITY_CERTIFIED'
        after=_step('PRODUCT_IDENTITY_RECHECK',lambda:_detail(spark,table))
        _require(after['id']==detail['id'],'PRODUCT_IDENTITY_CHANGED')
        record['detail_after']=after
        evidence['products'][logical]=record
        statuses[logical]={'binding':'CURRENT_REGISTRY_AND_SOURCE_RULE_RESOLVED',
                           'delta_identity':'CURRENT_IDENTITY_OBSERVED',
                           'history':'BOUNDED_WINDOW' if len(rows)>MAX_HISTORY else 'RETURNED_HISTORY_ONLY',
                           'schema':state,'assessment_association':'UNESTABLISHED'}
    return MetadataEvidence(evidence,{'state':'METADATA_COLLECTED_NOT_BRIDGE_READY','registry_profile_hash_relation':profile_relation,'products':statuses,
        'witness_contract':'NO_HISTORICAL_INPUT_ASSOCIATION_WITNESS_ESTABLISHED',
        'next_action':'Enterprise review of original records; no automatic version selection, reproduction or retention'})


def collect_mde_input_metadata(*, spark, post, run, versions, expected_results_namespace):
    """Explicit metadata-only entry. No manifest, Config or DIAG runtime prerequisite.

    versions must explicitly name all three products; None means no version schema
    read. Inactive exact historical run records are allowed; never selects an active
    run or mutates authority. Missing/duplicate registry records STOP, with no search
    for similarly named tables. Never display _enterprise_evidence outside enterprise.
    """
    return _step('COLLECTION',lambda:_collect(spark,post,run,versions,expected_results_namespace))
