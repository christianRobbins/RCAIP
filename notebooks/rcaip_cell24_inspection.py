"""Metadata-only inspection in the ORIGINAL DIAG 001 Python namespace.

No Spark actions, configuration writes, producer imports or publication.
Source references: DIAG Cell 24 certification, sections 26 (READY/status) and
27 (preserve the three outputs). These describe the contract, not live evidence.
"""
from rcaip_cell24_presentation import (
    PRODUCTS, CASE_COLUMNS, MEMBER_COLUMNS, CLOSURE_COLUMNS, READY, STATUS, POST, RUN,
)


def inspect_cell24(namespace, *, expected_post, expected_run):
    """Return status metadata only; never return values, plans or unknown names.

    Matching metadata cannot certify original execution or retained lineage.
    No uniquely bound live certification/retention reference is established by
    this interface. Do not inspect generic notebook scratch certification vars:
    later cells can overwrite them. Review existing execution/retention records
    within the approved environment separately; do not invent their location.
    """
    required = dict(zip(PRODUCTS, (CASE_COLUMNS, MEMBER_COLUMNS, CLOSURE_COLUMNS)))
    result = {'products': {}, 'authority': {}, 'supported_prerequisites': [],
              'certification_retention': 'UNVERIFIED',
              'collection_authorized': False}
    supported = result['supported_prerequisites']
    checks = {READY: lambda v: v is True,
              STATUS: lambda v: type(v) is str and v == 'CLOSED_FOR_CURRENT_RUN',
              POST: lambda v: type(v) is str and v.strip().lower() == expected_post.strip().lower(),
              RUN: lambda v: type(v) is str and v == expected_run}
    for name, check in checks.items():
        state = 'ABSENT_FROM_THIS_SESSION' if name not in namespace else (
            'MATCH' if check(namespace[name]) else 'MISMATCH')
        result['authority'][name] = state
        if state == 'MATCH': supported.append(name)
    sessions, timestamp_present = [], False
    for kind, name in PRODUCTS.items():
        if name not in namespace:
            result['products'][name] = {'state': 'ABSENT_FROM_THIS_SESSION'}
            continue
        item = {'state': 'PRESENT_PROVENANCE_RETENTION_UNVERIFIED'}
        result['products'][name] = item
        frame = namespace[name]
        schema, session = getattr(frame, 'schema', None), getattr(frame, 'sparkSession', None)
        if schema is None or session is None:
            item['schema'] = 'UNAVAILABLE'
            continue
        fields = schema.fields
        names = [field.name for field in fields]
        item['missing_required_fields'] = [col for col in required[kind] if col not in names]
        item['schema'] = ('MATCH' if not item['missing_required_fields'] and
                          len(names) == len(set(names)) else 'MISMATCH')
        if item['schema'] == 'MATCH': supported.append(name + ': required field names')
        timestamp_present |= any(field.dataType.typeName() == 'timestamp' for field in fields)
        sessions.append(session)
    common = len(sessions) == len(PRODUCTS) and all(s is sessions[0] for s in sessions)
    result['common_session'] = 'SUPPORTED' if common else 'UNVERIFIED_OR_MISMATCH'
    result['utc_requirement'] = 'UNVERIFIED'
    if common:
        supported.append('three existing DataFrame sparkSession references: identical')
        zone = sessions[0].conf.get('spark.sql.session.timeZone')
        result['utc_requirement'] = ('SUPPORTED' if zone in ('UTC', 'Etc/UTC', 'Z', '+00:00')
                                     else 'UNSUPPORTED') if timestamp_present else 'NOT_APPLICABLE'
    result['scope'] = 'Namespace authority only; row-level scope was not inspected.'
    result['limitations'] = ('Absence here is not absence everywhere. Field names and session identity '
                            'do not prove type/value compatibility, original certification or safe '
                            'retained lineage. No cache flag or count is proof. Closure has no '
                            'row-level Post/run. Existing certification and retention evidence '
                            'must bind all three objects to the original certified result before '
                            'any collection; no replay or publication is authorized.')
    return result
