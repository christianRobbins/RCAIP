"""Bounded Cell 10/11 reproduction. No import-time reads, Spark or writes.

Witness selectors are an evidence adapter, NOT a new governance certificate.
The separately reviewed trust map must name actual registry, Delta history and
assessment input-binding records. Hashes identify those records, not their truth.
If the records do not establish the historical association, inspection must STOP.
Do not manufacture a witness to satisfy this transport contract.
"""
import ast
import base64
from collections import Counter
from copy import deepcopy
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import re

import rcaip_mde_retained_check as check

require = check.require
InputContractError = check.InputContractError
retained = check.retained
SEARCH = 'IP006_MDE_REDISCOVERY_SEARCH_INPUT'
ENRICHMENT = 'IP006_MDE_REDISCOVERY_ENRICHMENT'
CONTROL = 'IP006_CONTROL_ASSURANCE_EVALUATION'
INPUTS = (SEARCH, ENRICHMENT, CONTROL)
RULE_VERSION = 'CELL10_11_PINNED_REPRODUCTION_V1'
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 100000


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'JSON byte bound')
    def invalid(_):
        raise InputContractError('Nonfinite JSON value')
    try:
        return json.loads(raw.decode('utf-8-sig'), object_pairs_hook=retained._pairs,
                          parse_float=Decimal, parse_int=lambda s: Decimal(s) if s == '-0' else int(s),
                          parse_constant=invalid)
    except (UnicodeError, json.JSONDecodeError):
        raise InputContractError('Invalid JSON encoding') from None


def encoded(value):
    raw = retained._json(value).encode('utf-8')
    require(len(raw) <= MAX_BYTES, 'Derived/input bundle byte bound')
    return raw


def _pointer(document, path):
    require(isinstance(path, list), 'Witness selector required')
    for part in path:
        require((isinstance(document, dict) and isinstance(part, str) and part in document)
                or (isinstance(document, list) and type(part) is int and 0 <= part < len(document)),
                'Witness selector not established')
        document = document[part]
    return document


def _witness(spec, evidence, trust, role):
    require(isinstance(spec, dict) and set(spec) == {'reference', 'fields'}, 'Witness contract')
    ref = spec['reference']
    require(isinstance(ref, str) and ref in trust and ref in evidence, 'Historical provenance reference unavailable')
    approved = trust[ref]
    require(isinstance(approved, dict) and approved.get('role') == role
            and isinstance(approved.get('sha256'), str) and re.fullmatch('[0-9a-f]{64}', approved['sha256']),
            'Independently reviewed witness role/hash required')
    raw = evidence[ref]
    require(type(raw) is bytes and digest(raw) == approved['sha256'], 'Historical provenance witness hash mismatch')
    doc = strict_json(raw)
    require(isinstance(spec['fields'], dict), 'Witness field selectors required')
    return {field: _pointer(doc, selector) for field, selector in spec['fields'].items()}


def validate_bindings(manifest, evidence, trusted_references):
    """No data reads. Validate caller bindings AGAINST independently reviewed records.

    trust is supplied separately by the reviewer/operator, never copied from the
    manifest. It must anchor actual records with semantics supporting each role.
    This cannot cryptographically establish the authenticity of arbitrary bytes.
    """
    require(isinstance(manifest, dict) and set(manifest) == {'post', 'run', 'retained_sha256', 'inputs'}, 'Manifest contract')
    require(isinstance(manifest['post'], str) and re.fullmatch('[a-z0-9][a-z0-9_-]*', manifest['post'])
            and isinstance(manifest['run'], str) and manifest['run'].strip(), 'Manifest scope')
    require(isinstance(manifest['retained_sha256'], str) and re.fullmatch('[0-9a-f]{64}', manifest['retained_sha256']), 'Retained hash required')
    require(isinstance(manifest['inputs'], dict) and set(manifest['inputs']) == set(INPUTS), 'Exact durable input set required')
    for logical in INPUTS:
        binding = manifest['inputs'][logical]
        require(isinstance(binding, dict) and set(binding) == {'table', 'table_id', 'version', 'schema_sha256', 'registry', 'snapshot', 'assessment'}, 'Input binding contract')
        require(isinstance(binding['table'], str) and re.fullmatch(r'[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*){1,2}', binding['table']), 'Registered physical table required')
        require(isinstance(binding['table_id'], str) and binding['table_id'] and type(binding['version']) is int and binding['version'] >= 0, 'Historical Delta identity/version required')
        require(isinstance(binding['schema_sha256'], str) and re.fullmatch('[0-9a-f]{64}', binding['schema_sha256']), 'Snapshot schema fingerprint required')
        registry = _witness(binding['registry'], evidence, trusted_references, 'registry')
        require(registry == {'logical_product': logical, 'physical_table': binding['table']}, 'Registry binding mismatch')
        identity = {'physical_table': binding['table'], 'table_id': binding['table_id'], 'version': binding['version'],
                    'schema_sha256': binding['schema_sha256'], 'format': 'delta', 'object_type': 'TABLE'}
        require(_witness(binding['snapshot'], evidence, trusted_references, 'snapshot') == identity, 'Historical snapshot identity mismatch')
        association = dict(identity, logical_product=logical, post=manifest['post'], run=manifest['run'],
                           retained_sha256=manifest['retained_sha256'], producer_fingerprints={str(n): h for n, h in check.FINGERPRINTS.items()})
        require(_witness(binding['assessment'], evidence, trusted_references, 'assessment_input_binding') == association,
                'Historical assessment/input association not established')
    return deepcopy(manifest)


def inspect_inputs(manifest, evidence, trusted_references):
    """Operation A: sanitized provenance readiness only; never reads/reproduces rows."""
    try:
        validate_bindings(manifest, evidence, trusted_references)
    except InputContractError as exc:
        return {'state': 'INPUT_PROVENANCE_NOT_ESTABLISHED', 'boundary': str(exc),
                'reproduction': 'NOT_RUN', 'retention': 'NOT_RUN'}
    return {'state': 'PROVENANCE_RECORDS_MATCH', 'schema_availability': 'REQUIRES_PINNED_READ',
            'reproduction': 'NOT_RUN', 'retention': 'NOT_RUN',
            'authority': 'Reviewed historical records only; not live assessment or availability certification'}


def _records(payload):
    cols, kinds = check._table(payload)
    rows = payload.get('data')
    require(payload.get('overflow') is False and isinstance(rows, list) and len(rows) <= MAX_ROWS, 'Incomplete/bounded structured product')
    result = []
    for values in rows:
        require(isinstance(values, list) and len(values) == len(cols), 'Positional row contract')
        for value, kind in zip(values, kinds): retained._typed(value, kind)
        result.append(dict(zip(cols, values)))
    encoded(payload)
    return result


def primary_summary_columns(source, number):
    """Complete schema/grain of the named primary display in the pinned cell."""
    name = 'mde_qualification_funnel_df' if number == 10 else 'mde_current_gate_summary_df'
    require(retained.producer_fingerprint(source) == check.FINGERPRINTS[number], 'Primary summary producer mismatch')
    node = check._assignments(source)[name]
    groups = check._calls(node, 'groupBy')
    aggregates = check._calls(node, 'agg')
    require(len(groups) == len(aggregates) == 1, 'Primary summary source contract')
    columns = [ast.literal_eval(a) for a in groups[0].args]
    columns += [check._column(a) for a in aggregates[0].args]
    columns += [ast.literal_eval(c.args[0]) for c in check._calls(node, 'withColumn')]
    return set(columns)


def load_original(raw, manifest):
    """Read original summaries/cases for reconciliation, NOT repeat availability test."""
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES and digest(raw) == manifest['retained_sha256'], 'Original retained hash/byte bound')
    def invalid(_): raise InputContractError('Nonfinite original JSON')
    try:
        doc = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=retained._ObjectPairs,
                         parse_float=Decimal, parse_int=lambda s: Decimal(s) if s == '-0' else int(s), parse_constant=invalid)
    except (UnicodeError, json.JSONDecodeError):
        raise InputContractError('Invalid original JSON') from None
    doc = retained._resolve_notebook_json(doc, (), [])
    require(isinstance(doc, dict) and doc.get('nbformat') == 4 and isinstance(doc.get('cells'), list), 'Original notebook envelope')
    sources, certs, tables = {}, {}, {}
    for cell in doc['cells']:
        require(isinstance(cell, dict), 'Original cell envelope')
        if cell.get('cell_type') != 'code': continue
        source = retained._joined(cell.get('source', ''))
        for n in check.PRODUCTS:
            if check._marker(n) not in source: continue
            require(n not in sources, 'Ambiguous original producer')
            try: fingerprint = retained.producer_fingerprint(source)
            except SyntaxError: raise InputContractError('Original producer syntax') from None
            require(fingerprint == check.FINGERPRINTS[n], 'Original producer fingerprint mismatch')
            outputs = cell.get('outputs')
            require(isinstance(outputs, list), 'Original outputs required')
            streams, tables[n] = [], []
            for out in outputs:
                require(isinstance(out, dict) and out.get('output_type') in ('stream', 'display_data', 'execute_result'), 'Original error/unsupported output')
                if out['output_type'] == 'stream':
                    require(out.get('name') == 'stdout', 'Original certification stream')
                    streams.append(retained._joined(out.get('text', '')))
                metadata = out.get('metadata', {})
                require(isinstance(metadata, dict), 'Original metadata envelope')
                if retained.MIME in metadata: tables[n].append(metadata[retained.MIME])
            cert = ''.join(streams)
            require(cert.count(check._marker(n)) == 1 and check._field(cert, 'Status') == 'PASS', 'Original certification PASS required')
            require(check._field(cert, 'Post').lower() == manifest['post'] and check._field(cert, 'Run ID') == manifest['run'], 'Original certification scope')
            sources[n], certs[n] = source, cert
    require(set(sources) == set(check.PRODUCTS), 'Original producer/certification missing')
    columns = check.contracts(sources)
    products, payloads = {}, {}
    for name in check.PRODUCTS[14]:
        matches = [t for t in tables[14] if set(columns[name]) <= set(check._table(t)[0])]
        require(len(matches) <= 1, 'Ambiguous original native product')
        if not matches:
            require(name in check.OPTIONAL and check._count(certs[14], check.OPTIONAL[name]) == 0, 'Original native product missing')
            products[name] = []
        else:
            products[name] = _records(matches[0]); payloads[name] = matches[0]
    summaries = {}
    for n, field in ((10, 'mde_qualification_cohort'), (11, 'mde_current_analytical_gate')):
        required = primary_summary_columns(sources[n], n)
        matches = [t for t in tables[n] if required == set(check._table(t)[0])]
        require(len(matches) == 1, 'Original Cell 10/11 grouped summary missing/ambiguous')
        summaries[n] = _records(matches[0])
    return sources, certs, products, payloads, summaries


def _projection(source, variable):
    node = check._assignments(source)[variable]
    while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == 'select': break
        node = node.func.value
    require(isinstance(node, ast.Call) and node.func.attr == 'select', 'Pinned projection missing')
    result = {}
    for col in node.args:
        if isinstance(col, ast.Constant): result[col.value] = col.value
        else:
            require(col.func.attr == 'alias' and col.func.value.func.attr == 'col', 'Pinned projection unsupported')
            result[ast.literal_eval(col.args[0])] = ast.literal_eval(col.func.value.args[0])
    return result


def qualification(row, source):
    """Port of Cell 10 conditions; SQL NULL NOT is not Python inequality."""
    r = deepcopy(row); v = r['rediscovery_visibility_class']; life = r['lifecycle_case_state']
    if v in ('CURRENT_RESOLVED', 'CURRENT_DURABLE_DEVICE_CHURN'): code = 'CURRENT_MDE_EVIDENCE_CONFIRMED'
    elif life == 'CURRENTLY_OBSERVED' and v == 'IDENTITY_OR_TEMPORAL_REVIEW_REQUIRED': code = 'CURRENT_IDENTITY_TEMPORAL_REVIEW'
    elif life == 'CURRENTLY_OBSERVED' and v in ('STALE_REDISCOVERY', 'HISTORICAL_REDISCOVERY'): code = 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'
    elif life == 'CURRENTLY_OBSERVED' and v in ('INSUFFICIENT_IDENTITY_FOR_LOOKBACK', 'NO_ENTERPRISE_NAME_HIT'): code = 'CURRENT_IDENTITY_EVIDENCE_LIMITATION'
    elif life is not None and life != 'CURRENTLY_OBSERVED': code = 'NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION'
    else: code = 'UNMAPPED_REDISCOVERY_STATE'
    r['mde_qualification_cohort'] = code
    require(code != 'UNMAPPED_REDISCOVERY_STATE', 'Cell 10 unmapped reproduction')
    for field in ('mde_qualification_meaning', 'mde_next_gate'):
        r[field] = check._decision_pair(source, 'mde_qualification_df', 'mde_qualification_cohort', field)[code]
    r['potential_current_control_review'] = code == 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'
    r['current_identity_review_required'] = code in ('CURRENT_IDENTITY_TEMPORAL_REVIEW', 'CURRENT_IDENTITY_EVIDENCE_LIMITATION')
    return r


def gate_decision(r):
    """Cell 11 ordered conditions only, not Cell 14 residual predicates."""
    app = r['ca_mde_applicability_state']; applicable = isinstance(app, str) and app.startswith('APPLICABLE')
    if app == 'NOT_APPLICABLE': return 'MDE_NOT_APPLICABLE_BY_DEVICE_SCOPE'
    if 'REVIEW_REQUIRED' in (app or ''): return 'MDE_APPLICABILITY_REVIEW_REQUIRED'
    if r['mde_qualification_cohort'] == 'CURRENT_IDENTITY_TEMPORAL_REVIEW': return 'MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED'
    if applicable and r['ca_identity_strength'] in ('WEAK', 'UNKNOWN'): return 'MDE_IDENTITY_CONFIDENCE_REVIEW_REQUIRED'
    if applicable and r['ca_attribution_conflict_flag'] is True: return 'MDE_ATTRIBUTION_CONFLICT_REVIEW_REQUIRED'
    if applicable and (r['ca_operational_boundary_confidence'] != 'HIGH' or r['ca_management_model'] in (None, 'UNKNOWN', 'SHARED', 'NON_ASSET')): return 'MDE_OWNERSHIP_ATTRIBUTION_REVIEW_REQUIRED'
    if applicable and r['mde_qualification_cohort'] == 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY':
        if r['ca_management_model'] == 'POST_MANAGED' or r['ca_management_owner'] == 'POST': return 'CURRENT_APPLICABLE_POST_MDE_CONTROL_REVIEW_CANDIDATE'
        if r['ca_management_model'] == 'ENTERPRISE_MANAGED' or r['ca_management_owner'] == 'ENTERPRISE': return check.RESIDUAL_GATE
    return 'MDE_GATE_MODEL_REVIEW_REQUIRED'


def reproduce_members(enrichment, control, sources):
    """Pure, separately testable bounded transformation; no reads or writes."""
    for n in (10, 11): require(retained.producer_fingerprint(sources[n]) == check.FINGERPRINTS[n], 'Reproduction rule fingerprint mismatch')
    q = [qualification(r, sources[10]) for r in enrichment]
    ci = check._index(control, 'asset_key', 'Control context')
    alias = _projection(sources[11], 'current_unresolved_rediscovery_df')
    ca = _projection(sources[11], 'mde_control_context_df')
    meanings = check._decision_pair(sources[11], 'mde_current_gate_df', 'mde_current_analytical_gate', 'mde_current_gate_meaning')
    current = [r for r in q if r['mde_qualification_cohort'] in check.GATE_COHORTS]
    require(all(r['asset_key'] in ci and ci[r['asset_key']]['applicability_state'] is not None for r in current),
            'Cell 11 required MDE Control Assurance context missing')
    gates = []
    for row in q:
        if row['mde_qualification_cohort'] not in check.GATE_COHORTS: continue
        require(row['asset_key'] in ci, 'Missing candidate MDE control context')
        r = {out: deepcopy(row[original]) for out, original in alias.items()}
        r.update({out: deepcopy(ci[row['asset_key']][original]) for out, original in ca.items()})
        code = gate_decision(r)
        require(code in meanings, 'Cell 11 unmapped reproduction')
        r.update(mde_current_analytical_gate=code, mde_current_gate_meaning=meanings[code]); gates.append(r)
    return q, gates, {'rediscovery': alias, 'control': ca}


def _validate_summaries(rows, saved, n):
    field = 'mde_qualification_cohort' if n == 10 else 'mde_current_analytical_gate'
    index = check._index(saved, field, 'Original grouped summary')
    counts = Counter(r[field] for r in rows)
    require(index.keys() == counts.keys(), 'Original grouped summary keys mismatch')
    for code, count in counts.items():
        record = index[code]
        require(type(record['assets']) is int and record['assets'] == count, 'Original grouped summary assets mismatch')
        selected = [r for r in rows if r[field] == code]
        meaning = 'mde_qualification_meaning' if n == 10 else 'mde_current_gate_meaning'
        require(record.get(meaning) == selected[0][meaning], 'Original grouped summary meaning mismatch')
        if n == 10:
            require(record.get('mde_next_gate') == selected[0]['mde_next_gate'], 'Original grouped summary next gate mismatch')
            for prefix in ('resolved', 'current', 'stale', 'historical'):
                require(record.get(prefix + '_mde_entities') == sum(r[prefix + '_mde_entity_count'] or 0 for r in selected), 'Original grouped summary entities mismatch')
        pct = 'share_of_mde_missing_pct' if n == 10 else 'share_of_current_unresolved_pct'
        require(pct in record and record[pct] == (Decimal(count) * 100 / len(rows)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP), 'Original grouped summary percentage mismatch')


def _native_reconcile(q, gates, products, certs, sources):
    start = check._check_qualification(q, certs[10], sources[10], check._field(certs[10], 'Post').lower(), check._field(certs[10], 'Run ID'))
    gate = check._check_gate(gates, certs[11], sources[11])
    require(gate.keys() == {k for k, r in start.items() if r['mde_qualification_cohort'] in check.GATE_COHORTS}, 'Reproduced current subset mismatch')
    check._check_native_decisions(products, sources[14])
    definitions = check._assignments(sources[14])['compression_rows'].elts
    for row in products[check.PRODUCTS[14][4]]:
        order = row['stage_order']
        require(type(order) is int and 1 <= order <= len(definitions), 'Accounting stage order')
        require(row['stage'] == ast.literal_eval(definitions[order-1].elts[1]) and row['interpretation'] == ast.literal_eval(definitions[order-1].elts[3]), 'Accounting stage meaning')
    mi, ri, stages = check._check_cases(products, certs[14])
    require((mi.keys() | ri.keys()) == {k for k in gate if check._residual_evidence(start[k], gate[k])}, 'Original residual membership mismatch')
    for key, member in {**mi, **ri}.items():
        check._same(start[key], member, 'Reproduced qualification/original member')
        check._same(gate[key], member, 'Reproduced gate/original member', [('rediscovery_'+f, f) for f in ('canonical_name', 'canonical_serial', 'canonical_ip')])
    case_index = {c['case_id']: c for c in products[check.PRODUCTS[14][0]]}
    for key, member in mi.items(): check._same(gate[key], case_index[member['case_id']], 'Reproduced gate/original case')
    require(stages[1]['count'] == len(start), 'Original starting accounting mismatch')
    for stage, code in ((2, 'CURRENT_MDE_EVIDENCE_CONFIRMED'), (3, 'NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION')):
        require(stages[stage]['count'] == sum(r['mde_qualification_cohort'] == code for r in q), 'Original outcome accounting mismatch')
    check._check_shared_support(start, mi, products[check.PRODUCTS[14][2]], sources[14])


def reproduce(manifest, evidence, trusted_references, inputs, original_raw):
    """Operation B on explicit materialized snapshot envelopes; no automatic fallback."""
    m = validate_bindings(manifest, evidence, trusted_references)
    core = _reproduce_bound(m, inputs, original_raw)
    bundle = {'format': 'RCAIP_MDE_DERIVED_REPORT_INPUT_V1',
              'authority': 'HISTORICAL_REPRODUCTION_NOT_LIVE_CERTIFICATION'}
    for key, value in core.items():
        bundle[key] = value
        if key == 'manifest':
            bundle['evidence'] = {ref: base64.b64encode(raw).decode('ascii') for ref, raw in evidence.items()}
            bundle['trusted_references'] = deepcopy(trusted_references)
    encoded(bundle)
    return bundle


def _reproduce_bound(m, inputs, original_raw):
    """Shared analytical core. Caller must establish its explicit provenance route.

    This is not a historical attestation entry point. All original source, input,
    membership, alias, required-context and retained-result checks remain here.
    """
    require(isinstance(inputs, dict) and set(inputs) == set(INPUTS), 'Complete materialized inputs required')
    sources, certs, products, originals, summaries = load_original(original_raw, m)
    records = {}
    for name in INPUTS:
        item = inputs[name]; b = m['inputs'][name]
        require(isinstance(item, dict) and set(item) == {'table', 'table_id', 'version', 'payload'}, 'Snapshot envelope contract')
        require(all(item[k] == b[k] for k in ('table', 'table_id', 'version')), 'Materialized snapshot identity mismatch')
        require(digest(encoded(item['payload']['schema'])) == b['schema_sha256'], 'Materialized schema fingerprint mismatch')
        records[name] = _records(item['payload'])
        for row in records[name]:
            require(row.get('post_slug') == m['post'] and row.get('rcaip_run_id') == m['run'], 'Materialized input scope mismatch')
    require({'asset_key', 'post_slug', 'rcaip_run_id', 'search_eligibility_class', 'search_eligible'} <= set(check._table(inputs[SEARCH]['payload'])[0]), 'Search authority schema contract')
    require(records[ENRICHMENT], 'Cell 10 requires nonempty historical population')
    si = check._index(records[SEARCH], 'asset_key', 'Search authority')
    ei = check._index(records[ENRICHMENT], 'asset_key', 'Enrichment')
    ci = check._index(records[CONTROL], 'asset_key', 'Control context')
    require(si.keys() == ei.keys(), 'Search/enrichment exact membership mismatch')
    require(ci.keys() <= si.keys(), 'Foreign control candidate')
    for key in si: check._same(si[key], ei[key], 'Search/enrichment')
    a = check._assignments(sources[11])
    needed = set(check.contracts(sources)[check.PRODUCTS[10][0]]) - {'mde_qualification_cohort', 'mde_qualification_meaning', 'mde_next_gate', 'potential_current_control_review', 'current_identity_review_required'}
    require(needed <= set(check._table(inputs[ENRICHMENT]['payload'])[0]), 'Enrichment source contract fields missing')
    require(set(ast.literal_eval(a['CONTROL_REQUIRED_COLUMNS'])) <= set(check._table(inputs[CONTROL]['payload'])[0]), 'Control source contract fields missing')
    for row in records[CONTROL]: require(isinstance(row['tool_name'], str) and row['tool_name'].strip(' ').upper() == 'MDE', 'Control asset/tool grain')
    q, gates, aliases = reproduce_members(records[ENRICHMENT], records[CONTROL], sources)
    # Current-subset guard, NOT the full-Post control table denominator.
    check._equal_count(certs[11], 'Missing MDE Control Assurance context',
                       sum(r['ca_mde_applicability_state'] is None for r in gates))
    check._equal_count(certs[11], 'Rediscovery vs Control Assurance ownership/boundary mismatches',
                       sum((r['rediscovery_management_owner'] or '') != (r['ca_management_owner'] or '')
                           or (r['rediscovery_operational_boundary'] or '') != (r['ca_operational_boundary'] or '') for r in gates))
    require(gates, 'Cell 11 requires nonempty current unresolved population')
    _native_reconcile(q, gates, products, certs, sources)
    for n, rows in ((10, q), (11, gates)): _validate_summaries(rows, summaries[n], n)
    bundle = {'rule_version': RULE_VERSION, 'producer_fingerprints': {str(n): h for n, h in check.FINGERPRINTS.items()},
              'manifest': m, 'inputs': deepcopy(inputs),
              'original_notebook_sha256': digest(original_raw), 'original_native_payloads': originals,
              'original_certifications': {str(k): v for k, v in certs.items()}, 'original_summaries': {str(k): v for k, v in summaries.items()},
              'original_sources': {str(k): v for k, v in sources.items()}, 'original_products': products,
              'original_kind': 'ORIGINAL_RETAINED', 'member_map_kind': 'REPRODUCED',
              'qualification': q, 'current_gate': gates, 'aliases': aliases,
              'cmdb_pairing': 'UNESTABLISHED: governed paired assignments, identity link, times and attribution meanings required'}
    enrichment_schema = inputs[ENRICHMENT]['payload']['schema']
    require(not any(f['name'] in ('mde_qualification_cohort', 'mde_qualification_meaning', 'mde_next_gate', 'potential_current_control_review', 'current_identity_review_required') for f in enrichment_schema), 'Enrichment contains unexpected reproduced decisions')
    qschema = deepcopy(enrichment_schema) + [{'name': f, 'type': 'boolean' if f.endswith('_review') or f == 'current_identity_review_required' else 'string'} for f in ('mde_qualification_cohort', 'mde_qualification_meaning', 'mde_next_gate', 'potential_current_control_review', 'current_identity_review_required')]
    source_schemas = {'rediscovery': {f['name']: f for f in qschema}, 'control': {f['name']: f for f in inputs[CONTROL]['payload']['schema']}}
    gschema = {}
    for origin, mapping in aliases.items():
        for out, original in mapping.items(): gschema[out] = dict(deepcopy(source_schemas[origin][original]), name=out)
    bundle['derived_schemas'] = {'qualification': qschema, 'current_gate': list(gschema.values()) + [{'name': f, 'type': 'string'} for f in ('mde_current_analytical_gate', 'mde_current_gate_meaning')]}
    bundle['input_payload_sha256'] = {name: digest(encoded(inputs[name]['payload'])) for name in INPUTS}
    for product, schema in bundle['derived_schemas'].items():
        _records({'schema': schema, 'data': [[r[f['name']] for f in schema] for r in bundle[product]], 'overflow': False})
    encoded(bundle)
    return bundle


def validate_bundle(bundle, trusted_references, original_raw):
    """In-memory semantic consistency ONLY; use load_bundle for saved-file identity."""
    require(isinstance(bundle, dict) and bundle.get('format') == 'RCAIP_MDE_DERIVED_REPORT_INPUT_V1', 'Derived bundle contract')
    require(bundle.get('trusted_references') == trusted_references, 'External trust references changed')
    expected = reproduce(bundle['manifest'], {k: base64.b64decode(v, validate=True) for k, v in bundle['evidence'].items()},
                         trusted_references, bundle['inputs'], original_raw)
    require(encoded(expected) == encoded(bundle), 'Derived bundle content/provenance mismatch')
    return bundle


def load_bundle_bytes(raw, *, expected_sha256, trusted_references, original_raw):
    """External receipt hash first; embedded hashes are never file identity authority."""
    require(isinstance(expected_sha256, str) and re.fullmatch('[0-9a-f]{64}', expected_sha256), 'External saved-bundle SHA-256 required')
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'Saved-bundle byte bound')
    require(digest(raw) == expected_sha256, 'Saved-bundle external SHA-256 mismatch')
    return validate_bundle(strict_json(raw), trusted_references, original_raw)


def load_bundle(path, *, expected_sha256, trusted_references, original_raw):
    """Explicit later consumption of one saved file, using a separately recorded hash."""
    with Path(path).open('rb') as stream: raw = stream.read(MAX_BYTES + 1)
    return load_bundle_bytes(raw, expected_sha256=expected_sha256,
                             trusted_references=trusted_references, original_raw=original_raw)


def retain_bundle(bundle, destination, *, approved_directory, trusted_references, original_raw):
    """Explicit new-file retention; external approval selects a new reporting path."""
    validate_bundle(bundle, trusted_references, original_raw)
    dest = Path(destination).resolve(); root = Path(approved_directory).resolve()
    require(root.is_dir() and dest.parent == root and dest.suffix == '.json' and not dest.exists(), 'Approved new report-input destination required')
    raw = encoded(bundle)
    with dest.open('xb') as stream: stream.write(raw)
    with dest.open('rb') as stream: reopened = stream.read(MAX_BYTES + 1)
    require(reopened == raw and encoded(strict_json(reopened)) == raw, 'Derived bundle round-trip mismatch')
    load_bundle_bytes(reopened, expected_sha256=digest(raw), trusted_references=trusted_references, original_raw=original_raw)
    return {'state': 'DERIVED_REPORT_INPUT_RETAINED', 'sha256': digest(raw),
            'authority': 'Historical reproduction; not workbook or live certification'}
