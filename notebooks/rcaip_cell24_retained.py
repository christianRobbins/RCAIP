"""Read retained Cell 24 notebook results; never execute captured source or Spark.

Supported table envelope: output.metadata[MIME] with schema (Spark struct or
name/type field list), positional data rows and explicit overflow=false.
Unsupported export dialects fail closed; HTML is never a data source.
"""
import ast
from decimal import Decimal
import hashlib
import inspect
import json
from pathlib import Path
import re

import rcaip_cell24_presentation as consumer

MIME = 'application/vnd.databricks.v1+output'
MARKER = 'RCAIP DIAG 001 - CELL 24 CERTIFICATION'
# Executable AST of the repository Cell 24 producer; comments/location excluded.
PRODUCER_AST_SHA256 = '96b5d6054c0f24b038e2563319dc35beffc75c8f40b71858f410fa8369800412'
require = consumer.require


def _joined(value):
    require(isinstance(value, str) or isinstance(value, list) and
            all(isinstance(x, str) for x in value), 'Unsupported notebook text encoding')
    return value if isinstance(value, str) else ''.join(value)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'Duplicate JSON key')
        result[key] = value
    return result


class _ObjectPairs(list):
    """Keep JSON objects distinct from arrays until structural validation."""


def _resolve_notebook_json(value, path, redundant_outputs):
    if isinstance(value, _ObjectPairs):
        grouped = {}
        for key, item in value:
            grouped.setdefault(key, []).append(item)
        result = {}
        for key, items in grouped.items():
            if len(items) != 1:
                allowed = (len(path) == 4 and path[0] == 'cells' and
                           type(path[1]) is int and path[2] == 'outputs' and
                           type(path[3]) is int and key == 'output_type' and
                           len(items) == 2 and all(type(v) is str for v in items) and
                           items[0] == items[1] and items[0] in
                           ('stream', 'display_data', 'execute_result', 'error'))
                require(allowed, 'Duplicate JSON key outside permitted output_type pair')
                redundant_outputs.append(f'$.cells[{path[1]}].outputs[{path[3]}]')
            result[key] = _resolve_notebook_json(items[0], path + (key,), redundant_outputs)
        return result
    if isinstance(value, list):
        return [_resolve_notebook_json(item, path + (index,), redundant_outputs)
                for index, item in enumerate(value)]
    return value


def _json(value):
    """Serialize parsed metadata without rounding Decimal JSON numbers."""
    if isinstance(value, Decimal): return str(value)
    if isinstance(value, list): return '[' + ','.join(map(_json, value)) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + ':' + _json(v) for k, v in value.items()) + '}'
    return json.dumps(value, ensure_ascii=True, allow_nan=False)


def _typed(value, kind):
    supported = (isinstance(kind, dict) and kind.get('type') == 'array' and kind.get('elementType') == 'string'
                 or isinstance(kind, str) and (kind in ('string', 'timestamp', 'timestamp_ntz', 'date',
                    'byte', 'short', 'integer', 'long', 'boolean', 'float', 'double')
                    or re.fullmatch(r'decimal\(\d+,\d+\)', kind)))
    require(supported, 'Unsupported exported schema type')
    if value is None: return
    if isinstance(kind, dict) and kind.get('type') == 'array':
        require(kind.get('elementType') == 'string' and isinstance(value, list) and
                all(v is None or isinstance(v, str) for v in value), 'Unsupported array transport')
    elif kind in ('string', 'timestamp', 'timestamp_ntz', 'date'):
        require(isinstance(value, str), 'Text/temporal payload must remain exported text')
    elif kind in ('byte', 'short', 'integer', 'long'):
        require(type(value) is int, 'Integer payload type mismatch')
    elif kind == 'boolean':
        require(type(value) is bool, 'Boolean payload type mismatch')
    elif isinstance(kind, str) and (kind in ('float', 'double') or re.fullmatch(r'decimal\(\d+,\d+\)', kind)):
        require(type(value) is int or isinstance(value, Decimal), 'Unsupported numeric payload encoding')
        require(Decimal(value).is_finite(), 'Nonfinite evidence')
    else:
        raise consumer.InputContractError('Unsupported exported schema type')


def _schema_type(kind):
    """Decode one exported JSON descriptor layer, never evidence values."""
    if isinstance(kind, str) and kind.lstrip().startswith(('"', '{', '[')):
        try:
            kind = json.loads(kind, object_pairs_hook=_pairs)
        except json.JSONDecodeError as exc:
            raise consumer.InputContractError('Malformed JSON schema descriptor') from exc
    _typed(None, kind)  # Recursively encoded and unsupported descriptors fail here.
    return kind


def producer_fingerprint(source):
    """Retain empty AST lists explicitly on 3.13+; older dump retains them.

    Always compare this representation with the reviewed reference fingerprint.
    Captured source is parsed only, never compiled for execution or executed.
    """
    options = {'show_empty': True} if 'show_empty' in inspect.signature(ast.dump).parameters else {}
    representation = ast.dump(ast.parse(source.strip()), annotate_fields=True,
                              include_attributes=False, indent=None, **options)
    return hashlib.sha256(representation.encode('utf-8')).hexdigest()


def load_retained_cell24(path, *, expected_post, expected_run, max_bytes=64 * 1024 * 1024):
    """Local artifact only. Returns validated historical materialized evidence.

    READY in the private validation mapping represents saved PASS only. It is not
    written into any live namespace and is never evidence of current readiness.
    """
    path = Path(path)
    require(path.suffix.lower() == '.ipynb', 'Explicit local .ipynb path required')
    require(type(max_bytes) is int and 0 < max_bytes <= 64 * 1024 * 1024, 'Invalid byte bound')
    with path.open('rb') as stream: raw = stream.read(max_bytes + 1)
    require(len(raw) <= max_bytes, 'Notebook exceeds byte bound; no truncation')
    def invalid(_): raise consumer.InputContractError('Nonfinite JSON number')
    book = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=_ObjectPairs,
                      parse_float=Decimal, parse_int=lambda s: Decimal(s) if s == '-0' else int(s),
                      parse_constant=invalid)
    redundant_outputs = []
    book = _resolve_notebook_json(book, (), redundant_outputs)
    require(isinstance(book, dict) and book.get('nbformat') == 4 and isinstance(book.get('cells'), list),
            'Unsupported notebook envelope')
    candidates = []
    for cell in book['cells']:
        require(isinstance(cell, dict), 'Invalid notebook cell')
        source = _joined(cell.get('source', ''))
        if cell.get('cell_type') == 'code' and (MARKER in source or
                all(name in source for name in (*consumer.PRODUCTS.values(), consumer.READY))):
            candidates.append((cell, source))
    require(len(candidates) == 1, 'Missing or ambiguous Cell 24 producer source')
    cell, source = candidates[0]
    digest = producer_fingerprint(source)
    require(digest == PRODUCER_AST_SHA256, 'Unexpected Cell 24 executable source')
    outputs = cell.get('outputs')
    require(isinstance(outputs, list), 'Missing saved cell outputs')
    products, streams = {}, []
    schemas = {'cases': list(consumer.CASE_COLUMNS), 'members': list(consumer.MEMBER_COLUMNS),
               'closure': list(consumer.CLOSURE_COLUMNS)}
    for output in outputs:
        require(isinstance(output, dict) and output.get('output_type') != 'error', 'Saved cell error/invalid output')
        if output.get('output_type') == 'stream':
            require(output.get('name') == 'stdout', 'Unexpected certification stream')
            streams.append(_joined(output.get('text', '')))
        payload = output.get('metadata', {}).get(MIME)
        if payload is None: continue
        require(isinstance(payload, dict) and payload.get('overflow') is False,
                'Missing/overflowed/unsupported structured output')
        schema = payload.get('schema')
        if isinstance(schema, dict):
            require(schema.get('type') == 'struct', 'Unsupported table schema')
            schema = schema.get('fields')
        require(isinstance(schema, list) and all(isinstance(f, dict) and
                isinstance(f.get('name'), str) and 'type' in f for f in schema), 'Unsupported table fields')
        columns = [f['name'] for f in schema]
        schema = [{**field, 'type': _schema_type(field['type'])} for field in schema]
        matches = [kind for kind, cols in schemas.items() if columns == cols]
        require(len(matches) == 1 and matches[0] not in products, 'Unexpected/duplicate product schema')
        rows = payload.get('data')
        require(isinstance(rows, list) and len(rows) <= 100000, 'Missing/oversized table data')
        records = []
        for row in rows:
            require(isinstance(row, list) and len(row) == len(columns), 'Malformed positional table row')
            for value, field in zip(row, schema): _typed(value, field['type'])
            records.append(dict(zip(columns, row)))
        products[matches[0]] = {'columns': columns, 'rows': records}
    require(set(products) == set(schemas), 'Three distinct same-cell products required')
    saved = ''.join(streams)
    require(saved.count(MARKER) == 1, 'Missing/ambiguous saved certification')
    def line(label):
        found = re.findall(r'^' + re.escape(label) + r':\s*([^\r\n]+)$', saved, flags=re.M)
        require(len(found) == 1, 'Missing/ambiguous certification field: ' + label)
        return found[0].strip()
    require(line('Status') == 'PASS', 'Saved certification is not PASS')
    require(line('Unresolved-IP branch closure state') == 'CLOSED_FOR_CURRENT_RUN', 'Saved branch not closed')
    require(line('Post').lower() == expected_post.strip().lower(), 'Saved certification Post mismatch')
    require(line('Run ID') == expected_run, 'Saved certification run mismatch')
    # Private compatibility mapping: historical PASS, never live-session authority.
    materialized = {consumer.READY: True, consumer.STATUS: 'CLOSED_FOR_CURRENT_RUN',
                    consumer.POST: expected_post, consumer.RUN: expected_run,
                    **{consumer.PRODUCTS[k]: v for k, v in products.items()}}
    data = consumer.validate_cell24(materialized, expected_post, expected_run)
    cases, members = products['cases']['rows'], products['members']['rows']
    stages = {r['stage_order']: r['count'] for r in products['closure']['rows']}
    totals = {'Starting active unresolved-IP analytical asset rows': stages[1],
              'Tenable source-local identity groups': stages[2],
              'Stronger source-local UUID relationship groups': stages[3],
              'Stronger exact-IP relationship groups': stages[4],
              'True no-link Tenable identity groups': stages[5],
              'Cell 21 groups not ready for workload promotion': stages[6],
              'Cell 22 temporal identity groups': stages[7],
              'Cell 22 currentness-validation groups': stages[8],
              'Cell 22 residual current identity groups': stages[9],
              'Cell 23 RCSO-routed residual members': stages[10],
              'Cell 23 Post-routed residual members': stages[11],
              'Cell 23 enterprise-routed residual members': stages[12],
              'Cell 23 customer-qualified residual rows': len(members),
              'Final assurance-case members': len(members),
              'Final deterministic assurance cases': len(cases), 'Final RCSO cases': stages[14],
              'Final Post DT cases': sum(r['primary_customer'] == 'POST_DT' for r in cases),
              'Final enterprise cases': sum(r['primary_customer'] in ('ENTERPRISE_SERVICE_OWNER', 'ENTERPRISE_DATA_OWNER') for r in cases),
              'Final RCAIP engineering cases': sum(r['primary_customer'] == 'RCAIP_ENGINEERING' for r in cases),
              'Final monitoring cases': sum(r['primary_customer'] == 'MONITORING' for r in cases),
              'Members assigned to multiple cases': 0, 'Noncanonical case classes': 0,
              'Cybersecurity-control cases created from unresolved identity alone': 0}
    for label, total in totals.items():
        require(line(label) == str(total), 'Saved count mismatch: ' + label)
    require(line('Case-member reconciliation') == f'{len(members)} / {len(members)}', 'Saved member reconciliation mismatch')
    for label, total in [('Identity-group reconciliation', stages[2]),
                         ('Analytical asset-row reconciliation', stages[1]),
                         ('No-link / Cell 21 reconciliation', stages[5]),
                         ('Temporal reconciliation', stages[7]),
                         ('Customer-routing reconciliation', len(members))]:
        require(line(label) == f'{total} / {total}', 'Saved reconciliation mismatch: ' + label)
    require(line('Cell 22 temporal limitation / other groups') == str(stages[7] - stages[8] - stages[9]),
            'Saved temporal reconciliation mismatch')
    provenance = {'mode': 'RETAINED_NOTEBOOK_RESULTS', 'input_sha256': hashlib.sha256(raw).hexdigest(),
                  'json_compatibility': {'redundant_output_type_count': len(redundant_outputs),
                                         'structural_locations': redundant_outputs},
                  'producer_ast_sha256': digest, 'saved_certification': saved,
                  'execution_count': cell.get('execution_count'), 'cell_metadata': cell.get('metadata', {}),
                  'notebook_metadata': book.get('metadata', {}),
                  'output_metadata': [{k: v for k, v in o.get('metadata', {}).items() if k != MIME} for o in outputs],
                  'limitation': 'Artifact identity, not a signed certificate or current live-state validity.'}
    data['historical_provenance'] = _json(provenance)
    consumer.evidence_value(data['historical_provenance'])  # Fail if Excel cannot retain the full record.
    return data


def workbook_from_retained_cell24(path, *, expected_post, expected_run, synthetic=False):
    """No automatic save. Caller keeps the workbook inside the approved environment."""
    return consumer.build_cell24_workbook(load_retained_cell24(
        path, expected_post=expected_post, expected_run=expected_run), synthetic=synthetic)
