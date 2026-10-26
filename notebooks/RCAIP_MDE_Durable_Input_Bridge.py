# Databricks notebook source
"""Explicit MDE bridge. Import performs no helper load, I/O or enterprise call.

Every public operation requires module_folder. All helper bytes are verified
before any direct helper load; no PYTHONPATH or notebook globals are required.
Retained input is explicitly DBFS (existing paginated route) OR local, never an
automatic fallback. Public failures are sanitized and fatal, not availability.
No operation invokes Config/DIAG, writes Delta, or generates a workbook.
"""
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import sys

EXPECTED_MODULES = {
    'rcaip_cell24_presentation': 'c2d54f4b019a1630965a6c0b947d7fab54bf86cf76f236b4e352ddc59f8736d6',
    'rcaip_cell24_retained': '326fb26c2c8abbbf52af76fe104e0fec119ac083efa3abb954c8f2ca1276b55e',
    'rcaip_mde_retained_check': 'feb72cd4cb05743276cd6d01cf874ea6eab108b4d70cf8f9964bc0013cf6eb8c',
    'rcaip_mde_durable_bridge': '6e90125e0a2ae65e21126c3ffc25246638b3d2d87600bdd6bb804bafc78ad7ef',
}


class BridgeOperationError(RuntimeError):
    """Safe category only; never a successful unavailable result."""


def _safe(stage, operation, contract=None):
    try:
        return operation()
    except BridgeOperationError:
        raise
    except (MemoryError, RecursionError):
        raise MemoryError('MDE bridge fatal resource failure during ' + stage) from None
    except KeyboardInterrupt:
        raise KeyboardInterrupt('MDE bridge interrupted during ' + stage) from None
    except SystemExit:
        raise SystemExit('MDE bridge terminated during ' + stage) from None
    except Exception as exc:
        category = 'CONTRACT_REJECTED' if contract is not None and isinstance(exc, contract) else 'OPERATION_FAILED'
        raise BridgeOperationError(stage + ': ' + category + '; no validated result. Debug only within the approved environment.') from None


def _load_helpers(module_folder):
    if not isinstance(module_folder, (str, os.PathLike)):
        raise BridgeOperationError('HELPER_FOLDER: filesystem path required')
    folder = Path(module_folder)
    if not folder.is_dir(): raise BridgeOperationError('HELPER_FOLDER: directory unavailable')
    sources = {}
    for name, expected in EXPECTED_MODULES.items():
        path = folder / (name + '.py')
        if not path.is_file(): raise BridgeOperationError('HELPER_SOURCE: required module missing')
        text = path.read_bytes().decode('utf-8-sig').replace('\r\n', '\n').replace('\r', '\n')
        text = text.rstrip('\n') + '\n'
        if hashlib.sha256(text.encode('utf-8')).hexdigest() != expected:
            raise BridgeOperationError('HELPER_SOURCE: reviewed hash mismatch')
        sources[name] = text
    # Execute ONLY the verified helper bytes, never captured producer source.
    # Restore the caller's import namespace after linking the direct-loaded modules.
    previous = {name: sys.modules.get(name) for name in EXPECTED_MODULES}
    modules = {}
    try:
        for name, text in sources.items():
            spec = importlib.util.spec_from_file_location(name, folder / (name + '.py'))
            if spec is None: raise BridgeOperationError('HELPER_SOURCE: direct loader unavailable')
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            exec(compile(text, str(folder / (name + '.py')), 'exec'), module.__dict__)
            modules[name] = module
        return modules['rcaip_mde_durable_bridge']
    finally:
        for name, previous_module in previous.items():
            if previous_module is None: sys.modules.pop(name, None)
            else: sys.modules[name] = previous_module


def _download_retained(client, source_uri, expected_input_sha256):
    if not isinstance(source_uri, str) or not source_uri.startswith('dbfs:/'):
        raise ValueError('Explicit single DBFS retained input required')
    if not isinstance(expected_input_sha256, str) or not re.fullmatch('[a-f0-9]{64}', expected_input_sha256):
        raise ValueError('Explicit input SHA-256 required')
    source_path = source_uri[len('dbfs:'):]
    max_bytes = 64 * 1024 * 1024
    info = client.dbfs.get_status(source_path)
    if info.is_dir or type(info.file_size) is not int or not 0 < info.file_size <= max_bytes:
        raise RuntimeError('Retained input must be a nonempty file within the 64 MiB bound')
    notebook_buffer = bytearray()
    with client.dbfs.download(source_path) as stream:
        while True:
            request_bytes = min(1024 * 1024, max_bytes + 1 - len(notebook_buffer))
            chunk = stream.read(request_bytes)
            if not chunk: break
            if type(chunk) is not bytes or len(chunk) > request_bytes:
                raise RuntimeError('Unsupported binary download response')
            notebook_buffer.extend(chunk)
            if len(notebook_buffer) > max_bytes:
                raise RuntimeError('Retained notebook exceeds the byte bound')
    raw = bytes(notebook_buffer)
    if len(raw) != info.file_size: raise RuntimeError('Downloaded length differs from file metadata')
    if hashlib.sha256(raw).hexdigest() != expected_input_sha256:
        raise RuntimeError('Retained input SHA-256 mismatch')
    return raw


def _detail(spark, binding, bridge):
    # Table syntax has already passed validate_bindings; no arbitrary SQL/name.
    name = '.'.join('`'+part+'`' for part in binding['table'].split('.'))
    detail = spark.sql('DESCRIBE DETAIL '+name).first()
    bridge.require(detail is not None and detail['format'] == 'delta'
                   and detail['id'] == binding['table_id'], 'Named Delta table identity/format mismatch')


def _frames(spark, manifest, bridge):
    result = {}
    for name in bridge.INPUTS:
        binding = manifest['inputs'][name]
        _detail(spark, binding, bridge)
        frame = spark.read.format('delta').option('versionAsOf', binding['version']).table(binding['table'])
        fields = bridge.strict_json(frame.schema.json().encode('utf-8'))['fields']
        # Canonical transport schema deliberately retains original field order,
        # types, nullability and metadata as supplied by the snapshot schema.
        bridge.check._table({'schema': fields})
        bridge.require(bridge.digest(bridge.encoded(fields)) == binding['schema_sha256'], 'Pinned schema/witness mismatch')
        result[name] = (frame, fields)
    zone = spark.conf.get('spark.sql.session.timeZone')
    if any(bridge.retained._schema_type(f['type']) == 'timestamp' for _, fields in result.values() for f in fields):
        bridge.require(zone in ('UTC','Etc/UTC','Z','+00:00'), 'Native timestamp snapshot transport requires supported UTC session')
    return result, zone


def _literal(value):
    # UTF-8 hex literal prevents scope data from becoming SQL syntax.
    return "CAST(unhex('"+value.encode('utf-8').hex()+"') AS STRING)"


def _collect(frame, fields, bridge):
    columns = [f['name'] for f in fields]
    projections, numeric = [], []
    for field in fields:
        name = field['name']; escaped = '`'+name.replace('`','``')+'`'
        kind = bridge.retained._schema_type(field['type'])
        if isinstance(kind,str) and (kind in ('timestamp','timestamp_ntz','date','float','double') or kind.startswith('decimal(')):
            projections.append('CAST('+escaped+' AS STRING) AS '+escaped)
            if kind in ('float','double') or kind.startswith('decimal('):numeric.append(name)
        else: projections.append(escaped)
    limited = frame.selectExpr(*projections).limit(bridge.MAX_ROWS+1)
    # Size-check serialized representation BEFORE bringing rows to the driver.
    json_rows = limited.selectExpr("to_json(struct(*), map('ignoreNullFields','false')) AS transport")
    sizes = json_rows.selectExpr('count(*) AS rows', 'coalesce(sum(octet_length(transport)),0) AS bytes').first()
    bridge.require(sizes['rows'] <= bridge.MAX_ROWS and sizes['bytes'] <= bridge.MAX_BYTES, 'Snapshot row/byte bound exceeded; no truncation')
    collected = limited.collect()
    bridge.require(len(collected) == sizes['rows'] and len(collected) <= bridge.MAX_ROWS, 'Snapshot changed during bounded collection')
    rows = []
    for row in collected:
        record = row.asDict(recursive=True)
        for name in numeric:
            if record[name] is not None:
                bridge.require(isinstance(record[name],str), 'Numeric snapshot cast must retain text')
                try: record[name] = bridge.Decimal(record[name])
                except ArithmeticError: raise bridge.InputContractError('Invalid numeric snapshot transport') from None
        rows.append([record[c] for c in columns])
    payload = {'schema': fields, 'data': rows, 'overflow': False}
    bridge._records(payload)
    return payload


def _retained_bytes(bridge, expected_hash, *, input_route, source_uri=None, retained_path=None, client=None):
    if input_route == 'dbfs':
        if retained_path is not None: raise BridgeOperationError('INPUT_ROUTE: conflicting local path')
        if client is None:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient()
        return _download_retained(client, source_uri, expected_hash)
    if input_route == 'local':
        if source_uri is not None or client is not None: raise BridgeOperationError('INPUT_ROUTE: conflicting DBFS input')
        path = Path(retained_path)
        if path.suffix.lower() != '.ipynb': raise BridgeOperationError('INPUT_ROUTE: explicit local notebook required')
        with path.open('rb') as stream: raw = stream.read(bridge.MAX_BYTES + 1)
        bridge.require(0 < len(raw) <= bridge.MAX_BYTES and bridge.digest(raw) == expected_hash, 'Local retained byte/hash mismatch')
        return raw
    raise BridgeOperationError('INPUT_ROUTE: select dbfs or local explicitly')


def inspect_bridge(manifest, evidence_bytes, reviewed_trust, *, module_folder, spark=None):
    bridge = _safe('HELPER_LOADING', lambda: _load_helpers(module_folder))
    report = _safe('PROVENANCE_INSPECTION', lambda: bridge.inspect_inputs(manifest,evidence_bytes,reviewed_trust), bridge.InputContractError)
    if report['state'] != 'PROVENANCE_RECORDS_MATCH' or spark is None: return report
    _safe('PINNED_SCHEMA_INSPECTION', lambda: _frames(spark,manifest,bridge), bridge.InputContractError)
    return dict(report, schema_availability='PINNED_SCHEMAS_MATCH; ROWS_NOT_READ')


def _read_snapshots(bridge, spark, m):
    frames,zone = _frames(spark,m,bridge)
    scoped = {}
    predicate = "lower(trim(post_slug)) = "+_literal(m['post'])+' AND rcaip_run_id = '+_literal(m['run'])
    for name,(frame,fields) in frames.items():scoped[name]=frame.filter(predicate)
    scoped[bridge.CONTROL]=scoped[bridge.CONTROL].filter("upper(trim(tool_name)) = 'MDE'")
    # Search is the candidate authority; never use source evidence to expand it.
    # Enrichment is read in full exact scope so foreign/missing keys can be rejected.
    # Control is candidate-bound before expensive collection; no identity matching.
    scoped[bridge.CONTROL]=scoped[bridge.CONTROL].join(scoped[bridge.SEARCH].select('asset_key'),on='asset_key',how='left_semi')
    inputs={}
    for name in bridge.INPUTS:
        b=m['inputs'][name]
        inputs[name]={'table':b['table'],'table_id':b['table_id'],'version':b['version'],
                      'payload':_collect(scoped[name],frames[name][1],bridge)}
        _detail(spark,b,bridge)
    bridge.require(spark.conf.get('spark.sql.session.timeZone')==zone, 'Snapshot timestamp session changed')
    return inputs


def reproduce_bridge(manifest, evidence_bytes, reviewed_trust, *, module_folder, spark,
                     input_route, source_uri=None, retained_path=None, client=None):
    bridge = _safe('HELPER_LOADING', lambda: _load_helpers(module_folder))
    m = _safe('HISTORICAL_BINDINGS', lambda: bridge.validate_bindings(manifest,evidence_bytes,reviewed_trust), bridge.InputContractError)
    original = _safe('RETAINED_TRANSPORT', lambda: _retained_bytes(bridge,m['retained_sha256'],input_route=input_route,source_uri=source_uri,retained_path=retained_path,client=client), bridge.InputContractError)
    _safe('ORIGINAL_CASE_CONTRACT', lambda: bridge.load_original(original,m), bridge.InputContractError)
    inputs = _safe('PINNED_INPUT_READ', lambda: _read_snapshots(bridge,spark,m), bridge.InputContractError)
    return _safe('MEMBER_REPRODUCTION', lambda: bridge.reproduce(m,evidence_bytes,reviewed_trust,inputs,original), bridge.InputContractError)


def retain_bridge(bundle, destination, *, module_folder, approved_directory, reviewed_trust,
                  input_route, source_uri=None, retained_path=None, client=None):
    bridge = _safe('HELPER_LOADING', lambda: _load_helpers(module_folder))
    original = _safe('RETAINED_TRANSPORT', lambda: _retained_bytes(bridge,bundle['original_notebook_sha256'],input_route=input_route,source_uri=source_uri,retained_path=retained_path,client=client), bridge.InputContractError)
    return _safe('DERIVED_RETENTION', lambda: bridge.retain_bundle(bundle,destination,approved_directory=approved_directory,trusted_references=reviewed_trust,original_raw=original), bridge.InputContractError)


def reopen_bridge(bundle_path, *, expected_bundle_sha256, module_folder, reviewed_trust,
                  expected_original_sha256, input_route, source_uri=None, retained_path=None, client=None):
    bridge = _safe('HELPER_LOADING', lambda: _load_helpers(module_folder))
    original = _safe('RETAINED_TRANSPORT', lambda: _retained_bytes(bridge,expected_original_sha256,input_route=input_route,source_uri=source_uri,retained_path=retained_path,client=client), bridge.InputContractError)
    return _safe('SAVED_BUNDLE_IDENTITY_AND_CONTRACT', lambda: bridge.load_bundle(bundle_path,expected_sha256=expected_bundle_sha256,trusted_references=reviewed_trust,original_raw=original), bridge.InputContractError)
