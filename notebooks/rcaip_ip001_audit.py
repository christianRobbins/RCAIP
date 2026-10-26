"""Bounded independent IP-001 audit. No automatic invocation, writes or scans.

Inputs stay in enterprise. status() is the only shareable surface; approval of
aggregate disclosure is still required. Bindings are operator selections, not
historical execution certificates. This module cannot establish password policy.
"""
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import re
import xml.etree.ElementTree as ET


class AuditError(ValueError):
    """Safe boundary error; never include evidence values or raw engine messages."""


def require(condition, code):
    if not condition:
        raise AuditError(code)


def safe_audit(function):
    from functools import wraps
    @wraps(function)
    def guarded(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except AuditError:
            raise
        except (MemoryError, RecursionError):
            raise AuditError("AUDIT_RESOURCE_FAILURE") from None
        except Exception:
            # Public boundary is STOP, not empty/source-unavailable success.
            raise AuditError("AUDIT_INPUT_OR_TRANSPORT_FAILURE") from None
    return guarded


def generation_key(value):
    """UTC generation identity; never rewrite the retained evidence value.

    Zone-free text is the existing generated_at_utc/captured-generation contract.
    Native datetime objects must establish UTC explicitly. Only microsecond ISO
    text is supported: do not silently truncate precision or infer a zone.
    """
    from datetime import datetime, timedelta
    if isinstance(value, datetime):
        require(value.tzinfo is not None and value.utcoffset() == timedelta(0),
                'GENERATION_TIMEZONE_UNESTABLISHED')
        return value.replace(tzinfo=None)
    require(isinstance(value, str) and bool(re.fullmatch(
        r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)?', value)),
        'GENERATION_FORMAT_OR_TIMEZONE_UNESTABLISHED')
    try:
        return datetime.fromisoformat(value).replace(tzinfo=None)
    except ValueError:
        raise AuditError('INVALID_GENERATION') from None


@dataclass(frozen=True)
class Binding:
    table: str
    version: int
    table_id: str
    schema: tuple  # exact (column, Spark simpleString type) pairs
    producer_sha256: str  # recorded source identity, not proof of historical use
    generation: str

    def validate(self):
        require(bool(re.fullmatch(r'[A-Za-z_][\w]*\.[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?', self.table)), 'INVALID_TABLE_BINDING')
        require(type(self.version) is int and self.version >= 0, 'EXPLICIT_VERSION_REQUIRED')
        require(bool(self.table_id) and bool(self.schema) and bool(self.generation), 'INCOMPLETE_BINDING')
        require(bool(re.fullmatch('[0-9a-f]{64}', self.producer_sha256)), 'SOURCE_FINGERPRINT_REQUIRED')


@safe_audit
def read_delta_rows(spark, binding, post, run, max_rows=20000):
    """Read exactly one supplied immutable version; no registry/catalog discovery."""
    binding.validate()
    require(type(max_rows) is int and 0 < max_rows <= 100000, 'INVALID_ROW_BOUND')
    try:
        identity = spark.sql('DESCRIBE DETAIL ' + binding.table).select('id').collect()
        require(len(identity) == 1 and identity[0]['id'] == binding.table_id, 'TABLE_IDENTITY_CHANGED')
        df = spark.read.option('versionAsOf', binding.version).table(binding.table)
        require(tuple((f.name, f.dataType.simpleString()) for f in df.schema.fields) == binding.schema, 'SCHEMA_MISMATCH')
        from pyspark.sql import functions as F
        # Native temporal fields must not pass through Python datetime conversion.
        temporal = [f for f in df.schema.fields if f.dataType.simpleString() in ('timestamp','timestamp_ntz','date')]
        if any(f.dataType.simpleString() == 'timestamp' for f in temporal):
            require(spark.conf.get('spark.sql.session.timeZone') in ('UTC','Etc/UTC','+00:00','Z'), 'UTC_REQUIRED')
        for field in temporal:
            df = df.withColumn(field.name, F.col(field.name).cast('string'))
        rows = [r.asDict(recursive=True) for r in df.limit(max_rows + 1).collect()]
    except (OSError, TimeoutError):
        raise AuditError('INPUT_READ_UNAVAILABLE') from None
    require(len(rows) <= max_rows, 'ROW_BOUND_EXCEEDED')
    for row in rows:
        require(row.get('post_slug') == post and row.get('rcaip_run_id') == run, 'FOREIGN_OR_MISSING_SCOPE')
        require(generation_key(row.get('generated_at_utc')) == generation_key(binding.generation), 'GENERATION_MISMATCH')
    return rows


@safe_audit
def read_local_xml(path, expected_sha256, max_bytes=16*1024*1024):
    require(type(max_bytes) is int and 0 < max_bytes <= 64*1024*1024, 'INVALID_BYTE_BOUND')
    with Path(path).open('rb') as stream:
        raw = stream.read(max_bytes + 1)
    require(len(raw) <= max_bytes, 'XML_BYTE_BOUND_EXCEEDED')
    require(hashlib.sha256(raw).hexdigest() == expected_sha256, 'XML_HASH_MISMATCH')
    return raw


@dataclass
class AuditResult:
    _status: dict
    _details: dict  # stays in enterprise; deliberately excluded from repr

    def status(self):
        return dict(self._status)

    def __repr__(self):
        return 'AuditResult(' + repr(self.status()) + ')'


@safe_audit
def audit(artifacts, products, *, post, run, sample=None, max_bytes=16*1024*1024, max_rows=20000):
    """Pure bounded computation; artifacts contain bytes, sha256, scan_id,
    perspective and time_basis. products maps stage -> None (NOT_PRODUCED),
    'READ_ERROR', or {binding: Binding, rows: already read pinned rows}.
    Stages: normalized, general, q1143, actions, report_detail.
    sample is optional [{scan_id, ip_address, approved: True}].
    No artifact filename or row is included in status().
    """
    require(bool(post) and bool(run), 'SCOPE_REQUIRED')
    require(0 < max_bytes <= 64*1024*1024 and 0 < max_rows <= 100000, 'INVALID_BOUND')
    require(isinstance(artifacts, list) and len(artifacts) <= 100, 'ARTIFACT_BOUND')
    observations, contexts, seen = [], {}, set()
    printer_hosts = set()
    byte_total = 0
    for item in artifacts:
        raw = item['bytes']; scan = item['scan_id']
        byte_total += len(raw)
        require(byte_total <= max_bytes, 'XML_BYTE_BOUND_EXCEEDED')
        require(hashlib.sha256(raw).hexdigest() == item['sha256'], 'XML_HASH_MISMATCH')
        require(scan not in seen and bool(scan), 'DUPLICATE_OR_MISSING_SCAN')
        seen.add(scan)
        # UTF-8 (optionally BOM-prefixed) is the supported transport. Reject
        # UTF-16/32 NUL layouts and alternative declared codecs BEFORE Expat can
        # interpret declarations differently from the byte-level restrictions.
        try:
            raw.decode('utf-8-sig', errors='strict')
        except UnicodeDecodeError:
            raise AuditError('UNSUPPORTED_XML_ENCODING') from None
        require(b'\x00' not in raw, 'UNSUPPORTED_XML_ENCODING')
        encoding_declarations = re.findall(
            br'<\?xml\s+[^?]*\?>', raw, flags=re.IGNORECASE)
        for declaration in encoding_declarations:
            encodings = re.findall(br"encoding\s*=\s*(['\"])(.*?)\1", declaration)
            require(all(value.lower() == b'utf-8' for _, value in encodings),
                    'UNSUPPORTED_XML_ENCODING')
        # Accept only the inert Nmap declaration. Expat parses the ORIGINAL bytes;
        # no DTD fetch, textual rewriting or entity expansion is permitted.
        declarations = re.findall(br'<!DOCTYPE[^>]*>', raw, flags=re.IGNORECASE)
        require(b'<!ENTITY' not in raw.upper() and len(declarations) <= 1,
                'UNSUPPORTED_XML_DECLARATION')
        require(all(re.fullmatch(br'<!DOCTYPE\s+nmaprun\s*>', d) for d in declarations),
                'UNSUPPORTED_XML_DECLARATION')
        require(raw.upper().count(b'<!DOCTYPE') == len(declarations), 'MALFORMED_XML_DECLARATION')
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            raise AuditError('INVALID_XML') from None
        require(root.tag == 'nmaprun', 'UNSUPPORTED_XML_ROOT')
        scanned_ports = {}
        for info in root.findall('scaninfo'):
            protocol = info.get('protocol')
            require(protocol in ('tcp','udp','sctp'), 'SCAN_PROTOCOL_UNESTABLISHED')
            selected = set()
            for token in info.get('services','').split(','):
                require(bool(re.fullmatch(r'\d+(?:-\d+)?',token)), 'SCAN_PORT_SCOPE_UNESTABLISHED')
                parts = [int(x) for x in token.split('-')]
                lo, hi = parts[0], parts[-1]
                require(0 < lo <= hi <= 65535, 'INVALID_SCAN_PORT_SCOPE')
                selected.update(range(lo,hi+1))
            scanned_ports.setdefault(protocol,set()).update(selected)
        finish = root.find('runstats/finished')
        complete = finish is not None and finish.get('exit') == 'success'
        def epoch(value):
            if not isinstance(value,str) or not re.fullmatch(r'\d{1,12}(?:\.\d{1,6})?',value):
                return None
            from decimal import Decimal
            number = Decimal(value)
            return number if 0 <= number <= 253402300799 else None
        start = epoch(root.get('start'))
        end = epoch(finish.get('time')) if finish is not None else None
        time_valid = start is not None and end is not None and start <= end
        lineage = item.get('scope_lineage', {})
        scope_valid = (item.get('perspective') in ('INTERNAL_OPENNET','INTERNAL_ENTERPRISE_WIFI','PUBLIC_FACING')
            and lineage.get('post_slug') == post and lineage.get('rcaip_run_id') == run
            and lineage.get('scan_id') == scan and lineage.get('sha256') == item['sha256']
            and lineage.get('perspective') == item.get('perspective') and bool(lineage.get('reference')))

        for host in root.findall('host'):
            addresses = [a.get('addr') for a in host.findall('address') if a.get('addrtype') in ('ipv4','ipv6')]
            require(len(addresses) == 1 and bool(addresses[0]), 'AMBIGUOUS_HOST_ADDRESS')
            ip = addresses[0]
            context_key = (scan, ip)
            require(context_key not in contexts, 'AMBIGUOUS_HOST_OBSERVATION')
            ports = host.find('ports')
            signal = ' '.join([n.get('name','') for n in host.findall('hostnames/hostname')] +
                              [n.get(k,'') for n in host.findall('ports/port/service') for k in ('name','product')])
            if re.search(r'prnt|print|printer|jetdirect|ipp|lpd|xerox|ricoh|canon|lexmark|laserjet|officejet|mfp|mfd',signal.lower()):
                printer_hosts.add(context_key)
            contexts[context_key] = dict(complete=complete and host.get('timedout') not in ('true','1'),
                extraports=ports is not None and ports.find('extraports') is not None,
                basis=scope_valid and time_valid and item.get('time_basis') == 'UNIX_EPOCH_UTC',
                scanned={}, planned=scanned_ports)
            # Independent explicit-element oracle, not imported producer logic.
            if ports is None:
                continue
            for node in list(ports):
                if node.tag != 'port':
                    continue
                state = node.find('state'); service = node.find('service')
                try:
                    port = int(node.attrib['portid'])
                except (KeyError, ValueError):
                    raise AuditError('INVALID_PORT') from None
                protocol = node.get('protocol')
                require(protocol in ('tcp','udp','sctp') and 0 < port <= 65535, 'INVALID_ENDPOINT')
                state_name = state.get('state') if state is not None else None
                require(state_name in (None,'open','closed','filtered','unfiltered','open|filtered','closed|filtered'), 'UNSUPPORTED_PORT_STATE')
                row = dict(scan_id=scan, ip_address=ip, protocol=protocol, port=port, port_state=state_name,
                    service=service.get('name') if service is not None else None,
                    product=service.get('product') if service is not None else None,
                    version=service.get('version') if service is not None else None)
                observations.append(row)
                require(len(observations) <= max_rows, 'XML_ROW_BOUND_EXCEEDED')
                contexts[context_key]['scanned'].setdefault((protocol,port), []).append(state_name)
    fields = ('scan_id','ip_address','protocol','port','port_state','service','product','version')
    def members(rows):
        return Counter(tuple(row.get(f) for f in fields) for row in rows)
    positives = [r for r in observations if r['port_state'] == 'open']
    expected = members(positives)
    status = dict(xml_observations=len(observations), positive_observations=len(positives),
        q1143_scope='UNESTABLISHED' if sample is None else 'EXPLICIT_SAMPLE',
        password_validation='NOT_ESTABLISHED', unique_defaults='NOT_ESTABLISHED',
        stakeholder_action='NOT_ESTABLISHED', secondary_scan_verification='NOT_ESTABLISHED',
        overall_compliance='NOT_ASSESSED', historical_source_association='NOT_ESTABLISHED',
        oracle_lineage_limit='XML address/scan/protocol/port/service comparison; durable host identity and administrative context NOT independently established')
    details = {'coverage': [], 'differences': {}}
    for stage in ('normalized','general','q1143','actions','report_detail'):
        if stage not in products:
            status[stage] = 'UNAVAILABLE_BINDING'
            continue
        product = products[stage]
        if product is None or product == 'READ_ERROR':
            status[stage] = 'NOT_PRODUCED' if product is None else 'UNAVAILABLE_READ_ERROR'
            continue
        binding = product['binding']; binding.validate()
        rows = product['rows']
        host_keys = ('post_slug','rcaip_run_id','scan_id','host_key','ip_address',
                     'asset_domain','scan_scope','scan_location','scan_source_file')
        required_keys = host_keys if stage == 'actions' else host_keys + ('protocol','port')
        schema_names = [name for name,kind in binding.schema]
        require(len(schema_names) == len(set(schema_names)) and set(required_keys).issubset(schema_names), 'PRODUCT_KEY_SCHEMA_MISSING')
        require(len(rows) <= max_rows, 'PRODUCT_ROW_BOUND_EXCEEDED')
        for row in rows:
            require(all(row.get(k) is not None and str(row[k]).strip() for k in required_keys), 'PRODUCT_KEY_MISSING')
            require(row.get('post_slug') == post and row.get('rcaip_run_id') == run, 'FOREIGN_OR_MISSING_SCOPE')
            require(generation_key(row.get('generated_at_utc')) == generation_key(binding.generation), 'GENERATION_MISMATCH')
        if stage == 'actions':
            expanded=[]
            for row in rows:
                require(isinstance(row.get('port_members'),list), 'ACTION_MEMBER_AUTHORITY_MISSING')
                for value in row['port_members']:
                    member = json.loads(value)
                    require(member.get('post_slug') == post and member.get('rcaip_run_id') == run, 'FOREIGN_MEMBER_SCOPE')
                    require(all(member.get(k) is not None and str(member[k]).strip() for k in host_keys + ('protocol','port')), 'MEMBER_KEY_MISSING')
                    require(all(member[k] == row[k] for k in host_keys) and all(k in row and k in member and row[k] == member[k] for k in ('hostname','target_cidr')), 'MEMBER_PARENT_MISMATCH')
                    expanded.append(member)
            rows=expanded
            require(len(rows)<=max_rows, 'MEMBER_BOUND_EXCEEDED')
        actual=members(rows)
        target = members(observations) if stage=='normalized' else expected
        if stage in ('q1143','actions'):
            # Selection is not universal applicability. Orphans are still invalid.
            missing=Counter()
            if stage=='q1143':
                # XML signals independently establish a minimum candidate set.
                # KB-only candidate additions cannot be excluded by this oracle.
                required=members([r for r in positives if (r['scan_id'],r['ip_address']) in printer_hosts])
                missing=required-actual
                status['q1143_selection_limit']='XML_SIGNAL_MINIMUM; KB_ONLY_CONTEXT_NOT_INDEPENDENTLY_AUDITED'
            if stage=='actions':
                required=members([r for r in positives if r['protocol']=='tcp' and r['port'] in (21,23,80)])
                missing=required-actual
        else:
            missing=target-actual
        extra=actual-target
        status[stage] = 'MEMBERSHIP_MISMATCH' if missing or extra else ('AVAILABLE_POSITIVE' if rows else 'AVAILABLE_ZERO_POSITIVES')
        status[stage+'_missing']=sum(missing.values()); status[stage+'_extra']=sum(extra.values())
        details['differences'][stage]=(missing,extra)
    generations={generation_key(p['binding'].generation) for p in products.values() if isinstance(p,dict)}
    require(len(generations)<=1, 'MIXED_PRODUCT_GENERATIONS')
    if sample is not None:
        require(isinstance(sample,list) and len(sample)<=max_rows, 'SAMPLE_BOUND')
        for member in sample:
            require(member.get('approved') is True, 'SAMPLE_APPROVAL_REQUIRED')
            context=contexts.get((member['scan_id'],member['ip_address']))
            for port in (21,23,80):
                states=[] if context is None else context['scanned'].get(('tcp',port),[])
                observed=states[0] if len(set(states))==1 else 'UNESTABLISHED'
                coverage=('UNESTABLISHED' if context is None or not context['basis'] else
                          'INCOMPLETE' if not context['complete'] else
                          'UNESTABLISHED' if 'tcp' not in context['planned'] else
                          'NOT_SCANNED' if port not in context['planned']['tcp'] else
                          'EXTRAPORTS_LIMITATION' if not states and context['extraports'] else
                          'UNESTABLISHED' if not states else 'EXPLICIT_OBSERVATION')
                details['coverage'].append((observed,coverage))
    status['coverage_counts']=dict(Counter(c for _,c in details['coverage']))
    status['observed_state_counts']=dict(Counter(s or 'UNESTABLISHED' for s,_ in details['coverage']))
    return AuditResult(status,details)

# Prospective binding functions are read-only. They never execute the producer,
# write tables, choose a version, retain evidence, or invoke a report.
PORT_PRODUCTS = {'open_port_summary': 'IP001_PORT_SUMMARY',
                 'open_port_actions': 'IP001_POST_ACTION_TABLE_WITH_PROVENANCE',
                 'open_port_detail': 'IP001_OPEN_PORT_REVIEW_WITH_PROVENANCE'}


def table_identity(spark, table):
    require(bool(re.fullmatch(r'[A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?',table)), 'INVALID_TABLE_BINDING')
    rows=spark.sql('DESCRIBE DETAIL '+table).select('id').collect()
    require(len(rows)==1 and bool(rows[0]['id']), 'TABLE_IDENTITY_UNAVAILABLE')
    return rows[0]['id']


@safe_audit
def prepare_execution(spark, *, post, run, kb_table, kb_version, kb_table_id, source_files, xml_identities):
    """source_files: role -> {path, sha256}; roles producer, kb_builder, report.
    Files are deployed source-only exports, not output-bearing notebooks.
    xml_identities: [{source_file_name, source_file_sha256}] from approved intake.
    Explicit version required, never selected by timestamp/latest. Returns private
    runtime manifest; do not publish its contents externally.
    """
    from uuid import uuid4
    from datetime import datetime, timezone
    require(bool(post) and bool(run), 'SCOPE_REQUIRED')
    require(type(kb_version) is int and kb_version >= 0, 'EXPLICIT_VERSION_REQUIRED')
    require(set(source_files)=={'producer','kb_builder','report'}, 'SOURCE_ROLES_REQUIRED')
    sources={}
    for role, spec in source_files.items():
        raw=Path(spec['path']).read_bytes()
        digest=hashlib.sha256(raw.replace(b'\r\n',b'\n')).hexdigest()
        require(digest==spec['sha256'], 'DEPLOYED_SOURCE_HASH_MISMATCH')
        sources[role]={'path':str(spec['path']), 'sha256':digest}
    require(table_identity(spark,kb_table)==kb_table_id, 'KB_TABLE_REPLACED')
    frame=spark.read.option('versionAsOf',kb_version).table(kb_table)
    schema=tuple((f.name,f.dataType.simpleString()) for f in frame.schema.fields)
    require(bool(schema), 'KB_SCHEMA_UNAVAILABLE')
    require(table_identity(spark,kb_table)==kb_table_id, 'KB_TABLE_REPLACED')
    require(isinstance(xml_identities,list) and 0 < len(xml_identities) <= 100, 'XML_IDENTITY_BOUND')
    require(len({x['source_file_name'] for x in xml_identities})==len(xml_identities), 'DUPLICATE_XML_IDENTITY')
    for item in xml_identities:
        require(bool(item['source_file_name']) and bool(re.fullmatch('[0-9a-f]{64}',item['source_file_sha256'])), 'INVALID_XML_IDENTITY')
    return dict(state='PREPARED', execution_id=str(uuid4()), post_slug=post, rcaip_run_id=run,
                generation=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                sources=sources, kb=dict(table=kb_table,version=kb_version,table_id=kb_table_id,schema=schema),
                expected_xml=xml_identities, actual_xml=[], outputs={})


@safe_audit
def verified_producer_source(plan):
    """Return verified source bytes. Caller must separately authorize execution."""
    spec=plan['sources']['producer'];raw=Path(spec['path']).read_bytes().replace(b'\r\n',b'\n')
    require(hashlib.sha256(raw).hexdigest()==spec['sha256'], 'PRODUCER_CHANGED_AFTER_PREPARATION')
    return raw


@safe_audit
def pinned_kb(spark, plan, registered_table):
    kb=plan['kb']
    require(registered_table==kb['table'] and table_identity(spark,kb['table'])==kb['table_id'], 'KB_BINDING_CHANGED')
    result=spark.read.option('versionAsOf',kb['version']).table(kb['table'])
    require(tuple((f.name,f.dataType.simpleString()) for f in result.schema.fields)==kb['schema'], 'KB_SCHEMA_CHANGED')
    return result


@safe_audit
def bind_xml_inputs(plan, rows):
    require(len(rows)==len(plan['expected_xml']), 'XML_INPUT_COUNT_MISMATCH')
    actual=[]
    for row in rows:
        require(row['post_slug']==plan['post_slug'] and row['rcaip_run_id']==plan['rcaip_run_id'], 'XML_SCOPE_MISMATCH')
        actual.append({k:row[k] for k in ('source_file_name','source_file_sha256')})
    require(Counter(json.dumps(x,sort_keys=True) for x in actual)==Counter(json.dumps(x,sort_keys=True) for x in plan['expected_xml']), 'XML_INPUT_IDENTITY_MISMATCH')
    plan['actual_xml']=[dict(row) for row in rows]


@safe_audit
def output_write_token(plan, logical_name):
    from uuid import uuid4
    require(plan['state']=='RUNNING' and bool(plan['actual_xml']), 'EXECUTION_INPUTS_NOT_BOUND')
    require(logical_name not in plan['outputs'], 'DUPLICATE_OUTPUT_WRITE')
    return json.dumps(dict(execution_id=plan['execution_id'], write_id=str(uuid4()), logical_name=logical_name,
        producer_sha256=plan['sources']['producer']['sha256'], post_slug=plan['post_slug'],
        rcaip_run_id=plan['rcaip_run_id'], generation=plan['generation']),sort_keys=True)


@safe_audit
def capture_output(spark, plan, logical_name, table, token, prior_table_id):
    """Match writer userMetadata in bounded Delta history; NEVER choose latest.
    Failure to locate the exact write (including history limit) stops handoff.
    """
    require(plan['state']=='RUNNING' and logical_name not in plan['outputs'], 'OUTPUT_CAPTURE_STATE_INVALID')
    token_data=json.loads(token)
    require(token_data['execution_id']==plan['execution_id'] and token_data['logical_name']==logical_name
            and token_data['producer_sha256']==plan['sources']['producer']['sha256']
            and token_data['generation']==plan['generation']
            and token_data['post_slug']==plan['post_slug'] and token_data['rcaip_run_id']==plan['rcaip_run_id'], 'OUTPUT_TOKEN_MISMATCH')
    identity=table_identity(spark,table)
    require(prior_table_id is None or identity==prior_table_id,'OUTPUT_TABLE_REPLACED')
    history=spark.sql('DESCRIBE HISTORY '+table+' LIMIT 100').select('version','userMetadata').collect()
    matches=[r for r in history if r['userMetadata']==token]
    require(len(matches)==1,'EXACT_OUTPUT_COMMIT_NOT_FOUND')
    version=matches[0]['version'];require(type(version) is int and version>=0,'INVALID_OUTPUT_VERSION')
    frame=spark.read.option('versionAsOf',version).table(table)
    from pyspark.sql import functions as F
    for field,value in (('runtime_post_slug',plan['post_slug']),('rcaip_run_id',plan['rcaip_run_id']),('generated_at_utc',plan['generation'])):
        require(field in frame.columns,'OUTPUT_SCOPE_SCHEMA_MISSING')
        # Timestamp equality avoids string-format differences in trailing zeros.
        expected=F.lit(value).cast('timestamp') if field=='generated_at_utc' else F.lit(value)
        require(not frame.filter(~F.col(field).eqNullSafe(expected)).limit(1).count(),'OUTPUT_SCOPE_GENERATION_MISMATCH')
    require(table_identity(spark,table)==identity,'OUTPUT_TABLE_REPLACED')
    plan['outputs'][logical_name]=dict(logical_name=logical_name,table=table,version=version,table_id=identity,
        generation=plan['generation'],execution_id=plan['execution_id'],producer_sha256=plan['sources']['producer']['sha256'],
        schema=tuple((f.name,f.dataType.simpleString()) for f in frame.schema.fields),write_token=token)


@safe_audit
def port_report_bindings(plan):
    if plan['state']=='PREPARED':
        return {key:None for key in PORT_PRODUCTS}
    require(plan['state']=='CERTIFIED','PRODUCER_NOT_CERTIFIED')
    require(all(logical in plan['outputs'] for logical in PORT_PRODUCTS.values()),'REQUIRED_OUTPUT_NOT_CAPTURED')
    return {key:dict(plan['outputs'][logical]) for key,logical in PORT_PRODUCTS.items()}
