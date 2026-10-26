"""Run-scoped devresults adapter. Inert; no Config/producers executed at import.

All completed products are immutable Delta commits, never filesystem markers.
Workspace Files are presentation only. Metadata records are bounded single rows.
"""
import ast
import hashlib
import io
import json
import re
import sys
from copy import deepcopy
from types import ModuleType

DEPENDENCIES = {
    'registry': 'a7e9168f1af14dccfb15fef5a1de4ccf16eae5883b84178e7b3fa872abc11832',
    'writer': '7fbd254de65d6bb050094da9f5f2aad0249584ca1e14cd71307f92196fe2746b',
    'audit': '4896cfd6c1933bfe3773bd0837b63b1cf4b2b055a5663f0b1a0580ce38df373f'}
ROLES = ('manifest', 'intent', 'proof', 'evidence', 'checkpoint', 'keys',
         'contact_groups', 'missing_identity', 'profile', 'pages', 'render',
         'receipts', 'publication', 'events')
OPS = ['ACQUIRE_2558', 'WORKSPACE_BINDING', 'DELTA_CHECKPOINT', 'PROFILE_SAVED', 'DELTA_PROFILE',
       'DELTA_REPORT_MATERIAL', 'WORKSPACE_PUBLISH']
META = ('runtime_post_slug', 'rcaip_run_id', 'generated_at_utc')
MAX_BYTES = 1024 * 1024
MAX_PAGES = 10000
MAX_RECEIPT_BYTES = 8192


class Stop(ValueError):
    """Fixed safe code; never expose arbitrary transport errors."""


def need(value, code):
    if not value:
        raise Stop(code)


def blob(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def extract(source, names, namespace):
    """Compile only identified functions from hash-verified source, not notebook cells."""
    tree = ast.parse(source)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    need({n.name for n in nodes} == set(names), 'SOURCE_FUNCTIONS_MISSING')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<verified-contract-functions>', 'exec'), namespace)
    return namespace


def contracts(sources):
    for name, sha in DEPENDENCIES.items():
        need(digest(sources[name].replace('\r\n', '\n').encode()) == sha, 'CONTRACT_SOURCE_CHANGED')
    writer = extract(sources['writer'], ['ip001_write_with_metadata'], {})['ip001_write_with_metadata']
    audit = ModuleType('verified_intune_delta_audit')
    sys.modules[audit.__name__] = audit
    exec(compile(sources['audit'], '<verified-audit>', 'exec'), audit.__dict__)
    return writer, audit


def workspace_host(value):
    """Exact configured HTTPS origin; authenticated by current_user.me on this client.

    Supplied planning metadata is not itself authentication. No inferred host,
    credentials, paths, query strings or implicit normalization are accepted.
    """
    from urllib.parse import urlsplit
    need(isinstance(value, str) and bool(value), 'WORKSPACE_IDENTITY_REQUIRED')
    try:
        u = urlsplit(value)
        valid = (u.scheme == 'https' and bool(u.hostname) and not u.username and not u.password
                 and not u.path and not u.query and not u.fragment and u.port is None
                 and not any(c.isspace() for c in value) and value == 'https://' + u.hostname)
    except ValueError:
        valid = False
    need(valid, 'WORKSPACE_IDENTITY_REQUIRED')
    return value


def prepare(inputs, sources, helpers):
    need(set(inputs) == {'runtime', 'metadata', 'workflow_id', 'generation_utc',
                        'registry', 'principal', 'workspace_host', 'input_authority_reference'}, 'EXPLICIT_DEVRESULTS_INPUTS_REQUIRED')
    need(inputs['input_authority_reference'], 'INPUT_AUTHORITY_REQUIRED')
    workspace_host(inputs['workspace_host'])
    registry = inputs['registry']
    need(set(registry) == {'catalog', 'results_namespace', 'reference'}, 'REGISTRY_CONTRACT_REQUIRED')
    need(registry['results_namespace'] in ('devresults', registry['catalog'] + '.devresults')
         and re.fullmatch(r'[A-Za-z_]\w*', registry['catalog']) and registry['reference'], 'DEVRESULTS_QUALIFICATION_REQUIRED')
    post = inputs['runtime']['RCAIP_POST_SLUG']
    workflow = inputs['workflow_id']
    need(re.fullmatch('[a-z][a-z0-9_]*', post) and re.fullmatch('[a-z0-9_]{8,64}', workflow), 'SAFE_RUN_BINDING_REQUIRED')
    need(isinstance(inputs['principal'], str) and inputs['principal'] and
         not any(c in inputs['principal'] for c in '/\\\r\n'), 'AUTHENTICATED_PRINCIPAL_REQUIRED')
    need(re.fullmatch(r'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}', inputs['generation_utc']), 'EXPLICIT_UTC_GENERATION_REQUIRED')
    from datetime import datetime
    try:
        datetime.strptime(inputs['generation_utc'], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        raise Stop('INVALID_UTC_GENERATION') from None
    writer, audit = contracts(sources)
    base = {k: sources[k] for k in helpers.HASHES}
    v, p = helpers.modules(base)
    parent = v.prepare(engine_source=base['engine'], config_source=base['config'],
                       runtime=inputs['runtime'], metadata=inputs['metadata'])
    acquisition = v.prepare_distributed(parent, engine_source=base['engine'], config_source=base['config'])
    # An isolated additive registry overlay; no changes to existing RCAIP_TABLES.
    # Reuse Config001 physical-name and logical resolution functions exactly.
    ns = {'configured_results_namespace': registry['catalog'] + '.devresults'}
    extract(sources['registry'], ['_rcaip_shared_table', 'get_rcaip_table'], ns)
    ns['RCAIP_TABLES'] = {'INTUNE_DELIVERY_' + role.upper(): ns['_rcaip_shared_table'](
        post + '_intune_delivery_' + workflow + '_' + role) for role in ROLES}
    tables = {role: ns['get_rcaip_table']('INTUNE_DELIVERY_' + role.upper()) for role in ROLES}
    plan = {'contract': 'INTUNE_DEVRESULTS_V1', 'inputs': deepcopy(inputs), 'tables': tables,
            'report_directory': '/Users/' + inputs['principal'] + '/RCAIP_Reports/' + post + '/intune_' + workflow,
            'sources': dict({k: digest(val.replace('\r\n', '\n').encode()) for k, val in sources.items()},
                            delivery=helpers._LOADED_SOURCE_SHA),
            'operations': OPS, 'acquisition': acquisition.private,
            'scan': 'FULL_SNAPSHOT_SCAN_POSSIBLE', 'max_page_bytes': MAX_BYTES, 'max_pages': MAX_PAGES,
            'max_receipts': MAX_PAGES + 1, 'max_receipt_bytes': MAX_RECEIPT_BYTES}
    return plan, digest(blob(plan)), acquisition, v, p, writer, audit


class DeltaStore:
    """Actual table API adapter. No hidden managed paths or latest-version reads."""
    def __init__(self, spark, plan, plan_id, writer, audit):
        self.spark, self.plan, self.plan_id, self.writer, self.audit = spark, plan, plan_id, writer, audit

    def exists(self, role):
        return self.spark.catalog.tableExists(self.plan['tables'][role])

    def preflight(self):
        catalog = self.plan['inputs']['registry']['catalog']
        need(self.spark.catalog.currentCatalog() == catalog, 'AUTHENTICATED_CATALOG_CHANGED')
        need(self.spark.catalog.databaseExists(catalog + '.devresults'), 'DEVRESULTS_NAMESPACE_UNAVAILABLE')

    def context(self):
        return {'state': 'RUNNING', 'execution_id': self.plan['inputs']['workflow_id'],
                'post_slug': self.plan['inputs']['runtime']['RCAIP_POST_SLUG'],
                'rcaip_run_id': self.plan['inputs']['runtime']['RCAIP_RUN_ID'],
                'generation': self.plan['inputs']['generation_utc'], 'outputs': {},
                'sources': {'producer': {'sha256': self.plan['sources']['delivery']}}}

    def token(self, role):
        c = self.context()
        return json.dumps(dict(execution_id=c['execution_id'], logical_name=role,
            write_id=digest(blob([self.plan_id, role])), plan_id=self.plan_id,
            producer_sha256=c['sources']['producer']['sha256'], post_slug=c['post_slug'],
            rcaip_run_id=c['rcaip_run_id'], generation=c['generation']), sort_keys=True)

    def capture(self, role):
        c = self.context()
        self.audit.capture_output(self.spark, c, role, self.plan['tables'][role], self.token(role), None)
        return json.loads(blob(c['outputs'][role]))

    def read(self, role, ref):
        need(ref == self.capture(role), 'PINNED_COMMIT_OR_IDENTITY_CHANGED')
        result = self.spark.read.option('versionAsOf', ref['version']).table(ref['table'])
        need(self.audit.table_identity(self.spark, ref['table']) == ref['table_id'], 'TABLE_REPLACED')
        from pyspark.sql import functions as F
        result = result.drop(*META)
        return result.select(*[F.col(n).alias(n[len('__intune_native_'):])
            if n.startswith('__intune_native_') else F.col(n) for n in result.columns])

    def schema(self, frame):
        return frame.schema.jsonValue()

    def equal(self, left, right):
        a = [(f['name'], f['type']) for f in self.schema(left)['fields']]
        b = [(f['name'], f['type']) for f in self.schema(right)['fields']]
        need(a == b and left.exceptAll(right).limit(1).count() == 0 and
             right.exceptAll(left).limit(1).count() == 0, 'FULL_MULTISET_OR_SCHEMA_MISMATCH')

    def save(self, role, frame):
        from pyspark.sql import functions as F
        need(not any(n.startswith('__intune_native_') for n in frame.columns), 'RESERVED_SCOPE_COLUMNS')
        c = self.context()
        preserved = frame.select(*[F.col(n).alias('__intune_native_' + n) if n in META else F.col(n)
                                  for n in frame.columns])
        scoped = (preserved.withColumn(META[0], F.lit(c['post_slug']))
                  .withColumn(META[1], F.lit(c['rcaip_run_id']))
                  .withColumn(META[2], F.lit(c['generation']).cast('timestamp')))
        self.writer(self.spark, scoped, self.plan['tables'][role], self.token(role), 'errorifexists')
        ref = self.capture(role)
        self.equal(frame, self.read(role, ref))
        return ref

    def record(self, role, value):
        raw = blob(value)
        need(len(raw) <= MAX_BYTES, 'METADATA_RECORD_BOUND')
        frame = self.spark.createDataFrame([(raw.decode(),)], 'payload string')
        return self.save(role, frame)

    def get(self, role):
        ref = self.capture(role)
        rows = self.read(role, ref).limit(2).collect()
        need(len(rows) == 1 and len(rows[0]['payload'].encode()) <= MAX_BYTES, 'INVALID_COMPLETION_RECORD')
        def unique(pairs):
            d = {}
            for key, value in pairs:
                need(key not in d, 'DUPLICATE_RECORD_KEY'); d[key] = value
            return d
        return json.loads(rows[0]['payload'], object_pairs_hook=unique), ref

    def receipt_frame(self, receipts):
        # One bounded native row per artifact; never a giant completion JSON.
        need(1 <= len(receipts) <= MAX_PAGES + 1, 'PUBLICATION_RECEIPT_COUNT')
        need(len({r['path'] for r in receipts}) == len(receipts), 'PUBLICATION_RECEIPT_DUPLICATE')
        rows = []
        for ordinal, r in enumerate(receipts):
            need(set(r) == {'path', 'sha256', 'bytes'} and len(blob(r)) <= MAX_RECEIPT_BYTES
                 and re.fullmatch('[0-9a-f]{64}', r['sha256']) and type(r['bytes']) is int
                 and 0 <= r['bytes'] <= MAX_BYTES, 'PUBLICATION_RECEIPT_BOUND')
            rows.append((ordinal, r['path'], r['sha256'], r['bytes']))
        return self.spark.createDataFrame(rows, 'ordinal long, path string, sha256 string, bytes long')

    def proof(self, frame):
        from pyspark.sql import functions as F
        # Hash every native row including nulls, and retain every multiplicity.
        # This is cryptographic evidence, not an equality-of-counts shortcut.
        return frame.select(F.sha2(F.to_json(F.struct(*[F.col(n) for n in frame.columns]),
            options={'ignoreNullFields': 'false', 'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}), 256)
            .alias('row_sha256')).groupBy('row_sha256').count()

    def pages(self, products):
        from pyspark.sql import functions as F, Window
        def escape(c):
            for old, new in [('&', '&amp;'), ('<', '&lt;'), ('>', '&gt;'), ('"', '&quot;'), ("'", '&#x27;')]:
                c = F.regexp_replace(c, old, new)
            return c
        result = None
        for name, frame in products.items():
            cards = []
            for field in frame.schema.fields:
                c = F.col(field.name)
                if field.dataType.typeName() in ('timestamp', 'timestamp_ntz', 'date', 'decimal', 'float', 'double'):
                    c = c.cast('string')
                value = F.to_json(F.struct(c.alias('value')), options={'ignoreNullFields': 'false'})
                cards.append(F.concat(F.lit('<dt>' + field.name + '</dt><dd><pre>'), escape(value), F.lit('</pre></dd>')))
            card = F.concat(F.lit('<article><dl>'), *cards, F.lit('</dl></article>'))
            rows = frame.select(card.alias('card')).withColumn('rn', F.row_number().over(Window.orderBy('card')))
            groups = rows.withColumn('page', F.floor((F.col('rn') - 1) / 50).cast('long')).groupBy('page').agg(
                F.sort_array(F.collect_list(F.struct('rn', 'card'))).alias('parts'))
            part = groups.select(F.lit(name).alias('product'), 'page', F.concat(
                F.lit('<!doctype html><meta charset="utf-8"><a href="index.html">Time basis and field legend</a><h1>' + name + '</h1>'),
                F.concat_ws('', F.transform('parts', lambda x: x['card']))).alias('html'))
            result = part if result is None else result.unionByName(part)
        need(not result.filter(F.length(F.encode('html', 'UTF-8')) > MAX_BYTES).limit(1).count(), 'REPORT_PAGE_TOO_LARGE')
        need(result.count() <= MAX_PAGES, 'REPORT_PAGE_COUNT_BOUND')
        return result

    def page_rows(self, frame):
        # Only bounded identifiers and one bounded HTML page at a time on driver.
        ids = frame.select('product', 'page').limit(MAX_PAGES + 1).collect()
        need(len(ids) <= MAX_PAGES and len({(r['product'], r['page']) for r in ids}) == len(ids), 'REPORT_PAGE_IDENTITIES')
        from pyspark.sql import functions as F
        for r in sorted(ids, key=lambda x: (x['product'], x['page'])):
            rows = frame.filter((F.col('product') == r['product']) & (F.col('page') == r['page'])).limit(2).collect()
            need(len(rows) == 1, 'REPORT_PAGE_MEMBERSHIP')
            raw = rows[0]['html'].encode(); need(len(raw) <= MAX_BYTES, 'REPORT_PAGE_TOO_LARGE')
            yield r['product'] + '-' + str(r['page']) + '.html', raw

    def event(self, phase, status):
        # Append-only attempts are informational, never completion authority.
        from pyspark.sql import functions as F
        import uuid
        c = self.context()
        attempt = str(uuid.uuid4())
        frame = self.spark.createDataFrame([(phase, status, attempt, self.plan_id)],
            'phase string, status string, attempt string, plan_id string')
        for name, value in zip(META, (c['post_slug'], c['rcaip_run_id'], c['generation'])):
            frame = frame.withColumn(name, F.lit(value).cast('timestamp') if name == META[2] else F.lit(value))
        token = json.loads(self.token('events')); token['write_id'] = attempt
        token = json.dumps(token, sort_keys=True)
        table = self.plan['tables']['events']
        prior = self.audit.table_identity(self.spark, table) if self.exists('events') else None
        self.writer(self.spark, frame, table, token, 'append')
        self.audit.capture_output(self.spark, c, 'events', table, token, prior)


class WorkspacePublisher:
    """Report001 Cell 8 Workspace Files transport; new names, never overwrite."""
    def __init__(self, client, plan):
        self.client, self.plan = client, plan

    def verify_principal(self):
        actual = workspace_host(getattr(getattr(self.client, 'config', None), 'host', None))
        need(actual == self.plan['inputs']['workspace_host'], 'AUTHENTICATED_WORKSPACE_CHANGED')
        need(self.client.current_user.me().user_name == self.plan['inputs']['principal'], 'AUTHENTICATED_PRINCIPAL_CHANGED')

    def publish(self, name, raw):
        from databricks.sdk.errors import NotFound
        from databricks.sdk.service.workspace import ExportFormat, ImportFormat
        need(re.fullmatch(r'[a-z0-9_-]+\.(?:html|json)', name) and len(raw) <= MAX_BYTES, 'PUBLICATION_BOUND')
        path = self.plan['report_directory'] + '/' + name
        self.verify_principal()
        try:
            self.client.workspace.get_status(path)
        except NotFound:
            self.client.workspace.mkdirs(self.plan['report_directory'])
            self.client.workspace.upload(path=path, content=io.BytesIO(raw), format=ImportFormat.AUTO, overwrite=False)
        with self.client.workspace.download(path, format=ExportFormat.AUTO) as stream:
            copied = stream.read(MAX_BYTES + 1)
        need(copied == raw, 'PUBLICATION_READBACK_MISMATCH')
        return {'path': path, 'sha256': digest(raw), 'bytes': len(raw)}


def execute(inputs, sources, helpers, *, mode='PLAN_ONLY', approval=None, anchor=None,
            spark=None, client=None, backend=None, publisher=None, progress=None, fault=None):
    inputs, sources, approval, anchor = map(deepcopy, (inputs, sources, approval, anchor))
    plan, plan_id, acquisition, v, p, writer, audit = prepare(inputs, sources, helpers)
    need(mode in ('PLAN_ONLY', 'FRESH', 'RESUME'), 'MODE_REQUIRED')
    if mode == 'PLAN_ONLY':
        return {'status': mode, 'plan_id': plan_id, 'private_plan': plan, 'operations': OPS, 'values_read': False}
    operations = OPS if mode == 'FRESH' else OPS[1:]
    need(mode != 'FRESH' or callable(progress), 'PRIVATE_ANCHOR_RECORDER_REQUIRED')
    need(isinstance(approval, dict) and set(approval) == {'plan_id', 'reference', 'operations', 'allow_values',
         'allow_writes', 'allow_recomputation', 'anchor'} and approval['plan_id'] == plan_id
         and approval['operations'] == operations and approval['anchor'] == anchor and bool(approval['reference'])
         and all(approval[k] is True for k in ('allow_values', 'allow_writes', 'allow_recomputation')), 'EXACT_WORKFLOW_APPROVAL_REQUIRED')
    store = backend if backend is not None else DeltaStore(spark, plan, plan_id, writer, audit)
    pub = publisher if publisher is not None else WorkspacePublisher(client, plan)
    analysis = helpers.SparkIO(spark, v, p) if backend is None else backend.analysis(v, p)
    need(analysis.timezone() == inputs['metadata']['session_timezone'], 'SESSION_TIMEZONE_CHANGED')
    def point(name):
        if fault: fault(name)
    def notify(ref, value):
        result = {'manifest': ref, 'sha256': digest(blob(value)), 'plan_id': plan_id}
        pub.publish('binding.json', blob(result))
        if progress: progress(result)
        return result
    phase = 'MANIFEST'
    owned = False
    try:
        pub.verify_principal()
        store.preflight()
        if mode == 'FRESH':
            need(anchor is None and not any(store.exists(r) for r in ROLES), 'WORKFLOW_COLLISION')
            manifest = {'plan': plan, 'approval': approval, 'origin': 'NEW_EXPLICIT_WORKFLOW'}
            ref = store.record('manifest', manifest)
            owned = True
            anchor = notify(ref, manifest)  # observed binding, privately retain before values
        else:
            need(isinstance(anchor, dict) and anchor.get('plan_id') == plan_id, 'EXTERNAL_MANIFEST_BINDING_REQUIRED')
            store.read('manifest', anchor['manifest'])
            manifest, ref = store.get('manifest')
            need(ref == anchor['manifest'] and digest(blob(manifest)) == anchor['sha256']
                 and manifest['plan'] == plan, 'EXTERNAL_MANIFEST_MISMATCH')
            owned = True
        phase = 'ACQUISITION' if mode == 'FRESH' else 'CHECKPOINT'
        store.event(phase, 'STARTED')
        if mode == 'FRESH':
            pub.verify_principal()
            evidence, summary, native = analysis.acquire(acquisition, approval, {k: sources[k] for k in helpers.HASHES})
            phase = 'CHECKPOINT'
            store.record('intent', {'summary': summary, 'schema': store.schema(evidence), 'native_attempt': native})
            point('before_proof')
            store.save('proof', store.proof(evidence))
            point('after_proof')
            store.save('evidence', evidence)
            point('after_evidence_commit')
        # Missing proof/evidence means STOP; never call acquire on resume.
        need(store.exists('intent') and store.exists('proof') and store.exists('evidence'), 'INCOMPLETE_EVIDENCE_NO_REACQUISITION')
        intent, intent_ref = store.get('intent')
        evidence_ref, proof_ref = store.capture('evidence'), store.capture('proof')
        evidence = store.read('evidence', evidence_ref)
        need([(f['name'], f['type']) for f in store.schema(evidence)['fields']] ==
             [(f['name'], f['type']) for f in intent['schema']['fields']], 'EVIDENCE_SCHEMA_CHANGED')
        store.equal(store.proof(evidence), store.read('proof', proof_ref))
        need(analysis.summary(evidence) == intent['summary'], 'EVIDENCE_ACCOUNTING_CHANGED')
        checkpoint = {'plan_id': plan_id, 'evidence': evidence_ref, 'proof': proof_ref, 'intent': intent_ref}
        if store.exists('checkpoint'):
            need(store.get('checkpoint')[0] == checkpoint, 'CHECKPOINT_MISMATCH')
        else:
            point('before_checkpoint_completion'); store.record('checkpoint', checkpoint)
        point('after_checkpoint')
        phase = 'PROFILE'
        if store.exists('profile'):
            profile, profile_ref = store.get('profile')
            need(profile['checkpoint'] == checkpoint, 'PROFILE_CHECKPOINT_MISMATCH')
            products = {n: store.read(n, ref) for n, ref in profile['products'].items()}
            need(set(products) == {'keys', 'contact_groups', 'missing_identity'}, 'PROFILE_PRODUCTS_MISSING')
            status = profile['status']
        else:
            store.event(phase, 'STARTED')
            products, status = analysis.profile(evidence, intent['summary'])
            refs = {}
            for n, frame in products.items():
                if store.exists(n):
                    refs[n] = store.capture(n); store.equal(frame, store.read(n, refs[n]))
                else:
                    refs[n] = store.save(n, frame)
                point('after_profile_' + n)
            profile = {'checkpoint': checkpoint, 'products': refs, 'status': status}
            point('before_profile_completion'); profile_ref = store.record('profile', profile)
        need(status['baseline'] == intent['summary'], 'PROFILE_BASELINE_MISMATCH')
        phase = 'RENDER'
        if store.exists('render'):
            render, _ = store.get('render')
            need(render['profile'] == profile_ref, 'RENDER_PROFILE_MISMATCH')
            pages = store.read('pages', render['pages'])
        else:
            pages = store.pages(dict(products, observations=evidence))
            if store.exists('pages'):
                pages_ref = store.capture('pages'); store.equal(pages, store.read('pages', pages_ref))
            else:
                pages_ref = store.save('pages', pages)
            render = {'profile': profile_ref, 'pages': pages_ref}
            store.record('render', render)
        phase = 'PUBLICATION'
        # Verify already-uploaded bytes before reusing an orphan or final receipt.
        # No data/profile recomputation: all HTML comes from pinned Delta pages.
        receipts, links, names = [], [], set()
        for name, raw in store.page_rows(pages):
            need(len(links) < MAX_PAGES and re.fullmatch(
                r'(keys|contact_groups|missing_identity|observations)-[0-9]{1,5}\.html', name)
                and name not in names, 'REPORT_PAGE_IDENTITIES')
            names.add(name)
            receipts.append(pub.publish(name, raw)); links.append((name, name)); point('during_publication')
        report = helpers.render_summary(status, digest(blob(checkpoint)), links,
            'SYNTHETIC ONLY' if backend is not None else 'RESTORED DEVRESULTS EVIDENCE', plan)
        report = report.replace('Native typed Parquet copies accompany these escaped presentation cards.',
            'Native typed evidence remains in pinned devresults Delta products; these cards are presentation only.')
        raw = report.encode()
        need(len(raw) <= MAX_BYTES, 'REPORT_INDEX_TOO_LARGE')
        receipts.append(pub.publish('index.html', raw))
        point('after_uploads')
        receipt_rows = store.receipt_frame(receipts)
        if store.exists('receipts'):
            receipts_ref = store.capture('receipts')
            store.equal(receipt_rows, store.read('receipts', receipts_ref))
        else:
            receipts_ref = store.save('receipts', receipt_rows)
        point('after_receipts_commit')
        publication = {'render': render, 'receipts': receipts_ref,
                       'artifact_count': len(receipts), 'ordered_receipts_sha256': digest(blob(receipts))}
        if store.exists('publication'):
            need(store.get('publication')[0] == publication, 'PUBLICATION_RECEIPT_MISMATCH')
        else:
            point('before_publication_completion'); store.record('publication', publication)
        store.event('WORKFLOW', 'COMPLETE')
        return {'status': 'COMPLETE', 'origin': 'RESTORED_DEVRESULTS', 'anchor': anchor,
                'publication': store.capture('publication'), 'private_report': receipts[-1]['path']}
    except BaseException as exc:
        if owned:
            try: store.event(phase, 'FAILED')
            except Exception:
                raise Stop('STAGE_' + phase + '_FAILED_AND_LEDGER_UNAVAILABLE') from None
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        if isinstance(exc, Stop): raise
        raise Stop('STAGE_' + phase + '_FAILED_NO_FALLBACK') from None
