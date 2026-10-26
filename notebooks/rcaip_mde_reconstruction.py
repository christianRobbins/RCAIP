"""Explicit version-pinned reconstruction, never a strict-route fallback.

No import-time I/O. No provenance witnesses are constructed. Existing metadata
is operator input; current identity/schema verification does not attest old use.
Would this exact analytical logic run for Paris? YES: scope/versions are inputs.
"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import re

import rcaip_mde_durable_bridge as bridge
import RCAIP_MDE_Durable_Input_Bridge as transport
import rcaip_mde_input_metadata as metadata

require = bridge.require
MODE = 'DECLARED_VERSION_PINNED_RECONSTRUCTION'
FORMAT = 'RCAIP_MDE_DECLARED_RECONSTRUCTION_V1'
LIMITATION = ('Reconstructed from explicitly version-pinned RCAIP inputs using the '
               'original analytical rules. The original DIAG input-version association '
               'was not independently verified. Agreement with retained outputs is '
               'reconciliation evidence, not proof of which snapshots the original '
               'execution read.')


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value)


def validate_selection(selection):
    require(isinstance(selection, dict) and set(selection) == {
        'mode', 'post', 'run', 'retained_sha256', 'inputs', 'selection_provenance'},
        'Explicit reconstruction selection required')
    require(selection['mode'] == MODE, 'Select declared reconstruction explicitly')
    require(isinstance(selection['post'], str) and re.fullmatch('[a-z0-9][a-z0-9_]*', selection['post'])
            and isinstance(selection['run'], str) and selection['run'].strip(), 'Selection scope')
    require(_hash(selection['retained_sha256']), 'Original retained hash required')
    require(isinstance(selection['inputs'], dict) and set(selection['inputs']) == set(bridge.INPUTS), 'Three explicit inputs required')
    provenance = selection['selection_provenance']
    require(isinstance(provenance, dict) and set(provenance) == {
        'selected_at_utc', 'selected_by', 'metadata_collected_at_utc', 'run_registry',
        'profile_registry', 'meaning'}, 'Present-day selection provenance required')
    require(provenance['meaning'] == LIMITATION, 'Reconstruction limitation required')
    for f in ('selected_at_utc', 'metadata_collected_at_utc'):
        require(isinstance(provenance[f], str), 'Selection time required')
        try: instant = datetime.fromisoformat(provenance[f])
        except ValueError: raise bridge.InputContractError('Selection time unsupported') from None
        require(instant.utcoffset() is not None and instant.utcoffset().total_seconds() == 0, 'Selection time must be explicit UTC')
    require(isinstance(provenance['selected_by'], str) and provenance['selected_by'].strip(), 'Explicit selecting operator required')
    rr, pr = provenance['run_registry'], provenance['profile_registry']
    for record, table in ((rr, metadata.RUN_REGISTRY), (pr, metadata.PROFILE_REGISTRY)):
        require(isinstance(record, dict) and record.get('origin') == table
                and type(record.get('metadata_snapshot_version')) is int and record['metadata_snapshot_version'] >= 0
                and isinstance(record.get('record'), dict), 'Existing registry origin/version required')
    run, profile = rr['record'], pr['record']
    require(run.get('run_id') == selection['run'] and profile.get('post_slug') == selection['post']
            and run.get('post_profile_key') == profile.get('post_profile_key'), 'Selection registry scope mismatch')
    ns = run.get('results_namespace')
    require(isinstance(ns, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?', ns), 'Registered namespace required')
    for name, binding in selection['inputs'].items():
        require(isinstance(binding, dict) and set(binding) == {
            'table', 'table_id', 'version', 'schema_sha256', 'schema', 'metadata_origin',
            'version_observed_in_history', 'schema_observed_at_selection'}, 'Reconstruction binding contract')
        table = ns + '.' + selection['post'] + '_' + metadata.STEMS[name]
        require(binding['table'] == table == binding['metadata_origin'], 'Registered reconstruction binding mismatch')
        require(isinstance(binding['table_id'], str) and binding['table_id']
                and type(binding['version']) is int and binding['version'] >= 0, 'Explicit Delta identity/version required')
        require(type(binding['version_observed_in_history']) is bool
                and binding['schema_observed_at_selection'] is True, 'Selection observation contract')
        bridge.check._table({'schema': binding['schema']})
        require(_hash(binding['schema_sha256']) and bridge.digest(bridge.encoded(binding['schema'])) == binding['schema_sha256'], 'Selection schema fingerprint mismatch')
    return deepcopy(selection)


def prepare_selection(metadata_result, *, mode, post, run, versions, retained_sha256, selected_by, spark):
    """Explicit schema/identity verification using the EXISTING metadata object.

    Does not recollect registries/history or read rows. In particular, an explicitly
    requested Control Assurance version is checked here, not assumed readable.
    """
    require(mode == MODE, 'Select declared reconstruction explicitly')
    require(isinstance(post, str) and re.fullmatch('[a-z0-9][a-z0-9_]*', post)
            and isinstance(run, str) and run.strip(), 'Selection scope')
    require(_hash(retained_sha256) and isinstance(selected_by, str) and selected_by.strip(), 'Retained hash and explicit operator required')
    require(isinstance(versions, dict) and set(versions) == set(bridge.INPUTS)
            and all(type(v) is int and v >= 0 for v in versions.values()), 'Explicit product versions required')
    require(hasattr(metadata_result, '_enterprise_evidence'), 'Existing enterprise metadata result required')
    evidence = metadata_result._enterprise_evidence
    require(isinstance(evidence, dict) and set(evidence.get('products', {})) == set(bridge.INPUTS), 'Existing product metadata required')
    rr, pr = evidence['run_registry'], evidence['profile_registry']
    require(rr['origin'] == metadata.RUN_REGISTRY and pr['origin'] == metadata.PROFILE_REGISTRY,
            'Existing registry origins mismatch')
    ns = rr['record'].get('results_namespace')
    require(isinstance(ns, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?', ns), 'Registered namespace required')
    require(rr['record']['run_id'] == run and pr['record']['post_slug'] == post
            and rr['record']['post_profile_key'] == pr['record']['post_profile_key'], 'Existing metadata scope mismatch')
    selection = dict(mode=mode, post=post, run=run, retained_sha256=retained_sha256, inputs={},
                     selection_provenance=dict(selected_at_utc=datetime.now(timezone.utc).isoformat(),
                         selected_by=selected_by, metadata_collected_at_utc=evidence['collected_at_utc'],
                         run_registry={k: deepcopy(rr[k]) for k in ('origin', 'metadata_snapshot_version', 'record')},
                         profile_registry={k: deepcopy(pr[k]) for k in ('origin', 'metadata_snapshot_version', 'record')}, meaning=LIMITATION))
    for name in bridge.INPUTS:
        old = evidence['products'][name]
        table = rr['record']['results_namespace'] + '.' + post + '_' + metadata.STEMS[name]
        require(old['origin'] == table and old['detail']['format'] == 'delta'
                and old['detail']['id'] == old['detail_after']['id'], 'Existing table binding/identity mismatch')
        binding = dict(table=table, table_id=old['detail']['id'], version=versions[name])
        transport._detail(spark, binding, bridge)
        frame = spark.read.format('delta').option('versionAsOf', versions[name]).table(table)
        fields = bridge.strict_json(frame.schema.json().encode())['fields']
        bridge.check._table({'schema': fields})
        fingerprint = bridge.digest(bridge.encoded(fields))
        if old['requested_version'] == versions[name] and 'schema_json' in old:
            observed = bridge.strict_json(old['schema_json'].encode())['fields']
            require(bridge.encoded(observed) == bridge.encoded(fields), 'Previously observed schema changed')
        transport._detail(spark, binding, bridge)
        selection['inputs'][name] = dict(binding, schema_sha256=fingerprint, schema=fields,
            metadata_origin=table, version_observed_in_history=versions[name] in [h['version'] for h in old['history']],
            schema_observed_at_selection=True)
    return validate_selection(selection)


def reconstruct(selection, inputs, original_raw):
    """Explicit materialized-input entry. No reads, fallback, new classifier or writes."""
    s = validate_selection(selection)
    m = {k: deepcopy(s[k]) for k in ('post', 'run', 'retained_sha256')}
    m['inputs'] = {name: {k: b[k] for k in ('table', 'table_id', 'version', 'schema_sha256')}
                   for name, b in s['inputs'].items()}
    bundle = bridge._reproduce_bound(m, inputs, original_raw)
    bundle.update(format=FORMAT, authority=MODE, limitation=LIMITATION, selection=s)
    bridge.encoded(bundle)
    return bundle


def validate_bundle(bundle, original_raw):
    require(isinstance(bundle, dict) and bundle.get('format') == FORMAT
            and bundle.get('authority') == MODE and bundle.get('limitation') == LIMITATION, 'Declared reconstruction bundle required')
    expected = reconstruct(bundle['selection'], bundle['inputs'], original_raw)
    require(bridge.encoded(expected) == bridge.encoded(bundle), 'Reconstruction semantic/provenance mismatch')
    return bundle


def load_bundle(path, *, expected_sha256, original_raw):
    require(_hash(expected_sha256), 'External reconstruction bundle hash required')
    with Path(path).open('rb') as stream: raw = stream.read(bridge.MAX_BYTES + 1)
    require(0 < len(raw) <= bridge.MAX_BYTES and bridge.digest(raw) == expected_sha256, 'External reconstruction bundle hash/size mismatch')
    return validate_bundle(bridge.strict_json(raw), original_raw)


def retain_bundle(bundle, destination, *, approved_directory, original_raw):
    validate_bundle(bundle, original_raw)
    dest, root = Path(destination).resolve(), Path(approved_directory).resolve()
    require(root.is_dir() and dest.parent == root and dest.suffix == '.json' and not dest.exists(), 'New approved reconstruction destination required')
    raw = bridge.encoded(bundle)
    with dest.open('xb') as stream: stream.write(raw)
    load_bundle(dest, expected_sha256=bridge.digest(raw), original_raw=original_raw)
    return dict(state='DECLARED_RECONSTRUCTION_RETAINED', sha256=bridge.digest(raw),
                mode=MODE, limitation=LIMITATION,
                live_assessment='NOT_CERTIFIED', formal_csa='NOT_REOPENED')
