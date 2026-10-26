"""Metadata-only retained MDE contract checker; never execute captured code.

This checks saved associations, not control conclusions or live authority. The
existing Cell 10/11/14 source fingerprints define fields and decision vocabulary.
Extra saved member tables may be recognized by those contracts, not display names.
No aggregate can replace a member table. No Delta fallback or workbook output.
"""
import ast
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

import rcaip_cell24_retained as retained

require = retained.require
InputContractError = retained.consumer.InputContractError
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 100000
FINGERPRINTS = {
    10: 'fae9f0c145677cb44038a982f1e0acce8ab4b7a3db6c429a6be85a6779ae80fa',
    11: '85119698461126cef6bf870eb726e9c7ecbbb397fdd7af760c525eb1f954829a',
    14: 'd2f4343d5b873ef873052180fe4eb1482d3a509daac62c4b2c35ade574d3c2ec',
}
PRODUCTS = {
    10: ('mde_qualification_df',),
    11: ('mde_current_gate_df',),
    14: ('mde_enterprise_case_summary_df', 'mde_enterprise_case_member_df',
         'case_shared_identity_support_df', 'mde_case_model_review_df',
         'mde_missing_compression_accounting_df'),
}
OPTIONAL = {'case_shared_identity_support_df': 'Shared-identity supporting evidence rows touching formed cases',
            'mde_case_model_review_df': 'Residual members requiring model review'}
GATE_COHORTS = {'CURRENT_IDENTITY_TEMPORAL_REVIEW', 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'}
RESIDUAL_GATE = 'CURRENT_APPLICABLE_ENTERPRISE_MDE_CONTROL_REVIEW_CANDIDATE'


def _marker(number):
    return f'RCAIP DIAG 001 - CELL {number}' + (' v1.1' if number == 14 else '') + ' CERTIFICATION'


def _assignments(source):
    return {n.targets[0].id: n.value for n in ast.parse(source).body
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)}


def _calls(node, method):
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute) and n.func.attr == method]


def _column(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str): return node.value
    require(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'alias', 'Unsupported reviewed column expression')
    return ast.literal_eval(node.args[0])


def _select(node):
    # Follow the output chain, not selects inside a joined supporting input.
    while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == 'select': return [_column(a) for a in node.args]
        node = node.func.value
    raise InputContractError('Unsupported reviewed select contract')


def contracts(sources):
    """AST inspection only, after pinned identity check. No eval/compile/exec."""
    a = {n: _assignments(s) for n, s in sources.items()}
    result = {}
    if 10 in a:
        result[PRODUCTS[10][0]] = list(ast.literal_eval(a[10]['REQUIRED_COLUMNS'])) + [
            ast.literal_eval(c.args[0]) for c in _calls(a[10]['mde_qualification_df'], 'withColumn')]
        # Cell 10 retains enrichment columns. These inherited fields are needed
        # to check later aliases; missing evidence is not silently skipped.
        for n in (11, 14):
            if n in a:
                result[PRODUCTS[10][0]] += sorted(ast.literal_eval(a[n]['REDISCOVERY_REQUIRED_COLUMNS']))
        result[PRODUCTS[10][0]] = list(dict.fromkeys(result[PRODUCTS[10][0]]))
    if 11 in a:
        result[PRODUCTS[11][0]] = list(dict.fromkeys(
            _select(a[11]['current_unresolved_rediscovery_df']) + _select(a[11]['mde_control_context_df']) +
            ['mde_current_analytical_gate', 'mde_current_gate_meaning']))
    if 14 in a:
        summary = a[14][PRODUCTS[14][0]]
        result[PRODUCTS[14][0]] = [ast.literal_eval(x) for x in _calls(summary, 'groupBy')[0].args] + [
            _column(x) for x in _calls(summary, 'agg')[0].args]
        for name in PRODUCTS[14][1:4]: result[name] = _select(a[14][name])
        result[PRODUCTS[14][4]] = ['stage_order', 'stage', 'count', 'interpretation']
    return result


def _field(cert, label):
    found = re.findall(r'^' + re.escape(label) + r':\s*([^\r\n]+)$', cert, re.M)
    require(len(found) == 1, 'Missing/ambiguous certification field: ' + label)
    return found[0].strip()


def _count(cert, label):
    value = _field(cert, label)
    require(re.fullmatch(r'0|[1-9][0-9]*', value) is not None, 'Invalid certification count: ' + label)
    return int(value)


def _equal_count(cert, label, expected):
    require(_count(cert, label) == expected, 'Certification mismatch: ' + label)


def _index(rows, key, product):
    result = {}
    for row in rows:
        value = row[key]
        require(isinstance(value, str) and value.strip() and value not in result,
                product + ': duplicate/blank member key')
        result[value] = row
    return result


def _same(left, right, label, aliases=()):
    for a, b in [(k, k) for k in left.keys() & right.keys()] + list(aliases):
        if a in left and b in right:
            require(retained._json(left[a]) == retained._json(right[b]), label + ': conflicting inherited evidence')


def _literal_decision_values(source, variable, field):
    calls = [c for c in _calls(_assignments(source)[variable], 'withColumn')
             if ast.literal_eval(c.args[0]) == field]
    require(len(calls) == 1, 'Unsupported reviewed decision field')
    # Literal branch results only, never evaluate conditions or assign decisions.
    values = []
    for call in ast.walk(calls[0].args[1]):
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
            pos = 1 if call.func.attr == 'when' else 0 if call.func.attr == 'otherwise' else None
            if pos is not None:
                result = call.args[pos]
                if isinstance(result, ast.Call) and isinstance(result.func, ast.Attribute) and result.func.attr == 'lit':
                    values.append(ast.literal_eval(result.args[0]))
    return values


def _decision_pair(source, variable, primary, text_field):
    """Extract code -> literal meaning/next-gate comparisons; not raw classifiers."""
    call = next(c for c in _calls(_assignments(source)[variable], 'withColumn')
                if ast.literal_eval(c.args[0]) == text_field)
    pairs = {}
    for n in ast.walk(call.args[1]):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == 'when':
            cond, value = n.args
            if isinstance(cond, ast.Compare) and isinstance(cond.comparators[0], ast.Call):
                pairs[ast.literal_eval(cond.comparators[0].args[0])] = ast.literal_eval(value.args[0])
    return pairs


def _check_qualification(rows, cert, source, post, run):
    index = _index(rows, 'asset_key', 'mde_qualification_df')
    vocab = set(_literal_decision_values(source, 'mde_qualification_df', 'mde_qualification_cohort'))
    maps = {f: _decision_pair(source, 'mde_qualification_df', 'mde_qualification_cohort', f)
            for f in ('mde_qualification_meaning', 'mde_next_gate')}
    counts = Counter()
    for r in rows:
        require(r['post_slug'] == post and r['rcaip_run_id'] == run, 'Qualification member scope mismatch')
        require(r['original_mde_assurance_state'] == 'MISSING', 'Qualification original assurance state mismatch')
        c = r['mde_qualification_cohort']; require(c in vocab, 'Unsupported qualification decision')
        require(c != 'UNMAPPED_REDISCOVERY_STATE', 'Unmapped qualification cannot support certified associations')
        for f, mapping in maps.items(): require(r[f] == mapping[c], 'Contradictory qualification decision: ' + f)
        require(r['potential_current_control_review'] is (c == 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'),
                'Contradictory potential control-review flag')
        require(r['current_identity_review_required'] is (c in ('CURRENT_IDENTITY_TEMPORAL_REVIEW', 'CURRENT_IDENTITY_EVIDENCE_LIMITATION')),
                'Contradictory identity-review flag')
        require(r['original_classification_preserved'] is True, 'Original classification not preserved')
        for f in ('assurance_case_changed', 'routing_changed', 'priority_changed', 'fuzzy_matching_allowed', 'ip_only_matching_allowed'):
            require(r[f] is None or r[f] is False, 'Qualification guard mismatch: ' + f)
        for f in ('resolved_mde_entity_count', 'current_mde_entity_count', 'stale_mde_entity_count', 'historical_mde_entity_count'):
            require(r[f] is None or type(r[f]) is int and r[f] >= 0, 'Invalid qualification count: ' + f)
        if c == 'CURRENT_MDE_EVIDENCE_CONFIRMED':
            require(r['enterprise_mde_name_evidence_found'] is True and (r['current_mde_entity_count'] or 0) > 0,
                    'Current-confirmed evidence contradiction')
        if c == 'NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION':
            # Spark NOT(NULL == CURRENTLY_OBSERVED) is NULL, not True.
            require(r['lifecycle_case_state'] is not None and r['lifecycle_case_state'] != 'CURRENTLY_OBSERVED', 'Noncurrent lifecycle contradiction')
        elif c != 'CURRENT_MDE_EVIDENCE_CONFIRMED':
            require(r['lifecycle_case_state'] == 'CURRENTLY_OBSERVED', 'Current qualification lifecycle contradiction')
        visibility = r['rediscovery_visibility_class']
        require(visibility not in ('CURRENT_RESOLVED', 'CURRENT_DURABLE_DEVICE_CHURN')
                or c == 'CURRENT_MDE_EVIDENCE_CONFIRMED', 'Qualification current-confirmation precedence contradiction')
        allowed_visibility = {
            'CURRENT_MDE_EVIDENCE_CONFIRMED': {'CURRENT_RESOLVED', 'CURRENT_DURABLE_DEVICE_CHURN'},
            'CURRENT_IDENTITY_TEMPORAL_REVIEW': {'IDENTITY_OR_TEMPORAL_REVIEW_REQUIRED'},
            'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY': {'STALE_REDISCOVERY', 'HISTORICAL_REDISCOVERY'},
            'CURRENT_IDENTITY_EVIDENCE_LIMITATION': {'INSUFFICIENT_IDENTITY_FOR_LOOKBACK', 'NO_ENTERPRISE_NAME_HIT'},
        }
        if c in allowed_visibility:
            require(visibility in allowed_visibility[c], 'Qualification visibility contradiction')
        counts[c] += 1
    for label in ('Durable MDE enrichment rows', 'Qualified rows', 'Qualified distinct asset keys'):
        _equal_count(cert, label, len(rows))
    for label, value in [('Qualification cohorts observed', len(counts)), ('Unmapped rediscovery states', 0),
                         ('Current MDE evidence confirmed', counts['CURRENT_MDE_EVIDENCE_CONFIRMED']),
                         ('Non-current lifecycle/evidence-limitation assets', counts['NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION']),
                         ('Potential current control-review candidates', counts['CURRENT_STALE_OR_HISTORICAL_MDE_ONLY']),
                         ('Current identity/evidence review required', counts['CURRENT_IDENTITY_TEMPORAL_REVIEW'] + counts['CURRENT_IDENTITY_EVIDENCE_LIMITATION'])]:
        _equal_count(cert, label, value)
    for label in ('Current-MDE-confirmed rows without name evidence', 'Current-MDE-confirmed rows without current MDE entity',
                  'Non-current cohort currently-observed errors', 'Original classification not preserved',
                  'Upstream assurance-case changes', 'Upstream routing changes', 'Upstream priority changes',
                  'Fuzzy matching enabled rows', 'IP-only matching enabled rows'):
        _equal_count(cert, label, 0)
    return index


def _check_gate(rows, cert, source):
    index = _index(rows, 'asset_key', 'mde_current_gate_df')
    allowed = _literal_decision_values(source, 'mde_current_gate_df', 'mde_current_analytical_gate')
    meanings = _decision_pair(source, 'mde_current_gate_df', 'mde_current_analytical_gate', 'mde_current_gate_meaning')
    for r in rows:
        code = r['mde_current_analytical_gate']
        require(code in allowed, 'Unsupported current-gate decision')
        require(code != 'MDE_GATE_MODEL_REVIEW_REQUIRED', 'Unmapped current gate cannot support certified associations')
        require(r['mde_qualification_cohort'] in GATE_COHORTS, 'Current-gate cohort contradiction')
        require(r['mde_current_gate_meaning'] == meanings[code], 'Contradictory current-gate meaning')
        applicability = r['ca_mde_applicability_state']
        require(applicability is None or isinstance(applicability, str), 'Invalid gate applicability evidence')
        require(r['ca_attribution_conflict_flag'] is None or type(r['ca_attribution_conflict_flag']) is bool,
                'Invalid gate attribution-conflict evidence')
        if code == 'MDE_NOT_APPLICABLE_BY_DEVICE_SCOPE':
            require(r['ca_mde_applicability_state'] == 'NOT_APPLICABLE', 'Gate applicability contradiction')
        else:
            require(r['ca_mde_applicability_state'] != 'NOT_APPLICABLE', 'Gate applicability precedence contradiction')
        if code == 'MDE_APPLICABILITY_REVIEW_REQUIRED':
            require(isinstance(r['ca_mde_applicability_state'], str) and 'REVIEW_REQUIRED' in r['ca_mde_applicability_state'], 'Gate applicability-review contradiction')
        elif code != 'MDE_NOT_APPLICABLE_BY_DEVICE_SCOPE':
            require('REVIEW_REQUIRED' not in (applicability or ''), 'Gate applicability-review precedence contradiction')
            if code == 'MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED':
                require(r['mde_qualification_cohort'] == 'CURRENT_IDENTITY_TEMPORAL_REVIEW', 'Gate identity/temporal contradiction')
            else:
                require(r['mde_qualification_cohort'] == 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'
                        and isinstance(applicability, str) and applicability.startswith('APPLICABLE'), 'Gate cohort/applicability contradiction')
                if code == 'MDE_IDENTITY_CONFIDENCE_REVIEW_REQUIRED':
                    require(r['ca_identity_strength'] in ('WEAK', 'UNKNOWN'), 'Gate identity-confidence contradiction')
                else:
                    require(r['ca_identity_strength'] not in ('WEAK', 'UNKNOWN'), 'Gate identity-confidence precedence contradiction')
                    if code == 'MDE_ATTRIBUTION_CONFLICT_REVIEW_REQUIRED':
                        require(r['ca_attribution_conflict_flag'] is True, 'Gate attribution-conflict contradiction')
                    else:
                        require(r['ca_attribution_conflict_flag'] in (None, False), 'Gate attribution-conflict precedence contradiction')
                        if code == 'MDE_OWNERSHIP_ATTRIBUTION_REVIEW_REQUIRED':
                            require(r['ca_operational_boundary_confidence'] != 'HIGH' or r['ca_management_model'] in (None, 'UNKNOWN', 'SHARED', 'NON_ASSET'), 'Gate ownership-review contradiction')
        if code.startswith('CURRENT_APPLICABLE_'):
            require(r['mde_qualification_cohort'] == 'CURRENT_STALE_OR_HISTORICAL_MDE_ONLY'
                    and isinstance(r['ca_mde_applicability_state'], str) and r['ca_mde_applicability_state'].startswith('APPLICABLE')
                    and r['ca_identity_strength'] not in ('WEAK', 'UNKNOWN')
                    and r['ca_attribution_conflict_flag'] in (None, False)
                    and r['ca_operational_boundary_confidence'] == 'HIGH'
                    and r['ca_management_model'] not in (None, 'UNKNOWN', 'SHARED', 'NON_ASSET'), 'Owner-qualified gate contradiction')
            owner = 'ENTERPRISE' if code == RESIDUAL_GATE else 'POST'
            require(r['ca_management_model'] == owner + '_MANAGED' or r['ca_management_owner'] == owner, 'Gate owner contradiction')
            if owner == 'ENTERPRISE':
                require(r['ca_management_model'] != 'POST_MANAGED' and r['ca_management_owner'] != 'POST', 'Gate Post-owner precedence contradiction')
    for label in ('Current unresolved rediscovery assets', 'Current unresolved distinct asset keys',
                  'Joined analytical-gate rows', 'Joined analytical-gate distinct assets'):
        _equal_count(cert, label, len(rows))
    for label in ('Current unresolved duplicate asset keys', 'Missing MDE Control Assurance context', 'Unmapped analytical-gate rows'):
        _equal_count(cert, label, 0)
    counts = Counter(r['mde_current_analytical_gate'] for r in rows)
    for label, codes in {
        'MDE not applicable by certified device scope': ['MDE_NOT_APPLICABLE_BY_DEVICE_SCOPE'],
        'MDE applicability review required': ['MDE_APPLICABILITY_REVIEW_REQUIRED'],
        'MDE identity resolution still required': ['MDE_IDENTITY_TEMPORAL_RESOLUTION_REQUIRED', 'MDE_IDENTITY_CONFIDENCE_REVIEW_REQUIRED'],
        'MDE attribution / ownership review required': ['MDE_ATTRIBUTION_CONFLICT_REVIEW_REQUIRED', 'MDE_OWNERSHIP_ATTRIBUTION_REVIEW_REQUIRED'],
        'Current applicable Post MDE control-review candidates': ['CURRENT_APPLICABLE_POST_MDE_CONTROL_REVIEW_CANDIDATE'],
        'Current applicable enterprise MDE control-review candidates': [RESIDUAL_GATE],
    }.items():
        _equal_count(cert, label, sum(counts[c] for c in codes))
    return index


def _check_native_decisions(products, source):
    node = _assignments(source)['case_member_df']
    rule_expression = next(c.args[1] for c in _calls(node, 'withColumn')
                           if ast.literal_eval(c.args[0]) == 'case_rule_key')
    signals = {}
    for branch in _calls(rule_expression, 'when'):
        condition, result = branch.args
        if isinstance(condition, ast.Compare) and isinstance(result, ast.Call) and result.func.attr == 'lit':
            signals[ast.literal_eval(result.args[0])] = ast.literal_eval(condition.comparators[0].args[0])
    # Bind already-supplied rules to their source evidence, including the first
    # shared-identity branch. Never compute a replacement rule or case.
    for row in products[PRODUCTS[14][1]] + products[PRODUCTS[14][3]]:
        types = row['shared_identity_types']
        require(isinstance(types, str), 'Native shared-identity summary unavailable')
        rule = row['case_rule_key']
        if types.strip(' '):
            require(rule == 'MDE_CROSS_ASSET_IDENTITY_REUSE::' + types, 'Native shared-identity rule precedence contradiction')
        elif rule == 'MDE_CASE_MODEL_REVIEW_REQUIRED':
            require(row['final_resolution_signal'] not in signals.values(), 'Native model-review evidence contradiction')
        else:
            require(rule in signals and row['final_resolution_signal'] == signals[rule], 'Native case rule/source signal contradiction')
    fields = ('case_family', 'relationship_outcome', 'recommended_treatment', 'residual_concern', 'priority_basis')
    results = {}
    for field in fields:
        call = next(c for c in _calls(node, 'withColumn') if ast.literal_eval(c.args[0]) == field)
        results[field] = call.args[1]
    for row in products[PRODUCTS[14][0]]:
        rule = row['case_rule_key']
        shared = isinstance(rule, str) and rule.startswith('MDE_CROSS_ASSET_IDENTITY_REUSE::')
        require(shared or rule in ('MDE_STALE_FULL_DURABLE_LINEAGE', 'MDE_STALE_SINGLE_ENTITY_NO_CURRENT_VISIBILITY',
                                  'MDE_HISTORICAL_SINGLE_ENTITY_NO_CURRENT_VISIBILITY'), 'Unsupported native case rule')
        if shared:
            suffix = rule.split('::', 1)[1]
            require(suffix and suffix == row['shared_identity_types'] and
                    set(suffix.split(',')) <= {'MDE_ENTITY_ID', 'HARDWARE_UUID', 'AAD_DEVICE_ID', 'MERGE_TARGET_DEVICE_ID'}, 'Unsupported shared identity rule')
        # Validate code-to-code/narrative equivalence only. No feature classifier.
        for field, expr in results.items():
            chain = []
            while isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
                if expr.func.attr == 'when': chain.append(expr.args)
                expr = expr.func.value
            expected = None
            for cond, value in reversed(chain):
                text = ast.unparse(cond)
                match = ('startswith' in text and shared or
                         isinstance(cond, ast.Compare) and isinstance(cond.ops[0], ast.Eq) and rule in text or
                         '.isin(' in text and rule in text or
                         isinstance(cond, ast.Compare) and isinstance(cond.ops[0], ast.NotEq) and rule != 'MDE_CASE_MODEL_REVIEW_REQUIRED')
                if match:
                    if isinstance(value, ast.Call) and value.func.attr == 'lit': expected = ast.literal_eval(value.args[0])
                    elif field == 'relationship_outcome' and shared: expected = 'CROSS_ASSET_DURABLE_IDENTITY_REUSE:' + suffix
                    break
            require(expected is not None and row[field] == expected, 'Native case decision contradiction: ' + field)


def _check_cases(products, cert):
    cases, members, support, review, accounting = [products[p] for p in PRODUCTS[14]]
    ci = _index(cases, 'case_id', 'Case summaries')
    mi = _index(members, 'asset_key', 'Case members')
    ri = _index(review, 'asset_key', 'Model review')
    require(not mi.keys() & ri.keys(), 'Case/model-review member overlap')
    for m in members:
        require(m['case_id'] in ci, 'Orphan case member')
        _same(m, ci[m['case_id']], 'Case/member')
    for c in cases:
        require(c['case_class'] == 'ENTERPRISE_RECONCILIATION_REFERRAL' and c['primary_customer'] == 'ENTERPRISE_SERVICE_OWNER',
                'Native case routing/class mismatch')
        matched = [m for m in members if m['case_id'] == c['case_id']]
        keys = c['case_member_asset_keys']
        require(isinstance(keys, list) and all(isinstance(k, str) for k in keys) and len(keys) == len(set(keys))
                and set(keys) == {m['asset_key'] for m in matched} and matched, 'Native case membership mismatch')
        require(type(c['affected_assets']) is int and c['affected_assets'] == len(matched), 'Case affected-assets mismatch')
        require(c['distinct_serials'] == len({m['canonical_serial'] for m in matched if m['canonical_serial'] is not None}), 'Case serial count mismatch')
        for field, member in [('resolved_mde_entities', 'resolved_mde_entity_count'), ('stale_mde_entities', 'stale_mde_entity_count'), ('historical_mde_entities', 'historical_mde_entity_count')]:
            require(all(m[member] is None or type(m[member]) is int and m[member] >= 0 for m in matched), 'Invalid native entity count')
            require(c[field] == sum(m[member] or 0 for m in matched), 'Case entity aggregation mismatch')
        for field, member in [('rediscovery_recency', 'latest_rediscovered_mde_recency'),
                              ('active_source_patterns', 'ca_source_mask_active'),
                              ('visibility_states', 'ca_post_visibility_state'),
                              ('attribution_confidence_states', 'ca_attribution_confidence'),
                              ('shared_identity_types', 'shared_identity_types')]:
            values = {m[member] for m in matched if m[member] is not None
                      and (member != 'shared_identity_types' or m[member].strip())}
            require(all(isinstance(v, str) for v in values) and c[field] == ', '.join(sorted(values)), 'Case evidence aggregation mismatch')
        require(all(type(m['maximum_related_assets']) is int and m['maximum_related_assets'] >= 0 for m in matched)
                and c['max_assets_related_by_shared_identity'] == max(m['maximum_related_assets'] for m in matched), 'Case supporting-relationship maximum mismatch')
    seen = set()
    for r in support:
        key = tuple(r[f] for f in ('case_id', 'case_member_asset_key', 'identity_type', 'shared_identity_value'))
        require(all(isinstance(k, str) and k for k in key) and key not in seen, 'Invalid/duplicate supporting relationship')
        seen.add(key)
        require(r['case_member_asset_key'] in mi and mi[r['case_member_asset_key']]['case_id'] == r['case_id'], 'Orphan support relationship')
        keys = r['related_asset_keys']
        require(isinstance(keys, list) and all(isinstance(k, str) for k in keys) and len(keys) == len(set(keys))
                and r['case_member_asset_key'] in keys and type(r['related_asset_count']) is int
                and r['related_asset_count'] == len(keys) and len(keys) > 1, 'Support membership/count mismatch')
    for r in review: require(r['case_rule_key'] == 'MDE_CASE_MODEL_REVIEW_REQUIRED', 'Unsupported model-review decision')
    stages = {r['stage_order']: r for r in accounting}
    require(len(accounting) == 9 and set(stages) == set(range(1, 10)), 'Accounting stage contract mismatch')
    labels = ['Starting MDE MISSING population', 'Current MDE evidence confirmed', 'Non-current lifecycle/evidence-limitation',
              'Current identity/temporal review outside control-case set', 'Owner-qualified enterprise residual candidates',
              'Residual case members formed', 'Deterministic enterprise assurance cases',
              'RCSO MDE control cases from this branch', 'Post DT MDE actions from this branch']
    for i, label in enumerate(labels, 1):
        require(type(stages[i]['count']) is int and stages[i]['count'] >= 0, 'Invalid accounting count')
        _equal_count(cert, label, stages[i]['count'])
    require(stages[1]['count'] == sum(stages[i]['count'] for i in (2, 3, 4, 5)), 'Population accounting mismatch')
    require(stages[5]['count'] == len(mi) + len(ri) and stages[6]['count'] == len(mi)
            and stages[7]['count'] == len(ci) and stages[8]['count'] == stages[9]['count'] == 0, 'Residual accounting mismatch')
    for label, value in [('Residual members requiring model review', len(ri)), ('Enterprise service-owner cases', len(ci)),
                         ('Shared-identity supporting evidence rows touching formed cases', len(support)), ('Assets assigned to multiple cases', 0)]:
        _equal_count(cert, label, value)
    for label in ('Original classification not preserved', 'Upstream assurance-case changes', 'Upstream routing changes',
                  'Upstream priority changes', 'Fuzzy matching enabled', 'IP-only matching enabled'):
        _equal_count(cert, label, 0)
    require(_field(cert, 'Case formation readiness') == ('READY' if not ri else 'PARTIAL_MODEL_REVIEW_REQUIRED'), 'Case readiness mismatch')
    for label, n in [('Compression reconciliation', stages[1]['count']), ('Residual member reconciliation', len(mi) + len(ri))]:
        require(_field(cert, label) == f'{n} / {n}', 'Saved reconciliation mismatch: ' + label)
    return mi, ri, stages


def _residual_evidence(start, gate):
    """Cell 14 residual-membership assertion only, NOT a Cell 11 gate rule.

    True is used only to reconcile a supplied key set; no new decision is emitted.
    Null-aware equivalents of the pinned residual_df filters are intentional.
    """
    return (start['lifecycle_case_state'] == 'CURRENTLY_OBSERVED'
            and start['rediscovery_visibility_class'] in ('STALE_REDISCOVERY', 'HISTORICAL_REDISCOVERY')
            and (start['current_mde_entity_count'] or 0) == 0
            and isinstance(gate['ca_mde_applicability_state'], str)
            and gate['ca_mde_applicability_state'].startswith('APPLICABLE')
            and gate['ca_identity_strength'] is not None and gate['ca_identity_strength'] not in ('WEAK', 'UNKNOWN')
            and (gate['ca_attribution_conflict_flag'] is None or gate['ca_attribution_conflict_flag'] is False)
            and gate['ca_operational_boundary_confidence'] == 'HIGH'
            and (gate['ca_management_model'] == 'ENTERPRISE_MANAGED' or gate['ca_management_owner'] == 'ENTERPRISE'))


def _check_shared_support(start, members, support, source):
    # Field map is read from the pinned producer, not a new identity registry.
    field_map = dict(ast.literal_eval(_assignments(source)['IDENTITY_ARRAYS']))
    by_member = {key: [] for key in members}
    claims = {}
    for row in support:
        kind = row['identity_type']
        require(kind in field_map, 'Shared support: unsupported identity type')
        token = row['shared_identity_value']
        require(isinstance(token, str) and token and token == token.strip(' '), 'Shared support: invalid retained token')
        norm = token.lower()  # Spark lower(trim(string)); trim removes ASCII spaces.
        claim = (kind, norm)
        related = row['related_asset_keys']
        require(related == sorted(related), 'Shared support: related-key order contradicts producer')
        require(set(related) <= start.keys(), 'Shared support: foreign related asset')
        # Verify only this ALREADY CLAIMED token. Do not enumerate new token
        # matches, expand the candidate population or create relationships.
        corroborated = set()
        original_spelling_present = False
        for key, asset in start.items():
            values = asset[field_map[kind]]
            require(values is None or isinstance(values, list) and all(v is None or isinstance(v, str) for v in values),
                    'Shared support: unsupported retained identity evidence')
            trimmed = {v.strip(' ') for v in (values or []) if v is not None and v.strip(' ')}
            if any(v.lower() == norm for v in trimmed): corroborated.add(key)
            if key in related and token in trimmed: original_spelling_present = True
        require(corroborated == set(related) and original_spelling_present,
                'Shared support: claimed token/related keys not corroborated by retained identity evidence')
        # F.first can choose a retained case variant; it must be the SAME
        # representative and exact related set on every support row for that token.
        signature = (token, tuple(related), row['related_asset_count'])
        require(claim not in claims or claims[claim] == signature, 'Shared support: inconsistent token relationship')
        claims[claim] = signature
        owner = row['case_member_asset_key']
        require(owner in by_member, 'Shared support: orphan case member')
        require(not any((r['identity_type'], r['shared_identity_value'].lower()) == claim for r in by_member[owner]),
                'Shared support: duplicate normalized relationship')
        by_member[owner].append(row)
    for (kind, norm), (_, related, _) in claims.items():
        for key in set(related) & members.keys():
            require(any(r['identity_type'] == kind and r['shared_identity_value'].lower() == norm for r in by_member[key]),
                    'Shared support: missing reciprocal case-member relationship')
    for key, member in members.items():
        rows = by_member[key]
        require(member['shared_identity_types'] == ','.join(sorted({r['identity_type'] for r in rows})),
                'Shared support: member identity-type summary mismatch')
        require(member['shared_identity_values'] == ' || '.join(sorted({r['identity_type'] + ':' + r['shared_identity_value'] for r in rows})),
                'Shared support: member identity-value summary mismatch')
        require(member['maximum_related_assets'] == max((r['related_asset_count'] for r in rows), default=0),
                'Shared support: member related-count summary mismatch')


def _table(payload):
    require(isinstance(payload, dict), 'Invalid structured output envelope')
    fields = payload.get('schema')
    if isinstance(fields, dict):
        require(fields.get('type') == 'struct', 'Unsupported structured schema')
        fields = fields.get('fields')
    require(isinstance(fields, list) and all(isinstance(f, dict) and isinstance(f.get('name'), str) and 'type' in f for f in fields), 'Invalid structured fields')
    cols = [f['name'] for f in fields]
    require(len(cols) == len(set(cols)), 'Duplicate structured columns')
    kinds = [retained._schema_type(f['type']) for f in fields]
    return cols, kinds


def inspect_retained_bytes(raw, *, expected_input_sha256, expected_post, expected_run):
    """Only whitelisted structural statuses escape; no counts/values/raw metadata."""
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'Retained artifact byte bound exceeded or empty')
    require(isinstance(expected_input_sha256, str) and re.fullmatch(r'[0-9a-f]{64}', expected_input_sha256), 'Explicit artifact hash required')
    require(isinstance(expected_post, str) and re.fullmatch(r'[a-z0-9][a-z0-9_-]*', expected_post)
            and isinstance(expected_run, str) and expected_run.strip(), 'Explicit canonical scope required')
    require(hashlib.sha256(raw).hexdigest() == expected_input_sha256, 'Retained artifact hash mismatch')
    def invalid(_): raise InputContractError('Nonfinite retained JSON number')
    try:
        book = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=retained._ObjectPairs,
                          parse_float=Decimal, parse_int=lambda s: Decimal(s) if s == '-0' else int(s), parse_constant=invalid)
    except (UnicodeError, json.JSONDecodeError):
        raise InputContractError('Invalid retained JSON encoding') from None
    book = retained._resolve_notebook_json(book, (), [])
    require(isinstance(book, dict) and book.get('nbformat') == 4 and isinstance(book.get('cells'), list), 'Unsupported notebook envelope')
    selected, sources, certs = {}, {}, {}
    for index, cell in enumerate(book['cells']):
        require(isinstance(cell, dict), 'Invalid notebook cell')
        if cell.get('cell_type') != 'code': continue
        source = retained._joined(cell.get('source', ''))
        for n in PRODUCTS:
            if _marker(n) not in source: continue
            require(n not in selected, 'Ambiguous retained producer cell')
            try: digest = retained.producer_fingerprint(source)
            except SyntaxError: raise InputContractError('Invalid retained producer source') from None
            require(digest == FINGERPRINTS[n], f'Cell {n}: unexpected executable source')
            outputs = cell.get('outputs'); require(isinstance(outputs, list), f'Cell {n}: missing outputs')
            streams = []
            for out in outputs:
                require(isinstance(out, dict) and out.get('output_type') in ('stream', 'display_data', 'execute_result'), f'Cell {n}: invalid/error output')
                if out['output_type'] == 'stream':
                    require(out.get('name') == 'stdout', f'Cell {n}: unsupported certification stream')
                    streams.append(retained._joined(out.get('text', '')))
            cert = ''.join(streams)
            require(cert.count(_marker(n)) == 1 and _field(cert, 'Status') == 'PASS', f'Cell {n}: certification not unambiguous PASS')
            require(_field(cert, 'Post').lower() == expected_post and _field(cert, 'Run ID') == expected_run, f'Cell {n}: certification scope mismatch')
            selected[n] = (index, outputs); sources[n] = source; certs[n] = cert
    schemas = contracts(sources)
    products, report = {}, []
    for n, names in PRODUCTS.items():
        for name in names:
            item = {'logical_product': name, 'source_cell': n, 'notebook_cell_index': selected[n][0] if n in selected else None,
                    'output_index': None, 'status': 'NOT_RETAINED_IN_THIS_ARTIFACT', 'candidate_structure_found': False,
                    'missing_required_fields': [], 'next_input_requirement': 'Source-bound complete member product and matching saved certification'}
            report.append(item)
            if n not in selected:
                item['next_input_requirement'] = 'Reviewed producer cell and saved certification'; continue
            required = set(schemas[name]); candidates = []; closest = set()
            for oi, out in enumerate(selected[n][1]):
                metadata = out.get('metadata', {}); require(isinstance(metadata, dict), 'Invalid output metadata envelope')
                payload = metadata.get(retained.MIME)
                if payload is None: continue
                cols, kinds = _table(payload)
                available = required & set(cols)
                if len(available) > len(closest): closest = available
                if required <= set(cols): candidates.append((oi, payload, cols, kinds))
            item['missing_required_fields'] = sorted(required - closest)
            require(len(candidates) <= 1, name + ': ambiguous candidate products')
            if not candidates:
                if name in OPTIONAL and _count(certs[n], OPTIONAL[name]) == 0:
                    products[name] = []; item.update(status='CERTIFIED_ZERO_NOT_DISPLAYED', next_input_requirement='None for this optional zero product')
                continue
            oi, payload, cols, kinds = candidates[0]
            item.update(candidate_structure_found=True, output_index=oi, missing_required_fields=[], status='CANDIDATE_STRUCTURE_FOUND')
            require(payload.get('overflow') is False, name + ': incomplete/overflowed output')
            rows = payload.get('data'); require(isinstance(rows, list) and len(rows) <= MAX_ROWS, name + ': invalid/bounded row transport')
            records = []
            for row in rows:
                require(isinstance(row, list) and len(row) == len(cols), name + ': invalid positional row')
                for value, kind in zip(row, kinds): retained._typed(value, kind)
                records.append(dict(zip(cols, row)))
            products[name] = records
    start = gate = native = None
    if PRODUCTS[10][0] in products:
        start = _check_qualification(products[PRODUCTS[10][0]], certs[10], sources[10], expected_post, expected_run)
    if PRODUCTS[11][0] in products:
        gate = _check_gate(products[PRODUCTS[11][0]], certs[11], sources[11])
    if all(p in products for p in PRODUCTS[14]):
        _check_native_decisions(products, sources[14])
        definitions = _assignments(sources[14])['compression_rows'].elts
        for row in products[PRODUCTS[14][4]]:
            require(type(row['stage_order']) is int and 1 <= row['stage_order'] <= len(definitions), 'Invalid accounting stage order')
            definition = definitions[row['stage_order'] - 1]
            require(row['stage'] == ast.literal_eval(definition.elts[1]) and row['interpretation'] == ast.literal_eval(definition.elts[3]), 'Accounting stage meaning mismatch')
        native = _check_cases(products, certs[14])
    if start is not None and gate is not None:
        expected = {k for k, r in start.items() if r['mde_qualification_cohort'] in GATE_COHORTS}
        require(gate.keys() == expected, 'Qualified current subset membership mismatch')
        aliases = [(f, 'rediscovery_' + f) for f in ('canonical_name', 'canonical_serial', 'canonical_ip', 'management_owner', 'operational_boundary')]
        for key in gate: _same(start[key], gate[key], 'Qualification/current gate', aliases)
    complete = start is not None and gate is not None and native is not None
    if complete:
        mi, ri, stages = native
        for key in mi.keys() | ri.keys():
            require(key in gate and key in start and _residual_evidence(start[key], gate[key]),
                    'Native residual evidence contradiction (Cell 14 predicates, including no current MDE)')
        require((mi.keys() | ri.keys()) == {k for k in gate if _residual_evidence(start[k], gate[k])}, 'Native residual/current gate membership mismatch')
        for key, member in {**mi, **ri}.items():
            _same(start[key], member, 'Qualification/native member')
            _same(gate[key], member, 'Current gate/native member', [('rediscovery_' + f, f) for f in ('canonical_name', 'canonical_serial', 'canonical_ip')])
        case_index = {c['case_id']: c for c in products[PRODUCTS[14][0]]}
        for key, member in mi.items():
            _same(gate[key], case_index[member['case_id']], 'Current gate/native case')
        require(stages[1]['count'] == len(start), 'Starting membership/accounting mismatch')
        for stage, code in [(2, 'CURRENT_MDE_EVIDENCE_CONFIRMED'), (3, 'NONCURRENT_LIFECYCLE_OR_EVIDENCE_LIMITATION')]:
            require(stages[stage]['count'] == sum(r['mde_qualification_cohort'] == code for r in start.values()), 'Recorded outcome/accounting mismatch')
        _check_shared_support(start, mi, products[PRODUCTS[14][2]], sources[14])
    for item in report:
        if item['status'] == 'CANDIDATE_STRUCTURE_FOUND':
            item['status'] = 'VALIDATED_RETAINED_ASSOCIATIONS' if complete else 'CANDIDATE_STRUCTURE_FOUND_DEPENDENCIES_UNVERIFIED'
            item['next_input_requirement'] = 'None for checked associations; not product/live certification' if complete else 'Missing companion member products; no membership claim from counts'
    return {'artifact_scope': 'ONLY_THE_EXACT_HASH_VERIFIED_ARTIFACT',
            'state': 'RETAINED_ASSOCIATIONS_VALIDATED' if complete else 'CORE_ASSOCIATIONS_NOT_ESTABLISHED',
            'products': report,
            'inventory_comparison': {'status': 'NOT_ESTABLISHED_BY_REVIEWED_CONTRACTS',
                'known_available_fields': sorted({f for rows in products.values() for r in rows for f in r
                    if f in ('canonical_name', 'canonical_serial', 'canonical_ip', 'latest_rediscovered_mde_device_name', 'latest_rediscovered_mde_machine_group')}),
                'next_input_requirement': 'Governed paired CMDB assignment and tool attribution meanings, references, times and identity link; name/machine group is not location'},
            'authority': 'Historical retained-evidence check only; no workbook, current-state or delivery certification'}


def inspect_retained_file(path, **scope):
    path = Path(path)
    require(path.suffix.lower() == '.ipynb', 'Explicit retained .ipynb path required')
    with path.open('rb') as stream: raw = stream.read(MAX_BYTES + 1)
    return inspect_retained_bytes(raw, **scope)
