"""Inert operator entry point: run(..., mode='PLAN_ONLY') is the default.

Use the existing Department validation namespace. Never reset old/new guards.
This source contains no automatic call, data read, approval or write.
"""
import hashlib
from types import ModuleType

VALIDATOR_SHA = '70ce7c022a0ae39301eceba6c9989efc920517241de57034588af8f7a25b0988'
PREFIX = 'RCAIP_INTUNE_DISTRIBUTED_'


def run(namespace, *, validator_source, mode='PLAN_ONLY', exact_plan_id=None,
        allow_values=False, allow_full_snapshot_scan=False, authorization_reference=None):
    """Return fixed public status; retain private plans/results/errors in namespace.

    Every execution request consumes the new caller guard, even on refusal.
    No prior attempt, approval, exception or result is overwritten.
    """
    def require(ok, code):
        if not ok:
            raise RuntimeError(code)
    try:
        require(mode in ('PLAN_ONLY', 'EXECUTE'), 'MODE_REQUIRED')
        require(hashlib.sha256(validator_source.replace('\r\n', '\n').encode()).hexdigest() == VALIDATOR_SHA,
                'VALIDATOR_SOURCE_MISMATCH')
        needed = ('RCAIP_INTUNE_PLAN', 'RCAIP_INTUNE_ENGINE_SOURCE',
                  'RCAIP_INTUNE_CONFIG_SOURCE', 'RCAIP_INTUNE_VALUE_ATTEMPT')
        require(all(k in namespace for k in needed), 'RETAINED_CONTEXT_REQUIRED_NO_RELOAD')
        require(namespace['RCAIP_INTUNE_VALUE_ATTEMPT'].get('execute_dispatched') is True,
                'PRIOR_ATTEMPT_RECORD_REQUIRED')
        require(PREFIX+'ATTEMPT' not in namespace, 'NEW_ATTEMPT_ALREADY_RETAINED')
        if PREFIX+'MODULE' not in namespace:
            module = ModuleType('reviewed_distributed_intune')
            exec(compile(validator_source, '<reviewed-validator>', 'exec'), module.__dict__)
            namespace[PREFIX+'MODULE'] = module
            namespace[PREFIX+'PLAN'] = module.prepare_distributed(
                namespace['RCAIP_INTUNE_PLAN'], engine_source=namespace['RCAIP_INTUNE_ENGINE_SOURCE'],
                config_source=namespace['RCAIP_INTUNE_CONFIG_SOURCE'])
        module, plan = namespace[PREFIX+'MODULE'], namespace[PREFIX+'PLAN']
        rebuilt = module.prepare_distributed(namespace['RCAIP_INTUNE_PLAN'],
                    engine_source=namespace['RCAIP_INTUNE_ENGINE_SOURCE'],
                    config_source=namespace['RCAIP_INTUNE_CONFIG_SOURCE'])
        require(plan.plan_id == module.digest(plan.private) == rebuilt.plan_id, 'CHANGED_PLAN')
        if mode == 'PLAN_ONLY':
            return plan.status()
        attempt = module.DistributedAttempt()
        namespace[PREFIX+'ATTEMPT'] = attempt
        require(isinstance(authorization_reference, str) and authorization_reference.strip() and
                authorization_reference != namespace['RCAIP_INTUNE_VALUE_ATTEMPT'].get('authorization_reference'),
                'NEW_APPROVAL_REFERENCE_REQUIRED')
        approval = module.approve(plan, exact_plan_id=exact_plan_id,
                    allow_values=allow_values, allow_full_snapshot_scan=allow_full_snapshot_scan)
        namespace[PREFIX+'APPROVAL'] = dict(approval, authorization_reference=authorization_reference)
        result = module.execute_distributed(plan, approval=approval, attempt=attempt,
                    spark=namespace['spark'], engine_source=namespace['RCAIP_INTUNE_ENGINE_SOURCE'],
                    config_source=namespace['RCAIP_INTUNE_CONFIG_SOURCE'])
        namespace[PREFIX+'RESULT'] = result
        return {**attempt.status(), **result.status()}
    except (Exception, KeyboardInterrupt) as error:
        # No exception text, private values, query plans or arbitrary categories.
        # Preserve the first failure rather than replacing it during a refused retry.
        namespace.setdefault(PREFIX+'EXCEPTION', error)
        attempt = namespace.get(PREFIX+'ATTEMPT')
        return {'status': 'STOP', 'error_code': 'PRIVATE_DISTRIBUTED_FAILURE_RETAINED',
                'stage': attempt.stage if attempt else 'PRECONDITION',
                'execute_dispatched': bool(attempt and attempt.started),
                'complete_result_retained': bool(attempt and attempt.private_result is not None),
                'retry_authorized': False, 'delta_writes': 'NONE', 'assessment_refresh': False}
