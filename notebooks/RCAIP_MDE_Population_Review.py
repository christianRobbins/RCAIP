# Databricks notebook source
"""Explicit operator functions. Import does not load helpers or execute anything.

Separate calls: prepare -> reconstruct -> retain -> externally hash-bound reopen
and workbook. No operation automatically performs the next operation. Original
DBFS input is reused, never re-exported. Share receipts/status, not bundle rows.
"""
import hashlib
import importlib.util
from pathlib import Path
import sys

EXPECTED_MODULES = {
    'rcaip_cell24_presentation': 'c2d54f4b019a1630965a6c0b947d7fab54bf86cf76f236b4e352ddc59f8736d6',
    'rcaip_cell24_retained': '326fb26c2c8abbbf52af76fe104e0fec119ac083efa3abb954c8f2ca1276b55e',
    'rcaip_mde_retained_check': 'feb72cd4cb05743276cd6d01cf874ea6eab108b4d70cf8f9964bc0013cf6eb8c',
    'rcaip_mde_durable_bridge': '6e90125e0a2ae65e21126c3ffc25246638b3d2d87600bdd6bb804bafc78ad7ef',
    'RCAIP_MDE_Durable_Input_Bridge': '4ba2433dff506ba2d9c1f32775545c22cf17d62a847fbcdc980345212123c61b',
    'rcaip_mde_input_metadata': 'd5be9dabc76f43efaf92ad79d46bdf796b5ca3598983030aa78e37234f1ceab6',
    'rcaip_mde_reconstruction': '0be1a9b8dac8a46c1d3585dfb68e5cff353995e03002fce0b5126f503df9b3a1',
    'rcaip_mde_population': '46f0667c1326deb8dfca9ab872f846a72a178f3e1bc0d925fb89e4fdea4fceb6',
}


class PopulationOperationError(RuntimeError):
    """Sanitized fatal public boundary; no ordinary unavailable fallback."""


def _safe(stage, operation):
    try: return operation()
    except (MemoryError, RecursionError): raise MemoryError('MDE population resource failure: '+stage) from None
    except KeyboardInterrupt: raise KeyboardInterrupt('MDE population interrupted: '+stage) from None
    except SystemExit: raise SystemExit('MDE population terminated: '+stage) from None
    except Exception: raise PopulationOperationError(stage+': STOP; inspect the cause only inside the approved environment') from None


def _modules(folder):
    folder=Path(folder); sources={}
    for name,expected in EXPECTED_MODULES.items():
        text=(folder/(name+'.py')).read_bytes().decode('utf-8-sig').replace('\r\n','\n').replace('\r','\n').rstrip('\n')+'\n'
        if hashlib.sha256(text.encode()).hexdigest()!=expected: raise PopulationOperationError('Helper hash mismatch')
        sources[name]=text
    previous={name:sys.modules.get(name) for name in sources};modules={}
    try:
        for name,text in sources.items():
            spec=importlib.util.spec_from_file_location(name,folder/(name+'.py'))
            module=importlib.util.module_from_spec(spec);sys.modules[name]=module
            exec(compile(text,str(folder/(name+'.py')),'exec'),module.__dict__)
            modules[name]=module
        return modules
    finally:
        for name,old in previous.items():
            if old is None:sys.modules.pop(name,None)
            else:sys.modules[name]=old


def prepare(metadata_result, *, module_folder, spark, mode, post, run, versions, retained_sha256, selected_by):
    """Only explicit version-schema/identity checks. Uses existing metadata result."""
    m=_safe('HELPER_LOADING',lambda:_modules(module_folder))
    return _safe('SELECTION_SCHEMA_CHECK',lambda:m['rcaip_mde_reconstruction'].prepare_selection(
        metadata_result,mode=mode,post=post,run=run,versions=versions,retained_sha256=retained_sha256,selected_by=selected_by,spark=spark))


def reconstruct(selection, *, module_folder, spark, input_route, source_uri=None, retained_path=None, client=None):
    m=_safe('HELPER_LOADING',lambda:_modules(module_folder))
    core=m['rcaip_mde_reconstruction'];bridge=m['rcaip_mde_durable_bridge'];io=m['RCAIP_MDE_Durable_Input_Bridge']
    s=_safe('EXPLICIT_SELECTION',lambda:core.validate_selection(selection))
    original=_safe('ORIGINAL_TRANSPORT',lambda:io._retained_bytes(bridge,s['retained_sha256'],input_route=input_route,source_uri=source_uri,retained_path=retained_path,client=client))
    _safe('ORIGINAL_CASE_CONTRACT',lambda:bridge.load_original(original,s))
    inputs=_safe('PINNED_CANDIDATE_BOUND_READ',lambda:io._read_snapshots(bridge,spark,s))
    bundle=_safe('RECONSTRUCTION_RECONCILIATION',lambda:core.reconstruct(s,inputs,original))
    # Explicit private return; never display this result in shared output.
    return bundle,original


def retain(bundle, destination, *, module_folder, approved_directory, original_raw):
    m=_safe('HELPER_LOADING',lambda:_modules(module_folder))
    return _safe('RECONSTRUCTION_RETENTION',lambda:m['rcaip_mde_reconstruction'].retain_bundle(
        bundle,destination,approved_directory=approved_directory,original_raw=original_raw))


def publish_saved(bundle_path, destination, *, expected_bundle_sha256, module_folder, approved_directory,
                  expected_original_sha256, input_route, source_uri=None, retained_path=None, client=None, synthetic=False):
    m=_safe('HELPER_LOADING',lambda:_modules(module_folder))
    bridge=m['rcaip_mde_durable_bridge'];io=m['RCAIP_MDE_Durable_Input_Bridge']
    original=_safe('ORIGINAL_TRANSPORT',lambda:io._retained_bytes(bridge,expected_original_sha256,input_route=input_route,source_uri=source_uri,retained_path=retained_path,client=client))
    bundle=_safe('EXTERNAL_HASH_AND_SEMANTICS',lambda:m['rcaip_mde_reconstruction'].load_bundle(bundle_path,expected_sha256=expected_bundle_sha256,original_raw=original))
    return _safe('WORKBOOK',lambda:m['rcaip_mde_population'].save_workbook(bundle,destination,
        approved_directory=approved_directory,original_raw=original,synthetic=synthetic))
