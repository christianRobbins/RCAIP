# Databricks notebook source
"""Explicit retained-file check only. Import performs no I/O or SDK calls.

Deploy with the three hash-pinned helpers. Invoke check_mde_retained_input only
after separate enterprise approval, passing the exact operator input parameters.
No Workspace publication, Spark, live namespace inspection or Delta fallback.
"""
import hashlib
import importlib.util
from pathlib import Path
import re
import sys

EXPECTED_MODULES = {
    'rcaip_cell24_presentation': 'c2d54f4b019a1630965a6c0b947d7fab54bf86cf76f236b4e352ddc59f8736d6',
    'rcaip_cell24_retained': '326fb26c2c8abbbf52af76fe104e0fec119ac083efa3abb954c8f2ca1276b55e',
    'rcaip_mde_retained_check': 'feb72cd4cb05743276cd6d01cf874ea6eab108b4d70cf8f9964bc0013cf6eb8c',
}


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


def _load_helpers(module_folder):
    folder = Path(module_folder)
    for name, expected in EXPECTED_MODULES.items():
        text = (folder / (name + '.py')).read_bytes().decode('utf-8-sig').replace('\r\n', '\n').replace('\r', '\n')
        digest = hashlib.sha256((text.rstrip('\n') + '\n').encode('utf-8')).hexdigest()
        if digest != expected: raise RuntimeError('Deployed helper source hash mismatch: ' + name)
    modules = {}
    for name in EXPECTED_MODULES:
        spec = importlib.util.spec_from_file_location(name, folder / (name + '.py'))
        if spec is None or spec.loader is None: raise RuntimeError('Deployed helper loader unavailable')
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        modules[name] = module
    return modules['rcaip_mde_retained_check']


def check_mde_retained_input(*, module_folder, source_uri, expected_input_sha256, expected_post, expected_run):
    """Returns only checker structural/status metadata, never saved rows/counts.

    Unknown failures stay fatal. No raw SDK/parser exception is printed by this
    boundary: the standalone caller raises a fixed error, without evidence text.
    """
    if not isinstance(expected_post, str) or not re.fullmatch('[a-z0-9][a-z0-9_-]*', expected_post) or not isinstance(expected_run, str) or not expected_run.strip():
        raise ValueError('Explicit canonical Post and run required')
    try:
        checker = _load_helpers(module_folder)
        from databricks.sdk import WorkspaceClient
        raw = _download_retained(WorkspaceClient(), source_uri, expected_input_sha256)
        return checker.inspect_retained_bytes(raw, expected_input_sha256=expected_input_sha256,
                                              expected_post=expected_post, expected_run=expected_run)
    except (MemoryError, RecursionError):
        raise RuntimeError('Fatal resource failure; no inspection result') from None
    except Exception as exc:
        # Error sanitization, NOT optional-input handling or a successful result.
        # Original data-bearing errors must remain inside enterprise debugging.
        if 'checker' in locals() and isinstance(exc, checker.InputContractError):
            raise RuntimeError('Retained contract rejected: ' + str(exc)) from None
        raise RuntimeError('Retained inspection failed; no validated result. Review locally within the approved environment.') from None
