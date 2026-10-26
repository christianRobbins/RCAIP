# RCAIP — evidence-preserving assessment engineering

RCAIP reconciles asset identity, source observations and control evidence into reviewable analytical products. Its emphasis is explaining what the evidence supports, what remains unresolved, and who should review a condition before it becomes work. Source absence is not automatically a control failure.

This is a publication snapshot of an existing engineering project. The original development history is retained separately. It is not a reconstructed development timeline, a new production release, or a drop-in Databricks deployment. Deployment-specific bindings, operational records, real results and internal communications are excluded.

## Start here: three actual code files

1. [Asset Engine](notebooks/RCAIP_Asset_Engine_v3.py): source normalization and durable identity reconciliation. Follow the normalized identity helpers and source combination logic to see how the downstream asset denominator is formed.
2. [Intune evidence profile](notebooks/rcaip_intune_evidence_profile.py): `prepare`, `queries`, `complete` and `run` separate authorization, distributed evidence profiling and bounded status. Repeated, unknown and conflicting observations remain evidence.
3. [Durable delivery adapter](notebooks/rcaip_intune_devresults.py): `prepare`, `DeltaStore`, `WorkspacePublisher` and `execute` connect approved inputs to pinned Delta evidence, saved profile results and authenticated publication. Recovery reuses completed work instead of repeating acquisition.

Read [architecture and integrity boundaries](docs/architecture.md) next, then the focused tests alongside each module.

## What is retained

- Config/runtime selection, logical table registry, Knowledge Base and Asset Engine notebooks.
- IP-001 through IP-006 analytical notebooks, discovery notebooks, DIAG analysis, and both report implementations.
- Retained-result parsers, explicit reconstruction paths, population consumers, Q1143 review and restart-safe Intune delivery.
- Synthetic fixtures, unit-test sources and separately opt-in Spark verification sources. Test assertions and integrity guards are retained.

The code includes earlier and later implementation styles. Some notebooks execute substantial work at top level. Do not import or run them to browse the project.

## Setup and execution boundaries

Python code is available for inspection without a Databricks account. Local helper tests use Python's `unittest`; workbook helpers depend on `openpyxl`. The notebooks additionally reference PySpark, pandas, Py4J and the Databricks SDK. No dependencies or vendor implementation code are bundled. No universal environment lock or CI workflow is supplied.

The publication preparation ran syntax checks and only `python -B tests/test_cell24_inspection.py` (synthetic metadata substitutes). It did not execute Spark, Delta, acquisition, publication or analytical acceptance suites. See [the test limitations](docs/architecture.md#snapshot-and-test-limitations) before running other tests.

Deployment requires independently authorized configuration, access, runtime compatibility, source identities, assessment scope and version choices. `example_sources` bindings, `example_post_*` values, `operator@example.invalid`, example input folders and the example Intune version are fictional configuration placeholders. They are not working credentials or approved destinations. Original byte/AST fingerprint guards were not repinned to make this snapshot run; changed source must undergo a real release review before deployment.

## Known limitations

- Source review and synthetic results do not certify live data, platform compatibility, control compliance or desktop Excel behavior.
- Some tests require excluded operational handoffs, original Git history or generated review artifacts. Their assertions remain; those dependencies were not replaced with invented evidence.
- High-volume rendering and publication may perform expensive ordering, collection or repeated page operations. Performance of this sanitized snapshot has not been demonstrated.
- Historical reproduction and declared reconstruction are distinct from current live assessment authority. Their provenance checks must not be bypassed.
- This snapshot does not grant an open-source license. No license or redistribution rights are newly asserted on the owner's behalf. External runtime packages retain their own licenses; none are redistributed here.
