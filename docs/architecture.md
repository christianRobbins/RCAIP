# RCAIP architecture

## Evidence flow

`RCAIP_RUN_000_Post_Intake_and_Readiness_CLEAN_v4.py` prepares intake authority. `RCAIP_Config_000_Post_Run_Config.py` consumes that authority and resolves source aliases. `RCAIP_Config_001_Table_Registry.py` maps logical products to physical bindings. These are executable notebooks, not harmless configuration imports.

`RCAIP_Asset_Engine_v3.py` normalizes governed observations and reconciles durable identities. The resulting Asset Plane is the downstream asset denominator. CMDB is expected-state evidence, not an independent claim to contain every asset. Single-source, stale, unresolved and attribution-conflict evidence must remain visible.

The IP notebooks derive domain-specific products: network-port intelligence, boundary/coverage, lifecycle, identity hygiene, operational views and control-assurance features. The Knowledge Base supplies rule/context tables. Discovery notebooks describe evidence capabilities. `RCAIP_DIAG_001_Asset_Visibility_Funnel.py` performs staged population accounting and qualification; source presence alone does not prove those stages executed.

## Consumers and provenance

`RCAIP_REPORT_001_DT_Validation_Package_V2.py` and `RCAIP_REPORT_002_Asset_Enterprise_Visibility_Workbook.py` are different consumers with their own contracts. The latter's operational reference-only display totals are excluded here; its reconciliation predicates are preserved.

The Cell 24 presentation preserves cases, members and branch closure at separate grains. Its retained loader binds structured notebook outputs to the same source cell and saved certification. An artifact hash identifies bytes; it is not a signed certificate or current live-state guarantee. The IP population companion adds complete-population communication views without creating new case decisions.

The MDE retained checker validates existing associations. The durable bridge separates verified historical binding from transformation reproduction. Declared reconstruction discloses missing original input-version association rather than claiming historical certainty. Population rendering preserves original explanations and routing; missing recipient evidence must not become a fabricated assignment.

## Restart-safe Intune path

`rcaip_intune_validation.py` separates planning from approved acquisition, retains pre-deduplication observations and has a distributed result mode. `rcaip_intune_evidence_profile.py` profiles retained observations without changing source matching rules. Temporal ambiguity, nulls and conflicting representations remain explicit.

`rcaip_intune_delivery.py` orchestrates recovery and readable output. Its active persistent adapter is `rcaip_intune_devresults.py`. `DeltaStore` uses registered product roles, immutable write identities and captured Delta versions; `WorkspacePublisher` binds publication to the approved authenticated destination. Saved evidence and saved profile state allow output recovery without raw reacquisition or repeated profiling. Completion depends on verified checkpoints and bounded complete publication receipts. Earlier filesystem code remains as an explicitly separated reference path, not the recommended persistent route.

`rcaip_ip001_audit.py` and the shared writer extraction underpin exact output identity/version checks. Matching totals alone do not establish matching membership. Commit metadata, table identity, version, schema, scope and generation each have distinct jobs.

## Snapshot and test limitations

This repository intentionally has no operational `state/`, `history/`, `reference/`, generated `outputs/`, exported notebooks, internal operator handoffs or original Git history. The large archived MDE lookback scratch notebook is excluded; the actual reusable bridge/checker/reconstruction implementations are retained. No evidence was manufactured to fill the gaps.

Selected source aliases and environment inventories were replaced with fictional bindings. Private deployment paths and named assessment examples were generalized; valid IANA timezone identifiers and analytic field names are retained. The pinned Intune version is a fictional example. Original source/AST hashes and assertions are unchanged. Therefore integrity-pinned entry points are configuration-dependent references, not an independently certified release.

In particular, the Intune validation/profile/distributed/delivery chain, Cell 24 retained publication, IP/MDE retained/reconstruction entry points and source-extracting tests need an explicit source-identity review wherever their dependencies changed. Do not update fingerprints merely to obtain a green result.

`test_intune_source_alias.py` depends on an original Git commit that is deliberately absent. `test_intune_operator_loader.py` depends on an excluded internal operator guide. Other review/workbook tests refer to generated output files or omitted operator instructions. These are retained test designs, not a promise that `unittest discover` passes in a fresh snapshot. Synthetic Python fixtures use constructed identities and documentation IP addresses; the retained JSON fixture contains explicitly synthetic identities and reserved documentation addresses.

The only test suite executed during publication preparation was the metadata-only Cell 24 inspection suite, affected by fictionalized scope examples. No application notebook was imported. All Python sources were parsed and compiled in memory, and the JSON fixture was parsed. Spark/Delta launchers were not run. Full analytical, live-platform, performance and desktop Excel validation remain outside this preparation.

## Review principles

Keep evidence native and preserve population membership. Keep customer routing distinct from control conclusions. Unknown, stale or missing evidence is not automatically a failure. Reproduction is not live certification. Never use this snapshot's fictional deployment values or historical hashes as authorization to read or publish real data.
