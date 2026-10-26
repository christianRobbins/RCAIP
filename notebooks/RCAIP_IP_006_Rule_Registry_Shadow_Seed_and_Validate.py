# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP IP-006 - Rule Registry Shadow Seed and Validation
# Version: 0.1.0
#
# Purpose:
#   Seed the executable RCAIP v2.2 Rule Registry as an enterprise-shared
#   design/shadow product and certify its structural safety.
#
# Safety:
#   - No rule evaluation occurs in this notebook.
#   - Every rule is written with active_indicator = False.
#   - The notebook refuses to overwrite an existing active registry.
#   - No Asset Plane, IP-001 through IP-005, operational, or report table is
#     modified.
# ============================================================================

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import types as T


REQUIRED_RUNTIME_OBJECTS = [
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_TABLES",
    "get_rcaip_table",
    "validate_rcaip_table_contract"
]

missing_runtime_objects = [
    object_name
    for object_name in REQUIRED_RUNTIME_OBJECTS
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-006 shadow seed did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

IP006_REGISTRY_CONTRACT_VERSION = "0.1.0"
IP006_FEATURE_CONTRACT_VERSION = "2.2.0"
IP006_RULE_REGISTRY_LOGICAL_NAME = "KB_RULE_REGISTRY_V2_2"
IP006_DESIGN_STATUS = "DESIGN_BASELINE_NOT_ACTIVE"

IP006_REQUIRED_LOGICAL_BINDINGS = [
    "KB_RULE_REGISTRY_V2_2",
    "IP006_ASSET_FEATURE_MART",
    "IP006_RULE_MATCHES",
    "IP006_RULE_IMPACT_SUMMARY",
    "IP006_COHORT_SUMMARY",
    "IP006_TRIAGE_ROUTING",
    "IP006_UNCLASSIFIED_RESIDUAL"
]

missing_logical_bindings = [
    logical_name
    for logical_name in IP006_REQUIRED_LOGICAL_BINDINGS
    if logical_name not in RCAIP_TABLES
]

if missing_logical_bindings:
    raise RuntimeError(
        "Config 001 is missing required IP-006 logical bindings: "
        + " | ".join(missing_logical_bindings)
        + ". Apply the IP-006 Config 001 additive patch first."
    )

rule_registry_table = get_rcaip_table(
    IP006_RULE_REGISTRY_LOGICAL_NAME
)

expected_shared_table = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    "rcaip_kb_rule_registry_v2_2"
)

if str(rule_registry_table).lower() != expected_shared_table.lower():
    raise RuntimeError(
        "KB_RULE_REGISTRY_V2_2 resolved to an unexpected physical table. "
        f"Expected={expected_shared_table} | Actual={rule_registry_table}"
    )

expected_post_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()

invalid_post_bindings = {
    logical_name: get_rcaip_table(logical_name)
    for logical_name in IP006_REQUIRED_LOGICAL_BINDINGS
    if logical_name != IP006_RULE_REGISTRY_LOGICAL_NAME
    and not str(get_rcaip_table(logical_name)).lower().startswith(
        expected_post_prefix
    )
}

if invalid_post_bindings:
    raise RuntimeError(
        "IP-006 registry contains a cross-Post output binding: "
        + " | ".join(
            f"{logical_name}={physical_name}"
            for logical_name, physical_name in invalid_post_bindings.items()
        )
    )

print("IP-006 Config 001 logical binding validation: PASS")
print("Rule registry table:", rule_registry_table)
print("Current certified Post:", RCAIP_POST_NAME)
print("Current run ID:", RCAIP_RUN_ID)


# ============================================================================
# Define the complete executable registry contract and five design rules.
# ============================================================================

RULE_REGISTRY_ROWS = [{'registry_contract_version': '0.1.0',
  'rule_id': 'STRUCT-NET-001',
  'rule_name': 'Subnet Network or Broadcast Structural Exclusion',
  'rule_family': 'STRUCTURAL_NETWORK_ARTIFACT',
  'rule_version': '0.1.0',
  'design_date': '2026-08-02',
  'effective_date': None,
  'execution_layer': 'SOURCE_COLLECTION',
  'execution_order': 10,
  'precedence_group': 10,
  'evaluation_scope': 'NMAP_OBSERVATION',
  'match_mode': 'STRUCTURAL_EXCLUSION',
  'parallel_match_allowed': False,
  'rule_state': 'BLOCKED',
  'dependency_status': 'BLOCKED_NMAP_REFIT',
  'blocked_features': ['network_id',
                       'cidr_prefix_length',
                       'is_network_address',
                       'is_broadcast_address',
                       'mac_address_present'],
  'required_features': ['source_system',
                        'canonical_ip',
                        'ip_version',
                        'network_id',
                        'cidr_prefix_length',
                        'is_network_address',
                        'is_broadcast_address',
                        'mac_address_present',
                        'hostname_present',
                        'open_port_count',
                        'service_port_signature',
                        'scan_id',
                        'scan_source_file'],
  'trigger_expression': "source_system = 'NMAP' AND ip_version = 4 AND cidr_prefix_length < 31 AND "
                        '(is_network_address OR is_broadcast_address) AND NOT mac_address_present '
                        'AND NOT hostname_present AND COALESCE(open_port_count,0) = 0 AND '
                        "service_port_signature = 'NONE'",
  'required_evidence': 'Certified Nmap scan provenance, containing CIDR/prefix, calculated network '
                       'or broadcast address, and absence of host-specific evidence.',
  'exclusion_expression': 'cidr_prefix_length >= 31 OR ip_version <> 4 OR CIDR unavailable OR '
                          'MAC/hostname/service/port evidence exists.',
  'output_lane_code': 'STRUCTURAL_EXCLUSION_AUDIT',
  'output_lane_name': 'Structural Exclusion Audit',
  'default_priority': 'Structural',
  'priority_expression': 'Structural when all trigger conditions are met; otherwise do not match '
                         'and route any contradictory host evidence to Network Boundary Review.',
  'default_confidence': 'HIGH',
  'confidence_expression': 'HIGH only when CIDR provenance is certified and no host-specific '
                           'evidence exists.',
  'management_owner': 'NONE',
  'validation_owner': 'RCAIP Data Engineering; Post Network only for sampled exceptions',
  'recommended_action': 'Preserve the raw observation and provenance, record the structural '
                        'reason, and exclude it from the physical-asset denominator before Asset '
                        'Plane identity creation.',
  'guardrail': 'Never delete raw scan evidence. Never exclude /31 or /32 IPv4 observations, IPv6 '
               'observations under broadcast logic, or any observation with host-specific '
               'evidence. Structural exclusion is not proof that a device never existed.',
  'terminal_indicator': True,
  'structural_exclusion_indicator': True,
  'suppression_allowed': False,
  'suppression_condition': 'NONE',
  'sample_review_required': True,
  'sample_review_size': 5,
  'cohort_min_homogeneity': 1.0,
  'source_basis': ['NMAP_SCAN_PROVENANCE', 'NMAP_NORM', 'KB_ISSO_SUBNET_LOOKUP'],
  'active_indicator': False,
  'notes': 'Must be implemented in the Nmap/IP-001 refit before the one-row-per-asset Asset Plane. '
           'Remains blocked until CIDR and scan multiplicity are preserved.'},
 {'registry_contract_version': '0.1.0',
  'rule_id': 'ATTR-CMDB-001',
  'rule_name': 'CMDB Foreign-Post Token Attribution Review',
  'rule_family': 'ATTRIBUTION_CONFLICT',
  'rule_version': '0.1.0',
  'design_date': '2026-08-02',
  'effective_date': None,
  'execution_layer': 'ASSET_PLANE_ANALYTICS',
  'execution_order': 20,
  'precedence_group': 20,
  'evaluation_scope': 'ASSET',
  'match_mode': 'ROUTE',
  'parallel_match_allowed': True,
  'rule_state': 'DESIGN_READY',
  'dependency_status': 'READY',
  'blocked_features': [],
  'required_features': ['cmdb_present',
                        'current_post_token_match',
                        'foreign_post_token_match',
                        'foreign_post_candidate',
                        'attribution_confidence',
                        'current_observation_status',
                        'source_mask_active',
                        'post_slug'],
  'trigger_expression': "cmdb_present = 'Y' AND foreign_post_token_match = true AND "
                        'foreign_post_candidate IS NOT NULL AND foreign_post_candidate <> '
                        'post_slug AND (current_post_token_match = false OR attribution_confidence '
                        "= 'CONFLICTED')",
  'required_evidence': 'A matched active KB naming-token rule for another Post, the matched '
                       'evidence field, the candidate Post slug, and the current certified '
                       'Post/run identity.',
  'exclusion_expression': "attribution_confidence IN ('LOW','UNKNOWN') OR foreign_post_candidate "
                          'IS NULL OR the matched token is guarded/ambiguous with no corroborating '
                          'attribution evidence.',
  'output_lane_code': 'ENTERPRISE_DATA_QUALITY',
  'output_lane_name': 'Enterprise Data Quality',
  'default_priority': 'Medium',
  'priority_expression': 'High when current evidence exists and attribution_confidence = '
                         "'CONFLICTED'; Medium otherwise.",
  'default_confidence': 'MODERATE',
  'confidence_expression': 'HIGH when two or more independent attribution signals support the same '
                           'foreign Post; MODERATE for one governed token plus one supporting '
                           'metadata signal.',
  'management_owner': 'ENTERPRISE',
  'validation_owner': 'Enterprise CMDB/Asset Management with Post corroboration',
  'recommended_action': 'Validate the CMDB assignment and authoritative owner before any Post '
                        'remediation, tooling action, or inventory correction.',
  'guardrail': 'A naming token never automatically reassigns an asset, creates a finding, or '
               'proves that current Post tooling is missing. Guarded tokens may support candidate '
               'attribution only.',
  'terminal_indicator': False,
  'structural_exclusion_indicator': False,
  'suppression_allowed': False,
  'suppression_condition': 'NONE',
  'sample_review_required': False,
  'sample_review_size': 0,
  'cohort_min_homogeneity': 0.0,
  'source_basis': ['ASSET_RECONCILIATION_SCORED',
                   'IP006_ASSET_FEATURE_MART',
                   'KB_NAMING_TOKEN_RULES',
                   'KB_ISSO_SUBNET_LOOKUP'],
  'active_indicator': False,
  'notes': 'Runs before context cohorting so an attribution conflict cannot be hidden by an '
           'otherwise homogeneous tool pattern.'},
 {'registry_contract_version': '0.1.0',
  'rule_id': 'MGMT-INTUNE-001',
  'rule_name': 'CMDB Intune-Management Versus Intune-Source Discrepancy',
  'rule_family': 'MANAGEMENT_SOURCE_CONFLICT',
  'rule_version': '0.1.0',
  'design_date': '2026-08-02',
  'effective_date': None,
  'execution_layer': 'ASSET_PLANE_ANALYTICS',
  'execution_order': 30,
  'precedence_group': 30,
  'evaluation_scope': 'ASSET',
  'match_mode': 'ROUTE',
  'parallel_match_allowed': True,
  'rule_state': 'DESIGN_READY',
  'dependency_status': 'READY_WITH_FEATURE_AMENDMENT',
  'blocked_features': [],
  'required_features': ['cmdb_present',
                        'cmdb_intune_management_indicator',
                        'intune_source_status',
                        'intune_record_present',
                        'intune_active',
                        'current_observation_status',
                        'device_family',
                        'applicable_tool_mask',
                        'management_model'],
  'trigger_expression': "cmdb_present = 'Y' AND cmdb_intune_management_indicator = true AND "
                        "intune_source_status = 'AVAILABLE' AND ((intune_record_present = 0 AND "
                        "current_observation_status IN ('Active Multi-Source','Active "
                        "Single-Source')) OR (intune_record_present = 1 AND intune_active = 0 AND "
                        "current_observation_status IN ('Active Multi-Source','Active "
                        "Single-Source')))",
  'required_evidence': 'Explicit normalized CMDB Intune-management indicator, current asset '
                       'evidence, confirmed Intune source availability, and current/stale Intune '
                       'relationship.',
  'exclusion_expression': "intune_source_status <> 'AVAILABLE' OR device family/profile is not "
                          'Intune-applicable OR no current asset evidence exists OR a governed KB '
                          'exception applies.',
  'output_lane_code': 'ENTERPRISE_DATA_QUALITY',
  'output_lane_name': 'Enterprise Data Quality',
  'default_priority': 'Medium',
  'priority_expression': 'High when the asset is current, Intune is applicable, and no Intune '
                         'record exists; Medium when an Intune record exists but is stale.',
  'default_confidence': 'HIGH',
  'confidence_expression': 'HIGH for an explicit governed SG-Intune/Intune management indicator; '
                           'MODERATE for normalized textual inference.',
  'management_owner': 'ENTERPRISE',
  'validation_owner': 'Enterprise CMDB and Intune Source Owners with Post corroboration',
  'recommended_action': 'Validate identity matching, device applicability, CMDB management '
                        'metadata, Intune enrollment/reporting, and source ingestion before '
                        'classifying a tooling failure.',
  'guardrail': 'The discrepancy is not proof that Intune is absent from the physical device, that '
               'the device is noncompliant, or that Post failed to manage it. Source '
               'unavailability must not create a match.',
  'terminal_indicator': False,
  'structural_exclusion_indicator': False,
  'suppression_allowed': False,
  'suppression_condition': 'NONE',
  'sample_review_required': False,
  'sample_review_size': 0,
  'cohort_min_homogeneity': 0.0,
  'source_basis': ['ASSET_RECONCILIATION_SCORED',
                   'IP006_ASSET_FEATURE_MART',
                   'KB_REQUIRED_TOOLS_V2',
                   'KB_DEVICE_TOOL_EXPECTATIONS',
                   'KB_SOURCE_REGISTRY'],
  'active_indicator': False,
  'notes': 'Requires Feature Contract amendments F-049 and F-050 defined in this document.'},
 {'registry_contract_version': '0.1.0',
  'rule_id': 'COHORT-MOBILE-001',
  'rule_name': 'Intune-Only Enterprise-Mobile Context Cohort',
  'rule_family': 'ENTERPRISE_MANAGED_COHORT',
  'rule_version': '0.1.0',
  'design_date': '2026-08-02',
  'effective_date': None,
  'execution_layer': 'ASSET_PLANE_ANALYTICS',
  'execution_order': 70,
  'precedence_group': 70,
  'evaluation_scope': 'ASSET_THEN_COHORT',
  'match_mode': 'CONTEXT',
  'parallel_match_allowed': True,
  'rule_state': 'DESIGN_READY',
  'dependency_status': 'READY',
  'blocked_features': [],
  'required_features': ['device_family',
                        'management_model',
                        'source_mask_active',
                        'cmdb_present',
                        'intune_active',
                        'identity_strength',
                        'attribution_confidence',
                        'conflict_count',
                        'high_risk_feature_count',
                        'true_tooling_conflict_count',
                        'tool_applicability_profile'],
  'trigger_expression': "device_family = 'MOBILE' AND management_model = 'ENTERPRISE_MANAGED' AND "
                        "source_mask_active = 'INTUNE' AND cmdb_present = 'N' AND intune_active = "
                        "1 AND identity_strength IN ('STRONG','MODERATE') AND "
                        "attribution_confidence NOT IN ('CONFLICTED','LOW','UNKNOWN') AND "
                        'conflict_count = 0 AND high_risk_feature_count = 0 AND '
                        'true_tooling_conflict_count = 0',
  'required_evidence': 'Current Intune evidence, recognized enterprise-mobile device profile, '
                       'coherent identity evidence, and no attribution, risk, or applicable-tool '
                       'conflict.',
  'exclusion_expression': "management_model <> 'ENTERPRISE_MANAGED' OR Post-issued "
                          'MID/Post-managed profile OR stale/unknown Intune evidence OR '
                          'attribution conflict OR any high-risk/applicable-tool conflict.',
  'output_lane_code': 'ENTERPRISE_MANAGED_CONTEXT',
  'output_lane_name': 'Enterprise-Managed Context',
  'default_priority': 'Context',
  'priority_expression': 'Context only. Any independent attribution, boundary, risk, or tooling '
                         'rule retains its own priority and prevents suppression.',
  'default_confidence': 'HIGH',
  'confidence_expression': 'HIGH when current Intune, serial/profile, and identity evidence agree; '
                           'MODERATE if one nonessential profile field is unavailable.',
  'management_owner': 'ENTERPRISE',
  'validation_owner': 'Enterprise Mobility/Intune Owner; RCSO sample review',
  'recommended_action': 'Create an interpretable cohort, review five representative records '
                        'locally, and suppress the remainder from the default RCSO workbench only '
                        'if the sample and homogeneity checks pass.',
  'guardrail': 'Do not suppress any record with a parallel conflict, risk, boundary, or '
               'unclassified-feature match. Intune-only does not mean unmanaged, missing, or '
               'noncompliant.',
  'terminal_indicator': False,
  'structural_exclusion_indicator': False,
  'suppression_allowed': True,
  'suppression_condition': 'sample_status = PASS AND cohort_homogeneity >= 0.95 AND conflict_count '
                           '= 0 AND high_risk_feature_count = 0 AND true_tooling_conflict_count = '
                           '0 AND unexplained_feature_count = 0',
  'sample_review_required': True,
  'sample_review_size': 5,
  'cohort_min_homogeneity': 0.95,
  'source_basis': ['ASSET_RECONCILIATION_SCORED',
                   'IP006_ASSET_FEATURE_MART',
                   'KB_DEVICE_PROFILES',
                   'KB_DEVICE_TOOL_EXPECTATIONS',
                   'KB_REQUIRED_TOOLS_V2',
                   'KB_SOURCE_REGISTRY'],
  'active_indicator': False,
  'notes': 'Designed to remove predictable enterprise-mobile context from the default Post/RCSO '
           'burden without deleting evidence.'},
 {'registry_contract_version': '0.1.0',
  'rule_id': 'COHORT-PRINT-001',
  'rule_name': 'Printer/MFD Signature and Post-Management Cohort',
  'rule_family': 'POST_MANAGED_COHORT',
  'rule_version': '0.1.0',
  'design_date': '2026-08-02',
  'effective_date': None,
  'execution_layer': 'ASSET_PLANE_ANALYTICS',
  'execution_order': 80,
  'precedence_group': 70,
  'evaluation_scope': 'ASSET_THEN_COHORT',
  'match_mode': 'CONTEXT',
  'parallel_match_allowed': True,
  'rule_state': 'DESIGN_READY',
  'dependency_status': 'READY',
  'blocked_features': [],
  'required_features': ['device_family',
                        'printer_signature',
                        'infrastructure_signature',
                        'management_model',
                        'management_owner',
                        'current_observation_status',
                        'attribution_confidence',
                        'structural_exclusion_candidate',
                        'network_zone',
                        'conflict_count',
                        'high_risk_feature_count',
                        'applicable_coverage_gap',
                        'service_port_signature'],
  'trigger_expression': "(device_family = 'PRINTER_MFD' OR printer_signature = true) AND "
                        "infrastructure_signature = false AND management_model = 'POST_MANAGED' "
                        "AND management_owner = 'POST' AND current_observation_status IN ('Active "
                        "Multi-Source','Active Single-Source') AND attribution_confidence IN "
                        "('HIGH','MODERATE') AND structural_exclusion_candidate = false",
  'required_evidence': 'Printer/MFD classification or governed printer signature, current '
                       'evidence, and coherent Post-management attribution.',
  'exclusion_expression': 'infrastructure_signature = true OR attribution_confidence IN '
                          "('CONFLICTED','LOW','UNKNOWN') OR structural_exclusion_candidate = "
                          'true. Boundary and high-interest port conditions do not cancel the '
                          'context match but independently prevent suppression.',
  'output_lane_code': 'POST_MANAGED_DEVICES',
  'output_lane_name': 'Post-Managed Devices',
  'default_priority': 'Context',
  'priority_expression': 'Context only. Independent open-port, exposure, attribution, or risk '
                         'rules retain their own priority.',
  'default_confidence': 'HIGH',
  'confidence_expression': 'HIGH when device family and service/name evidence agree; MODERATE when '
                           'only one governed signature family is available.',
  'management_owner': 'POST',
  'validation_owner': 'Post DT with Enterprise Network/Vulnerability support when independent port '
                      'or boundary matches exist',
  'recommended_action': 'Group printers/MFDs as a Post-managed cohort, review five representative '
                        'records, validate local ownership and expected service profile, and '
                        'retain independent port/boundary actions.',
  'guardrail': 'Do not interpret missing endpoint-management tools as a gap when they are not '
               'applicable. A printer context match cannot suppress attribution, boundary, '
               'exposure, high-risk port, or lifecycle actions.',
  'terminal_indicator': False,
  'structural_exclusion_indicator': False,
  'suppression_allowed': True,
  'suppression_condition': 'sample_status = PASS AND cohort_homogeneity >= 0.90 AND conflict_count '
                           '= 0 AND high_risk_feature_count = 0 AND applicable_coverage_gap = '
                           "'NONE' AND network_zone NOT IN ('NEN','PUBLIC_EXTERNAL') AND "
                           'unexplained_feature_count = 0',
  'sample_review_required': True,
  'sample_review_size': 5,
  'cohort_min_homogeneity': 0.9,
  'source_basis': ['ASSET_RECONCILIATION_SCORED',
                   'IP006_ASSET_FEATURE_MART',
                   'KB_ASSET_PATTERNS_V2',
                   'KB_PORT_RULES',
                   'KB_REQUIRED_TOOLS_V2'],
  'active_indicator': False,
  'notes': 'Context classification is independent from port/service security review.'}]

RULE_REGISTRY_FIELD_ORDER = [
    "registry_contract_version",
    "rule_id",
    "rule_name",
    "rule_family",
    "rule_version",
    "design_date",
    "effective_date",
    "execution_layer",
    "execution_order",
    "precedence_group",
    "evaluation_scope",
    "match_mode",
    "parallel_match_allowed",
    "rule_state",
    "dependency_status",
    "blocked_features",
    "required_features",
    "trigger_expression",
    "required_evidence",
    "exclusion_expression",
    "output_lane_code",
    "output_lane_name",
    "default_priority",
    "priority_expression",
    "default_confidence",
    "confidence_expression",
    "management_owner",
    "validation_owner",
    "recommended_action",
    "guardrail",
    "terminal_indicator",
    "structural_exclusion_indicator",
    "suppression_allowed",
    "suppression_condition",
    "sample_review_required",
    "sample_review_size",
    "cohort_min_homogeneity",
    "source_basis",
    "active_indicator",
    "notes"
]

RULE_REGISTRY_SCHEMA = T.StructType([
    T.StructField("registry_contract_version", T.StringType(), False),
    T.StructField("rule_id", T.StringType(), False),
    T.StructField("rule_name", T.StringType(), False),
    T.StructField("rule_family", T.StringType(), False),
    T.StructField("rule_version", T.StringType(), False),
    T.StructField("design_date", T.DateType(), False),
    T.StructField("effective_date", T.DateType(), True),
    T.StructField("execution_layer", T.StringType(), False),
    T.StructField("execution_order", T.IntegerType(), False),
    T.StructField("precedence_group", T.IntegerType(), False),
    T.StructField("evaluation_scope", T.StringType(), False),
    T.StructField("match_mode", T.StringType(), False),
    T.StructField("parallel_match_allowed", T.BooleanType(), False),
    T.StructField("rule_state", T.StringType(), False),
    T.StructField("dependency_status", T.StringType(), False),
    T.StructField("blocked_features", T.ArrayType(T.StringType(), containsNull=False), False),
    T.StructField("required_features", T.ArrayType(T.StringType(), containsNull=False), False),
    T.StructField("trigger_expression", T.StringType(), False),
    T.StructField("required_evidence", T.StringType(), False),
    T.StructField("exclusion_expression", T.StringType(), False),
    T.StructField("output_lane_code", T.StringType(), False),
    T.StructField("output_lane_name", T.StringType(), False),
    T.StructField("default_priority", T.StringType(), False),
    T.StructField("priority_expression", T.StringType(), False),
    T.StructField("default_confidence", T.StringType(), False),
    T.StructField("confidence_expression", T.StringType(), False),
    T.StructField("management_owner", T.StringType(), False),
    T.StructField("validation_owner", T.StringType(), False),
    T.StructField("recommended_action", T.StringType(), False),
    T.StructField("guardrail", T.StringType(), False),
    T.StructField("terminal_indicator", T.BooleanType(), False),
    T.StructField("structural_exclusion_indicator", T.BooleanType(), False),
    T.StructField("suppression_allowed", T.BooleanType(), False),
    T.StructField("suppression_condition", T.StringType(), False),
    T.StructField("sample_review_required", T.BooleanType(), False),
    T.StructField("sample_review_size", T.IntegerType(), False),
    T.StructField("cohort_min_homogeneity", T.DoubleType(), False),
    T.StructField("source_basis", T.ArrayType(T.StringType(), containsNull=False), False),
    T.StructField("active_indicator", T.BooleanType(), False),
    T.StructField("notes", T.StringType(), False)
])

ALLOWED_RULE_STATES = [
    "DESIGN_READY",
    "BLOCKED",
    "SHADOW",
    "ACTIVE",
    "RETIRED"
]

ALLOWED_MATCH_MODES = [
    "STRUCTURAL_EXCLUSION",
    "ROUTE",
    "CONTEXT"
]

ALLOWED_ROUTING_LANES = [
    "EMERGING_ANOMALIES",
    "ENTERPRISE_DATA_QUALITY",
    "ENTERPRISE_MANAGED_CONTEXT",
    "NETWORK_BOUNDARY_REVIEW",
    "POST_MANAGED_DEVICES",
    "POST_VALIDATION",
    "RCSO_FOCUS_QUEUE",
    "STRUCTURAL_EXCLUSION_AUDIT",
    "SUPPRESSED_LOW_SIGNAL_CONTEXT",
    "UNCLASSIFIED_RESIDUAL"
]

normalized_rule_rows = []

for raw_rule_row in RULE_REGISTRY_ROWS:
    normalized_rule_row = dict(raw_rule_row)

    normalized_rule_row["design_date"] = date.fromisoformat(
        normalized_rule_row["design_date"]
    )

    if normalized_rule_row["effective_date"]:
        normalized_rule_row["effective_date"] = date.fromisoformat(
            normalized_rule_row["effective_date"]
        )
    else:
        normalized_rule_row["effective_date"] = None

    normalized_rule_rows.append(normalized_rule_row)

rule_registry_df = spark.createDataFrame(
    normalized_rule_rows,
    schema=RULE_REGISTRY_SCHEMA
)


# ============================================================================
# Pre-write safety certification.
# ============================================================================

expected_rule_count = 5
actual_rule_count = rule_registry_df.count()

if actual_rule_count != expected_rule_count:
    raise RuntimeError(
        "Unexpected v0.1 Rule Registry row count. "
        f"Expected={expected_rule_count} | Actual={actual_rule_count}"
    )

duplicate_rule_ids = (
    rule_registry_df
    .groupBy("rule_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_rule_ids:
    raise RuntimeError(
        f"Duplicate Rule Registry IDs detected: {duplicate_rule_ids}"
    )

unexpected_states = (
    rule_registry_df
    .filter(~F.col("rule_state").isin(ALLOWED_RULE_STATES))
    .count()
)

unexpected_match_modes = (
    rule_registry_df
    .filter(~F.col("match_mode").isin(ALLOWED_MATCH_MODES))
    .count()
)

unexpected_lanes = (
    rule_registry_df
    .filter(~F.col("output_lane_code").isin(ALLOWED_ROUTING_LANES))
    .count()
)

active_rule_count = (
    rule_registry_df
    .filter(F.col("active_indicator"))
    .count()
)

inactive_with_effective_date_count = (
    rule_registry_df
    .filter(
        (~F.col("active_indicator"))
        & F.col("effective_date").isNotNull()
    )
    .count()
)

unsafe_active_rule_count = (
    rule_registry_df
    .filter(
        F.col("active_indicator")
        & (
            (F.col("rule_state") != F.lit("ACTIVE"))
            | (F.col("dependency_status") != F.lit("READY"))
            | (F.size(F.col("blocked_features")) > 0)
        )
    )
    .count()
)

invalid_structural_rule_count = (
    rule_registry_df
    .filter(
        F.col("structural_exclusion_indicator")
        & (
            (F.col("match_mode") != F.lit("STRUCTURAL_EXCLUSION"))
            | (~F.col("terminal_indicator"))
            | F.col("suppression_allowed")
        )
    )
    .count()
)

invalid_suppression_rule_count = (
    rule_registry_df
    .filter(
        F.col("suppression_allowed")
        & (
            (F.col("match_mode") != F.lit("CONTEXT"))
            | (~F.col("sample_review_required"))
            | (F.col("sample_review_size") <= 0)
            | (F.col("cohort_min_homogeneity") <= F.lit(0.0))
        )
    )
    .count()
)

missing_guardrail_count = (
    rule_registry_df
    .filter(
        F.length(F.trim(F.col("guardrail"))) == 0
    )
    .count()
)

missing_action_count = (
    rule_registry_df
    .filter(
        F.length(F.trim(F.col("recommended_action"))) == 0
    )
    .count()
)

shadow_safety_checks = [
    (
        "Rule count",
        str(expected_rule_count),
        str(actual_rule_count),
        actual_rule_count == expected_rule_count
    ),
    (
        "Unique rule IDs",
        "0 duplicates",
        str(duplicate_rule_ids),
        duplicate_rule_ids == 0
    ),
    (
        "Allowed rule states",
        "0 invalid",
        str(unexpected_states),
        unexpected_states == 0
    ),
    (
        "Allowed match modes",
        "0 invalid",
        str(unexpected_match_modes),
        unexpected_match_modes == 0
    ),
    (
        "Allowed routing lanes",
        "0 invalid",
        str(unexpected_lanes),
        unexpected_lanes == 0
    ),
    (
        "Active rules in design seed",
        "0",
        str(active_rule_count),
        active_rule_count == 0
    ),
    (
        "Inactive rules with effective dates",
        "0",
        str(inactive_with_effective_date_count),
        inactive_with_effective_date_count == 0
    ),
    (
        "Unsafe active rules",
        "0",
        str(unsafe_active_rule_count),
        unsafe_active_rule_count == 0
    ),
    (
        "Invalid structural rules",
        "0",
        str(invalid_structural_rule_count),
        invalid_structural_rule_count == 0
    ),
    (
        "Invalid suppression rules",
        "0",
        str(invalid_suppression_rule_count),
        invalid_suppression_rule_count == 0
    ),
    (
        "Blank guardrails",
        "0",
        str(missing_guardrail_count),
        missing_guardrail_count == 0
    ),
    (
        "Blank recommended actions",
        "0",
        str(missing_action_count),
        missing_action_count == 0
    )
]

failed_shadow_safety_checks = [
    check_name
    for check_name, expected, actual, passed in shadow_safety_checks
    if not passed
]

shadow_safety_schema = T.StructType([
    T.StructField("validation_check", T.StringType(), False),
    T.StructField("expected_value", T.StringType(), False),
    T.StructField("actual_value", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False)
])

shadow_safety_df = spark.createDataFrame(
    shadow_safety_checks,
    schema=shadow_safety_schema
)

display(shadow_safety_df.orderBy("validation_check"))

if failed_shadow_safety_checks:
    raise RuntimeError(
        "Rule Registry pre-write certification failed: "
        + " | ".join(failed_shadow_safety_checks)
    )

print("Rule Registry pre-write shadow safety certification: PASS")


# ============================================================================
# Idempotent protected write.
#
# Refuse to replace an existing registry containing any active rule. An active
# registry must be changed only through a later controlled promotion process.
# ============================================================================

existing_registry_active_count = 0

if spark.catalog.tableExists(rule_registry_table):
    existing_registry_df = spark.table(rule_registry_table)

    if "active_indicator" not in existing_registry_df.columns:
        raise RuntimeError(
            "Existing Rule Registry table lacks active_indicator. Refusing "
            "to overwrite an unknown contract."
        )

    existing_registry_active_count = (
        existing_registry_df
        .filter(F.col("active_indicator").cast("boolean"))
        .count()
    )

    if existing_registry_active_count > 0:
        raise RuntimeError(
            "Existing Rule Registry contains active rules. Refusing shadow "
            f"overwrite. Active rows: {existing_registry_active_count}"
        )

(
    rule_registry_df
    .select(*RULE_REGISTRY_FIELD_ORDER)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(rule_registry_table)
)

print("Rule Registry shadow seed write: PASS")
print("Written table:", rule_registry_table)


# ============================================================================
# Post-write certification.
# ============================================================================

written_registry_df = spark.table(rule_registry_table)

written_rule_count = written_registry_df.count()
written_unique_rule_count = (
    written_registry_df
    .select("rule_id")
    .distinct()
    .count()
)
written_active_rule_count = (
    written_registry_df
    .filter(F.col("active_indicator"))
    .count()
)
written_blocked_rule_count = (
    written_registry_df
    .filter(F.col("rule_state") == F.lit("BLOCKED"))
    .count()
)
written_design_ready_rule_count = (
    written_registry_df
    .filter(F.col("rule_state") == F.lit("DESIGN_READY"))
    .count()
)
written_structural_rule_count = (
    written_registry_df
    .filter(F.col("structural_exclusion_indicator"))
    .count()
)
written_suppression_capable_count = (
    written_registry_df
    .filter(F.col("suppression_allowed"))
    .count()
)

post_write_checks = [
    (
        "Written row count",
        "5",
        str(written_rule_count),
        written_rule_count == 5
    ),
    (
        "Written unique rule IDs",
        "5",
        str(written_unique_rule_count),
        written_unique_rule_count == 5
    ),
    (
        "Written active rules",
        "0",
        str(written_active_rule_count),
        written_active_rule_count == 0
    ),
    (
        "Blocked rules",
        "1",
        str(written_blocked_rule_count),
        written_blocked_rule_count == 1
    ),
    (
        "Design-ready rules",
        "4",
        str(written_design_ready_rule_count),
        written_design_ready_rule_count == 4
    ),
    (
        "Structural exclusion rules",
        "1",
        str(written_structural_rule_count),
        written_structural_rule_count == 1
    ),
    (
        "Suppression-capable context rules",
        "2",
        str(written_suppression_capable_count),
        written_suppression_capable_count == 2
    )
]

post_write_df = spark.createDataFrame(
    post_write_checks,
    schema=shadow_safety_schema
)

display(post_write_df.orderBy("validation_check"))

post_write_failures = [
    check_name
    for check_name, expected, actual, passed in post_write_checks
    if not passed
]

if post_write_failures:
    raise RuntimeError(
        "Rule Registry post-write certification failed: "
        + " | ".join(post_write_failures)
    )

rule_state_summary = (
    written_registry_df
    .groupBy(
        "rule_state",
        "dependency_status",
        "output_lane_code",
        "active_indicator"
    )
    .agg(
        F.count(F.lit(1)).alias("rule_count")
    )
    .orderBy(
        "rule_state",
        "dependency_status",
        "output_lane_code"
    )
)

display(rule_state_summary)

print("=" * 100)
print("RCAIP IP-006 RULE REGISTRY SHADOW SEED CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Registry contract version:", IP006_REGISTRY_CONTRACT_VERSION)
print("Feature contract version:", IP006_FEATURE_CONTRACT_VERSION)
print("Design status:", IP006_DESIGN_STATUS)
print("Rule count:", written_rule_count)
print("Active rules:", written_active_rule_count)
print("Blocked rules:", written_blocked_rule_count)
print("Design-ready rules:", written_design_ready_rule_count)
print("Rule evaluation performed: NO")
print("Asset Plane modified: NO")
print("Suppression performed: NO")
print("Structural exclusion performed: NO")
print("Next gate: build IP006_ASSET_FEATURE_MART in shadow mode")
print("=" * 100)