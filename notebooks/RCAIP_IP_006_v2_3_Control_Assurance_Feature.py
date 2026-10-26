# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP IP-006 v2.3 - Control Assurance Feature, Evaluation, and Review Sample
# Version: 0.1.3
# Contract: 2.3.0-shadow
#
# Purpose:
#   Convert the certified v2.runtime Asset Feature Mart and current scored Asset Plane
#   into an asset-level control-assurance and visibility product.
#
# Durable products:
#   IP006_CONTROL_ASSURANCE_EVALUATION
#       One row per asset and tool. Expected grain: current Feature Mart assets x 5.
#   IP006_CONTROL_ASSURANCE_FEATURE
#       One wide row per certified asset.
#   IP006_CONTROL_ASSURANCE_REVIEW_SAMPLE
#       Deterministic sample of up to runtime assets per tool/adverse state.
#
# Safety and performance:
#   - Shadow only; no rule activation, finding, ticket, assignment, or remediation.
#   - Reads only durable Post-scoped RCAIP tables and registry metadata.
#   - Does not scan raw enterprise Intune, MDE, Qualys, or Tenable tables.
#   - Existing IP-005 and Report 001 products are not read or modified.
# ============================================================================

from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate runtime, bindings, and shadow safety
# ---------------------------------------------------------------------------

REQUIRED_RUNTIME_OBJECTS = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_TABLES",
    "get_rcaip_table"
]

missing_runtime_objects = [
    object_name
    for object_name in REQUIRED_RUNTIME_OBJECTS
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-006 v2.3 Control Assurance is missing runtime objects: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

CONTROL_ASSURANCE_CONTRACT_VERSION = "2.3.0-shadow"
CONTROL_ASSURANCE_BUILD_VERSION = "0.1.3"
CONTROL_ASSURANCE_BUILD_MODE = "SHADOW"
REVIEW_SAMPLE_SIZE_PER_STATE = 10

# Shadow-calibration reporting thresholds. These are explicit product metadata,
# not hidden assumptions. The 31-90 day range is preserved as REPORTING_STALE;
# values older than 90 days are PRESENT_NOT_REPORTING for persistent controls.
INTUNE_CURRENT_DAYS = 30
MDE_CURRENT_DAYS = 30
QUALYS_CURRENT_DAYS = 30
TENABLE_CURRENT_DAYS = 90
PERSISTENT_NOT_REPORTING_DAYS = 90

INPUT_FEATURE_LOGICAL_NAME = "IP006_ASSET_FEATURE_MART"
INPUT_SCORED_LOGICAL_NAME = "ASSET_RECONCILIATION_SCORED"
RULE_REGISTRY_LOGICAL_NAME = "KB_RULE_REGISTRY_V2_2"
OUTPUT_EVALUATION_LOGICAL_NAME = "IP006_CONTROL_ASSURANCE_EVALUATION"
OUTPUT_FEATURE_LOGICAL_NAME = "IP006_CONTROL_ASSURANCE_FEATURE"
OUTPUT_SAMPLE_LOGICAL_NAME = "IP006_CONTROL_ASSURANCE_REVIEW_SAMPLE"

required_logical_names = [
    INPUT_FEATURE_LOGICAL_NAME,
    INPUT_SCORED_LOGICAL_NAME,
    RULE_REGISTRY_LOGICAL_NAME,
    OUTPUT_EVALUATION_LOGICAL_NAME,
    OUTPUT_FEATURE_LOGICAL_NAME,
    OUTPUT_SAMPLE_LOGICAL_NAME
]

missing_logical_names = [
    logical_name
    for logical_name in required_logical_names
    if logical_name not in RCAIP_TABLES
]

if missing_logical_names:
    raise RuntimeError(
        "Config 001 is missing IP-006 v2.3 bindings: "
        + " | ".join(missing_logical_names)
    )

FEATURE_INPUT_TABLE = get_rcaip_table(INPUT_FEATURE_LOGICAL_NAME)
SCORED_INPUT_TABLE = get_rcaip_table(INPUT_SCORED_LOGICAL_NAME)
RULE_REGISTRY_TABLE = get_rcaip_table(RULE_REGISTRY_LOGICAL_NAME)
EVALUATION_OUTPUT_TABLE = get_rcaip_table(OUTPUT_EVALUATION_LOGICAL_NAME)
FEATURE_OUTPUT_TABLE = get_rcaip_table(OUTPUT_FEATURE_LOGICAL_NAME)
SAMPLE_OUTPUT_TABLE = get_rcaip_table(OUTPUT_SAMPLE_LOGICAL_NAME)

expected_post_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()

for logical_name, physical_name in {
    INPUT_FEATURE_LOGICAL_NAME: FEATURE_INPUT_TABLE,
    INPUT_SCORED_LOGICAL_NAME: SCORED_INPUT_TABLE,
    OUTPUT_EVALUATION_LOGICAL_NAME: EVALUATION_OUTPUT_TABLE,
    OUTPUT_FEATURE_LOGICAL_NAME: FEATURE_OUTPUT_TABLE,
    OUTPUT_SAMPLE_LOGICAL_NAME: SAMPLE_OUTPUT_TABLE
}.items():
    if not str(physical_name).lower().startswith(expected_post_prefix):
        raise RuntimeError(
            "IP-006 v2.3 detected a cross-Post binding: "
            f"{logical_name}={physical_name}"
        )

missing_physical_inputs = [
    table_name
    for table_name in [
        FEATURE_INPUT_TABLE,
        SCORED_INPUT_TABLE,
        RULE_REGISTRY_TABLE
    ]
    if not spark.catalog.tableExists(table_name)
]

if missing_physical_inputs:
    raise RuntimeError(
        "IP-006 v2.3 requires certified durable inputs: "
        + " | ".join(missing_physical_inputs)
    )

rule_registry_df = spark.table(RULE_REGISTRY_TABLE)
if "active_indicator" not in rule_registry_df.columns:
    raise RuntimeError("Rule Registry lacks active_indicator.")

active_rule_count = (
    rule_registry_df
    .filter(F.col("active_indicator").cast("boolean"))
    .count()
)

if active_rule_count != 0:
    raise RuntimeError(
        "Control Assurance is shadow-only and requires zero active rules. "
        f"Observed={active_rule_count}"
    )


def source_status(logical_name):
    if logical_name not in RCAIP_TABLES:
        return "UNREGISTERED"
    physical_name = get_rcaip_table(logical_name)
    return (
        "AVAILABLE"
        if spark.catalog.tableExists(physical_name)
        else "REGISTERED_UNAVAILABLE"
    )


SOURCE_STATUS = {
    "MDE": source_status("MDE_NORM"),
    "INTUNE": source_status("INTUNE_NORM"),
    "QUALYS": source_status("QUALYS_NORM"),
    "UMBRELLA": source_status("UMBRELLA_PUBLIC_IP_VALIDATION"),
    "TENABLE": source_status("TENABLE_NORM")
}

print("IP-006 v2.3 runtime and shadow-safety validation: PASS")
print("Post:", RCAIP_POST_NAME)
print("Run ID:", RCAIP_RUN_ID)
print("Source status:", SOURCE_STATUS)


# ---------------------------------------------------------------------------
# 2. Load and certify the two one-row-per-asset input boundaries
# ---------------------------------------------------------------------------

feature_input_df = (
    spark.table(FEATURE_INPUT_TABLE)
    .filter(
        (F.lower(F.trim(F.col("post_slug"))) == F.lit(str(RCAIP_POST_SLUG).lower()))
        & (F.col("rcaip_run_id") == F.lit(RCAIP_RUN_ID))
    )
)

scored_input_df = (
    spark.table(SCORED_INPUT_TABLE)
    .filter(
        (F.lower(F.trim(F.col("post_slug"))) == F.lit(str(RCAIP_POST_SLUG).lower()))
        & (F.col("rcaip_run_id") == F.lit(RCAIP_RUN_ID))
    )
)

FEATURE_REQUIRED_COLUMNS = {
    "post_name", "post_slug", "rcaip_run_id", "asset_key", "host_key",
    "canonical_name", "canonical_serial", "canonical_ip",
    "device_family", "device_family_confidence", "network_zone",
    "management_model", "management_owner", "identity_basis",
    "identity_strength", "source_mask_raw", "source_mask_active",
    "source_mask_stale", "current_observation_status",
    "attribution_confidence", "attribution_conflict_flag", "conflict_count",
    "printer_signature", "infrastructure_signature", "acr_band",
    "qualys_risk_band", "current_post_token_match",
    "foreign_post_token_match", "is_ip_only_identity"
}

SCORED_REQUIRED_COLUMNS = {
    "post_slug", "rcaip_run_id", "asset_key", "cmdb_present",
    "nmap_record_present", "nmap_active", "tenable_record_present",
    "tenable_active", "tenable_event_time", "intune_record_present",
    "intune_active", "intune_last_contact",
    "intune_compliance_state_normalized",
    "intune_compliance_evidence_source", "intune_device_health_state",
    "intune_encryption_state", "mde_record_present", "mde_active",
    "mde_last_seen", "mde_onboarding_status", "mde_sensor_health_state",
    "mde_exposure_level", "qualys_record_present", "qualys_active",
    "qualys_inventory_last_updated_on", "qualys_agent_id"
}

missing_feature_columns = sorted(
    FEATURE_REQUIRED_COLUMNS - set(feature_input_df.columns)
)
missing_scored_columns = sorted(
    SCORED_REQUIRED_COLUMNS - set(scored_input_df.columns)
)

if missing_feature_columns or missing_scored_columns:
    raise RuntimeError(
        "IP-006 v2.3 input contract failure. "
        + (
            "Feature Mart missing: " + " | ".join(missing_feature_columns)
            if missing_feature_columns else ""
        )
        + (
            " ; Scored Asset Plane missing: " + " | ".join(missing_scored_columns)
            if missing_scored_columns else ""
        )
    )


def profile_one_row_per_asset(dataframe, label):
    row = (
        dataframe
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct("asset_key").alias("distinct_asset_count"),
            F.sum(F.when(F.col("asset_key").isNull(), 1).otherwise(0)).alias("null_asset_count")
        )
        .first()
    )
    row_count = int(row["row_count"] or 0)
    distinct_count = int(row["distinct_asset_count"] or 0)
    null_count = int(row["null_asset_count"] or 0)
    if row_count == 0 or row_count != distinct_count or null_count != 0:
        raise RuntimeError(
            f"{label} failed one-row-per-asset certification. "
            f"Rows={row_count}; distinct={distinct_count}; null={null_count}"
        )
    return row_count


feature_input_count = profile_one_row_per_asset(
    feature_input_df,
    "IP006_ASSET_FEATURE_MART"
)
scored_input_count = profile_one_row_per_asset(
    scored_input_df,
    "ASSET_RECONCILIATION_SCORED"
)

feature_only_count = (
    feature_input_df
    .select("asset_key")
    .join(scored_input_df.select("asset_key"), on="asset_key", how="left_anti")
    .count()
)
scored_only_count = (
    scored_input_df
    .select("asset_key")
    .join(feature_input_df.select("asset_key"), on="asset_key", how="left_anti")
    .count()
)

if (
    feature_input_count != scored_input_count
    or feature_only_count != 0
    or scored_only_count != 0
):
    raise RuntimeError(
        "Feature Mart and scored Asset Plane are not aligned to the same asset snapshot. "
        f"Feature={feature_input_count}; scored={scored_input_count}; "
        f"feature_only={feature_only_count}; scored_only={scored_only_count}. "
        "Rebuild RCAIP_IP_006_Asset_Feature_Mart_Shadow from the current "
        "ASSET_RECONCILIATION_SCORED table before rerunning Control Assurance."
    )

print("IP-006 v2.3 input boundary certification: PASS")
print("Certified aligned assets:", feature_input_count)


# ---------------------------------------------------------------------------
# 3. Build one compact persisted asset evidence frame
# ---------------------------------------------------------------------------

FEATURE_BASE_COLUMNS = [
    "post_name", "post_slug", "rcaip_run_id", "asset_key", "host_key",
    "canonical_name", "canonical_serial", "canonical_ip",
    "device_family", "device_family_confidence", "network_zone",
    "management_model", "management_owner", "identity_basis",
    "identity_strength", "source_mask_raw", "source_mask_active",
    "source_mask_stale", "current_observation_status",
    "attribution_confidence", "attribution_conflict_flag", "conflict_count",
    "printer_signature", "infrastructure_signature", "acr_band",
    "qualys_risk_band", "current_post_token_match",
    "foreign_post_token_match", "is_ip_only_identity"
]

SCORED_EVIDENCE_COLUMNS = [
    "cmdb_present", "nmap_record_present", "nmap_active",
    "tenable_record_present", "tenable_active", "tenable_event_time",
    "intune_record_present", "intune_active", "intune_last_contact",
    "intune_compliance_state_normalized",
    "intune_compliance_evidence_source", "intune_device_health_state",
    "intune_encryption_state", "mde_record_present", "mde_active",
    "mde_last_seen", "mde_onboarding_status", "mde_sensor_health_state",
    "mde_exposure_level", "qualys_record_present", "qualys_active",
    "qualys_inventory_last_updated_on", "qualys_agent_id"
]

asset_base_df = (
    feature_input_df.alias("f")
    .join(
        scored_input_df.select("asset_key", *SCORED_EVIDENCE_COLUMNS).alias("s"),
        on="asset_key",
        how="inner"
    )
    .select(
        *[F.col(f"f.{column_name}").alias(column_name) for column_name in FEATURE_BASE_COLUMNS],
        *[F.col(f"s.{column_name}").alias(column_name) for column_name in SCORED_EVIDENCE_COLUMNS]
    )
    .withColumn(
        "network_scope",
        F.when(F.col("network_zone").isin(
            "OPENNET", "NEN", "ENTERPRISE_WIFI", "PUBLIC_EXTERNAL",
            "UNKNOWN_INTERNAL", "NO_IP"
        ), F.col("network_zone")).otherwise(F.lit("UNKNOWN_INTERNAL"))
    )
    .withColumn(
        "device_scope",
        F.when(F.col("device_family").isin("WORKSTATION", "MOBILE"), "END_USER_ENDPOINT")
        .when(F.col("device_family") == "SERVER", "SERVER")
        .when(F.col("device_family") == "PRINTER_MFD", "PRINTER_MFD")
        .when(F.col("device_family") == "NETWORK_INFRASTRUCTURE", "NETWORK_INFRASTRUCTURE")
        .when(F.col("device_family") == "SECURITY_APPLIANCE", "SECURITY_APPLIANCE")
        .when(F.col("device_family") == "IOT_OT", "IOT_OT")
        .otherwise("UNKNOWN_DEVICE_SCOPE")
    )
    .withColumn(
        "operational_boundary",
        F.when(F.col("management_model") == "POST_MANAGED", "POST_MANAGED")
        .when(F.col("management_model") == "ENTERPRISE_MANAGED", "ENTERPRISE_MANAGED_POST_OPERATED")
        .when(F.col("management_model") == "SHARED", "SHARED_MANAGEMENT")
        .when(F.col("management_model") == "NON_ASSET", "NON_ASSET")
        .when(F.upper(F.col("cmdb_present")) == "Y", "ENTERPRISE_ATTRIBUTED_TO_POST")
        .when(
            (F.col("nmap_active") == 1)
            | (F.col("tenable_active") == 1)
            | (F.col("intune_active") == 1)
            | (F.col("mde_active") == 1)
            | (F.col("qualys_active") == 1),
            "OBSERVED_IN_POST_SCOPE"
        )
        .otherwise("UNRESOLVED_ATTRIBUTION")
    )
    .withColumn(
        "operational_boundary_basis",
        F.when(F.col("management_model").isin("POST_MANAGED", "ENTERPRISE_MANAGED", "SHARED", "NON_ASSET"),
               F.concat(F.lit("V2_2_MANAGEMENT_MODEL:"), F.col("management_model")))
        .when(F.upper(F.col("cmdb_present")) == "Y", F.lit("CMDB_POST_ATTRIBUTION_NOT_MANAGEMENT_PROOF"))
        .when(F.col("source_mask_active") != "NONE", F.concat(F.lit("CURRENT_POST_SCOPE_OBSERVATION:"), F.col("source_mask_active")))
        .otherwise(F.lit("INSUFFICIENT_BOUNDARY_EVIDENCE"))
    )
    .withColumn(
        "operational_boundary_confidence",
        F.when(F.col("management_model").isin("POST_MANAGED", "ENTERPRISE_MANAGED", "SHARED", "NON_ASSET"), "HIGH")
        .when(F.upper(F.col("cmdb_present")) == "Y", "MODERATE")
        .otherwise("LOW")
    )
    .withColumn(
        "visibility_evidence_count",
        (
            F.coalesce(F.col("nmap_active").cast("int"), F.lit(0))
            + F.coalesce(F.col("tenable_active").cast("int"), F.lit(0))
            + F.coalesce(F.col("intune_active").cast("int"), F.lit(0))
            + F.coalesce(F.col("mde_active").cast("int"), F.lit(0))
            + F.coalesce(F.col("qualys_active").cast("int"), F.lit(0))
        ).cast("int")
    )
    .withColumn(
        "visibility_evidence_mask",
        F.when(
            F.length(F.concat_ws(
                "|",
                F.when(F.upper(F.col("cmdb_present")) == "Y", F.lit("CMDB")),
                F.when(F.col("nmap_active") == 1, F.lit("NMAP")),
                F.when(F.col("tenable_active") == 1, F.lit("TENABLE")),
                F.when(F.col("intune_active") == 1, F.lit("INTUNE")),
                F.when(F.col("mde_active") == 1, F.lit("MDE")),
                F.when(F.col("qualys_active") == 1, F.lit("QUALYS"))
            )) > 0,
            F.concat_ws(
                "|",
                F.when(F.upper(F.col("cmdb_present")) == "Y", F.lit("CMDB")),
                F.when(F.col("nmap_active") == 1, F.lit("NMAP")),
                F.when(F.col("tenable_active") == 1, F.lit("TENABLE")),
                F.when(F.col("intune_active") == 1, F.lit("INTUNE")),
                F.when(F.col("mde_active") == 1, F.lit("MDE")),
                F.when(F.col("qualys_active") == 1, F.lit("QUALYS"))
            )
        ).otherwise(F.lit("NONE"))
    )
    .withColumn(
        "visibility_class",
        F.when(F.col("is_ip_only_identity") | F.col("identity_strength").isin("WEAK", "UNKNOWN"), "IDENTITY_UNRESOLVED")
        .when(F.col("visibility_evidence_count") >= 4, "BROAD_MULTI_SOURCE")
        .when(F.col("visibility_evidence_count") == 3, "STRONG_MULTI_SOURCE")
        .when(F.col("visibility_evidence_count") == 2, "PARTIAL_MULTI_SOURCE")
        .when(F.col("visibility_evidence_count") == 1, "SINGLE_SOURCE")
        .otherwise("NO_CURRENT_VISIBILITY")
    )
    .withColumn(
        "post_visibility_state",
        F.when(
            ((F.upper(F.col("cmdb_present")) == "Y") | (F.col("nmap_active") == 1))
            & ((F.col("tenable_active") == 1) | (F.col("intune_active") == 1)
               | (F.col("mde_active") == 1) | (F.col("qualys_active") == 1)),
            "POST_AND_ENTERPRISE_VISIBLE"
        )
        .when((F.upper(F.col("cmdb_present")) == "Y") | (F.col("nmap_active") == 1), "POST_VISIBLE_ONLY")
        .when((F.col("tenable_active") == 1) | (F.col("intune_active") == 1)
              | (F.col("mde_active") == 1) | (F.col("qualys_active") == 1), "ENTERPRISE_VISIBLE_ONLY")
        .otherwise("NO_CURRENT_VISIBILITY")
    )
    .withColumn(
        "active_unresolved_ip_candidate",
        F.col("is_ip_only_identity")
        & ((F.col("nmap_active") == 1) | (F.col("tenable_active") == 1))
    )
    .withColumn(
        "active_unresolved_ip_observation_class",
        F.when(
            F.col("is_ip_only_identity")
            & (F.col("nmap_active") == 1)
            & (F.col("tenable_active") == 1),
            "NMAP_AND_TENABLE_ACTIVE"
        )
        .when(F.col("is_ip_only_identity") & (F.col("nmap_active") == 1), "NMAP_ONLY_ACTIVE")
        .when(F.col("is_ip_only_identity") & (F.col("tenable_active") == 1), "TENABLE_ONLY_ACTIVE")
        .when(F.col("is_ip_only_identity"), "IP_ONLY_NO_CURRENT_NMAP_OR_TENABLE")
        .otherwise("NOT_IP_ONLY")
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

asset_base_count = asset_base_df.count()
if asset_base_count != feature_input_count:
    raise RuntimeError(
        "Compact asset evidence join lost or duplicated assets. "
        f"Expected={feature_input_count}; actual={asset_base_count}"
    )

print("Compact one-row-per-asset evidence frame: PASS")


# ---------------------------------------------------------------------------
# 4. Preserve the field-validated calibration applicability logic
# ---------------------------------------------------------------------------

mde_applicability = (
    F.when(F.col("device_scope") == "END_USER_ENDPOINT", "APPLICABLE_END_USER_ENDPOINT")
    .when(F.col("device_scope").isin(
        "PRINTER_MFD", "NETWORK_INFRASTRUCTURE", "SECURITY_APPLIANCE", "IOT_OT"
    ), "NOT_APPLICABLE")
    .otherwise("REVIEW_REQUIRED_DEVICE_SCOPE")
)

intune_applicability = (
    F.when(F.col("network_scope") == "NEN", "NOT_APPLICABLE")
    .when(
        (F.col("network_scope") == "OPENNET")
        & (F.col("device_scope") == "END_USER_ENDPOINT"),
        "APPLICABLE_OPENNET_ENDPOINT"
    )
    .when(F.col("device_scope").isin(
        "PRINTER_MFD", "NETWORK_INFRASTRUCTURE", "SECURITY_APPLIANCE", "IOT_OT"
    ), "NOT_APPLICABLE")
    .otherwise("REVIEW_REQUIRED_NETWORK_OR_DEVICE_SCOPE")
)

qualys_applicability = (
    F.when(F.col("network_scope") == "OPENNET", "NOT_APPLICABLE")
    .when(
        (F.col("network_scope") == "NEN")
        & (F.col("device_scope") == "END_USER_ENDPOINT"),
        "APPLICABLE_NEN_ENDPOINT"
    )
    .when(F.col("device_scope").isin(
        "PRINTER_MFD", "NETWORK_INFRASTRUCTURE", "SECURITY_APPLIANCE", "IOT_OT"
    ), "NOT_APPLICABLE")
    .otherwise("REVIEW_REQUIRED_NETWORK_OR_DEVICE_SCOPE")
)

umbrella_applicability = (
    F.when(F.col("network_scope") == "NEN", "APPLICABLE_NEN_METHOD_UNRESOLVED")
    .when(F.col("network_scope") == "OPENNET", "NOT_APPLICABLE")
    .otherwise("REVIEW_REQUIRED_NETWORK_SCOPE")
)

tenable_applicability = F.lit("EVIDENCE_SOURCE_NOT_REQUIRED_CONTROL")


# ---------------------------------------------------------------------------
# 5. Native-Spark tool evaluation builder
# ---------------------------------------------------------------------------

COMMON_EVALUATION_COLUMNS = [
    "post_name", "post_slug", "rcaip_run_id", "asset_key", "host_key",
    "canonical_name", "canonical_serial", "canonical_ip",
    "device_family", "device_scope", "device_family_confidence",
    "network_scope", "operational_boundary", "operational_boundary_basis",
    "operational_boundary_confidence", "management_model", "management_owner",
    "identity_basis", "identity_strength", "source_mask_raw",
    "source_mask_active", "source_mask_stale", "current_observation_status",
    "visibility_evidence_count", "visibility_evidence_mask", "visibility_class",
    "post_visibility_state", "attribution_confidence",
    "attribution_conflict_flag", "conflict_count",
    "active_unresolved_ip_candidate", "active_unresolved_ip_observation_class"
]


def timestamp_age_days(timestamp_column):
    return F.when(
        timestamp_column.isNotNull(),
        F.datediff(F.current_date(), F.to_date(timestamp_column))
    ).cast("int")


def build_tool_evaluation(
    tool_name,
    control_role,
    applicability_expression,
    applicability_basis,
    source_status_value,
    record_column_name=None,
    timestamp_column_name=None,
    current_days=None,
    configuration_candidate=None,
    configuration_evidence=None,
    evidence_limitation=None,
    evidence_only=False
):
    source_available = source_status_value == "AVAILABLE"
    record_expression = (
        F.coalesce(F.col(record_column_name).cast("int"), F.lit(0))
        if record_column_name else F.lit(0)
    )
    timestamp_expression = (
        F.col(timestamp_column_name).cast("timestamp")
        if timestamp_column_name else F.lit(None).cast("timestamp")
    )
    age_expression = timestamp_age_days(timestamp_expression)
    configuration_candidate_expression = (
        configuration_candidate
        if configuration_candidate is not None
        else F.lit("UNKNOWN_NOT_MODELED")
    )
    configuration_evidence_expression = (
        configuration_evidence
        if configuration_evidence is not None
        else F.lit(None).cast("string")
    )

    working_df = (
        asset_base_df
        .withColumn("_applicability_state", applicability_expression)
        .withColumn("_record_present", record_expression)
        .withColumn("_evidence_timestamp", timestamp_expression)
        .withColumn("_evidence_age_days", age_expression)
        .withColumn("_configuration_candidate", configuration_candidate_expression)
        .withColumn("_configuration_evidence", configuration_evidence_expression)
        .withColumn("_source_available", F.lit(source_available))
    )

    applicable_condition = F.col("_applicability_state").startswith("APPLICABLE")
    review_condition = F.col("_applicability_state").contains("REVIEW_REQUIRED")
    evidence_only_condition = F.col("_applicability_state") == "EVIDENCE_SOURCE_NOT_REQUIRED_CONTROL"
    identity_unresolved_condition = (
        F.col("is_ip_only_identity")
        | F.col("identity_strength").isin("WEAK", "UNKNOWN")
    )

    presence_state = (
        F.when(F.col("_applicability_state") == "NOT_APPLICABLE", "NOT_APPLICABLE")
        .when(~F.col("_source_available"), "UNKNOWN_SOURCE_UNAVAILABLE")
        .when(F.col("_record_present") == 1, "PRESENT")
        .when(applicable_condition & identity_unresolved_condition, "UNKNOWN_IDENTITY_NOT_RECONCILED")
        .when(applicable_condition, "MISSING")
        .when(review_condition, "UNKNOWN_IDENTITY_NOT_RECONCILED")
        .when(evidence_only_condition, "MISSING")
        .otherwise("UNKNOWN_IDENTITY_NOT_RECONCILED")
    )

    if evidence_only:
        reporting_state = (
            F.when(~F.col("_source_available"), "UNKNOWN_SOURCE_UNAVAILABLE")
            .when(F.col("_record_present") == 0, "NO_RECORD")
            .when(F.col("_evidence_timestamp").isNull() | (F.col("_evidence_age_days") < 0), "UNKNOWN_TIMESTAMP")
            .when(F.col("_evidence_age_days") <= F.lit(int(current_days)), "REPORTING_CURRENT")
            .otherwise("REPORTING_STALE")
        )
    else:
        reporting_state = (
            F.when(F.col("_applicability_state") == "NOT_APPLICABLE", "NOT_APPLICABLE")
            .when(~F.col("_source_available"), "UNKNOWN_SOURCE_UNAVAILABLE")
            .when(F.col("_record_present") == 0, "NO_RECORD")
            .when(F.col("_evidence_timestamp").isNull() | (F.col("_evidence_age_days") < 0), "UNKNOWN_TIMESTAMP")
            .when(F.col("_evidence_age_days") <= F.lit(int(current_days)), "REPORTING_CURRENT")
            .when(F.col("_evidence_age_days") <= F.lit(PERSISTENT_NOT_REPORTING_DAYS), "REPORTING_STALE")
            .otherwise("PRESENT_NOT_REPORTING")
        )

    configuration_state = (
        F.when(F.col("_applicability_state") == "NOT_APPLICABLE", "NOT_APPLICABLE")
        .when(~F.col("_source_available"), "UNKNOWN_SOURCE_UNAVAILABLE")
        .when(F.col("_record_present") == 0, "UNKNOWN_NOT_MODELED")
        .when(evidence_only_condition, "NOT_APPLICABLE")
        .otherwise(F.col("_configuration_candidate"))
    )

    if evidence_only:
        assurance_state = (
            F.when(~F.col("_source_available"), "SOURCE_UNAVAILABLE")
            .when(F.col("_record_present") == 0, "EVIDENCE_ABSENT")
            .when(reporting_state == "REPORTING_CURRENT", "EVIDENCE_CURRENT")
            .when(reporting_state == "REPORTING_STALE", "EVIDENCE_STALE")
            .otherwise("EVIDENCE_TIMESTAMP_UNKNOWN")
        )
    else:
        assurance_state = (
            F.when(F.col("_applicability_state") == "NOT_APPLICABLE", "NOT_APPLICABLE")
            .when(~F.col("_source_available"), "SOURCE_UNAVAILABLE")
            .when(review_condition, "APPLICABILITY_REVIEW_REQUIRED")
            .when(presence_state == "UNKNOWN_IDENTITY_NOT_RECONCILED", "IDENTITY_RECONCILIATION_REQUIRED")
            .when(presence_state == "MISSING", "MISSING")
            .when(reporting_state == "PRESENT_NOT_REPORTING", "PRESENT_NOT_REPORTING")
            .when(reporting_state == "REPORTING_STALE", "REPORTING_STALE")
            .when(reporting_state == "UNKNOWN_TIMESTAMP", "PRESENT_NOT_REPORTING")
            .when(configuration_state == "MISCONFIGURED", "MISCONFIGURED")
            .when(configuration_state == "NONCOMPLIANT", "NONCOMPLIANT")
            .when(configuration_state == "CONFIGURED_CORRECTLY", "HEALTHY")
            .otherwise("PRESENT_REPORTING_CONFIGURATION_UNKNOWN")
        )

    review_priority = (
        F.when(assurance_state.isin("NONCOMPLIANT", "MISCONFIGURED", "MISSING"), "HIGH")
        .when(assurance_state.isin(
            "PRESENT_NOT_REPORTING", "REPORTING_STALE",
            "IDENTITY_RECONCILIATION_REQUIRED", "APPLICABILITY_REVIEW_REQUIRED",
            "SOURCE_UNAVAILABLE", "PRESENT_REPORTING_CONFIGURATION_UNKNOWN"
        ), "MEDIUM")
        .when(assurance_state.isin(
            "EVIDENCE_STALE", "EVIDENCE_ABSENT", "EVIDENCE_TIMESTAMP_UNKNOWN"
        ), "LOW")
        .otherwise("NONE")
    )

    routing_scope = (
        F.when(assurance_state.isin(
            "NONCOMPLIANT", "MISCONFIGURED", "MISSING",
            "PRESENT_NOT_REPORTING", "REPORTING_STALE"
        ), "JOINT_POST_ENTERPRISE_VALIDATION")
        .when(assurance_state == "SOURCE_UNAVAILABLE", "ENTERPRISE_SERVICE_OWNER")
        .when(assurance_state.isin(
            "IDENTITY_RECONCILIATION_REQUIRED", "APPLICABILITY_REVIEW_REQUIRED",
            "PRESENT_REPORTING_CONFIGURATION_UNKNOWN"
        ), "RCSO_ANALYSIS")
        .otherwise("DEFER_LOW_SIGNAL")
    )

    adverse_state_flag = ~assurance_state.isin(
        "HEALTHY", "NOT_APPLICABLE", "EVIDENCE_CURRENT"
    )

    validation_question = (
        F.when(assurance_state == "MISSING",
               F.concat(F.lit("RCAIP does not see a current "), F.lit(tool_name),
                        F.lit(" record for an applicable asset. Confirm identity, applicability, enrollment or agent status, and ingestion.")))
        .when(assurance_state.isin("PRESENT_NOT_REPORTING", "REPORTING_STALE"),
              F.concat(F.lit(tool_name), F.lit(" is present but not current. Confirm endpoint health, last successful reporting, and source ingestion.")))
        .when(assurance_state.isin("NONCOMPLIANT", "MISCONFIGURED"),
              F.concat(F.lit(tool_name), F.lit(" reports an adverse configuration or compliance state. Confirm the source value, policy scope, exception status, and remediation owner.")))
        .when(assurance_state == "SOURCE_UNAVAILABLE",
              F.concat(F.lit(tool_name), F.lit(" evidence is unavailable to RCAIP. Confirm the authoritative source and approved ingestion or coverage method.")))
        .when(assurance_state == "IDENTITY_RECONCILIATION_REQUIRED",
              F.concat(F.lit(tool_name), F.lit(" cannot be assessed reliably until the asset identity is reconciled beyond IP-only or weak identity evidence.")))
        .when(assurance_state == "APPLICABILITY_REVIEW_REQUIRED",
              F.concat(F.lit(tool_name), F.lit(" applicability is not yet defensible for this network or device class. Confirm the governing control boundary.")))
        .when(assurance_state == "PRESENT_REPORTING_CONFIGURATION_UNKNOWN",
              F.concat(F.lit(tool_name), F.lit(" is present and current, but the available normalized evidence does not prove configuration health.")))
        .when(assurance_state.startswith("EVIDENCE_"),
              F.concat(F.lit(tool_name), F.lit(" is audit or visibility evidence only; absence or staleness is not automatically a missing endpoint control.")))
        .otherwise(F.lit("No validation question generated for a healthy or not-applicable relationship."))
    )

    return (
        working_df
        .select(*COMMON_EVALUATION_COLUMNS,
                "is_ip_only_identity",
                "_applicability_state", "_record_present", "_evidence_timestamp",
                "_evidence_age_days", "_configuration_candidate",
                "_configuration_evidence", "_source_available")
        .withColumn("tool_name", F.lit(tool_name))
        .withColumn("control_role", F.lit(control_role))
        .withColumn("source_status", F.lit(source_status_value))
        .withColumn("applicability_state", F.col("_applicability_state"))
        .withColumn("applicability_basis", F.lit(applicability_basis))
        .withColumn("presence_state", presence_state)
        .withColumn("reporting_state", reporting_state)
        .withColumn("configuration_state", configuration_state)
        .withColumn("assurance_state", assurance_state)
        .withColumn("configuration_evidence_value", F.col("_configuration_evidence"))
        .withColumn("evidence_timestamp", F.col("_evidence_timestamp"))
        .withColumn("evidence_age_days", F.col("_evidence_age_days"))
        .withColumn("reporting_current_threshold_days", F.lit(int(current_days) if current_days is not None else None).cast("int"))
        .withColumn("evidence_limitation", F.lit(evidence_limitation or "No additional limitation recorded."))
        .withColumn("review_priority", review_priority)
        .withColumn("routing_scope", routing_scope)
        .withColumn("adverse_state_flag", adverse_state_flag.cast("boolean"))
        .withColumn("validation_question", validation_question)
        .withColumn("control_assurance_contract_version", F.lit(CONTROL_ASSURANCE_CONTRACT_VERSION))
        .withColumn("control_assurance_build_version", F.lit(CONTROL_ASSURANCE_BUILD_VERSION))
        .withColumn("control_assurance_build_mode", F.lit(CONTROL_ASSURANCE_BUILD_MODE))
        .withColumn("generated_at_utc", F.current_timestamp())
        .drop(
            "is_ip_only_identity",
            "_applicability_state", "_record_present", "_evidence_timestamp",
            "_evidence_age_days", "_configuration_candidate",
            "_configuration_evidence", "_source_available"
        )
    )


# ---------------------------------------------------------------------------
# 6. Build the five tool/control relationships
# ---------------------------------------------------------------------------

intune_configuration = (
    F.when(F.col("intune_compliance_state_normalized") == "REPORTED_COMPLIANT", "CONFIGURED_CORRECTLY")
    .when(F.col("intune_compliance_state_normalized").isin(
        "REPORTED_NONCOMPLIANT", "REPORTED_IN_GRACE_PERIOD"
    ), "NONCOMPLIANT")
    .when(F.col("intune_compliance_state_normalized").startswith("UNMAPPED"), "UNKNOWN_VALUE_REQUIRES_MAPPING")
    .otherwise("UNKNOWN_NOT_MODELED")
)

intune_evidence = F.concat_ws(
    " | ",
    F.concat(F.lit("COMPLIANCE="), F.coalesce(F.col("intune_compliance_state_normalized"), F.lit("NULL"))),
    F.concat(F.lit("SOURCE="), F.coalesce(F.col("intune_compliance_evidence_source"), F.lit("NULL"))),
    F.concat(F.lit("HEALTH="), F.coalesce(F.col("intune_device_health_state"), F.lit("NULL"))),
    F.concat(F.lit("ENCRYPTION="), F.coalesce(F.col("intune_encryption_state"), F.lit("NULL")))
)

mde_configuration = (
    F.when(F.lower(F.trim(F.col("mde_sensor_health_state"))) == "misconfigured", "MISCONFIGURED")
    .when(
        (F.lower(F.trim(F.col("mde_onboarding_status"))) == "onboarded")
        & (F.lower(F.trim(F.col("mde_sensor_health_state"))) == "active"),
        "CONFIGURED_CORRECTLY"
    )
    .otherwise("UNKNOWN_NOT_MODELED")
)

mde_evidence = F.concat_ws(
    " | ",
    F.concat(F.lit("ONBOARDING="), F.coalesce(F.col("mde_onboarding_status"), F.lit("NULL"))),
    F.concat(F.lit("SENSOR="), F.coalesce(F.col("mde_sensor_health_state"), F.lit("NULL"))),
    F.concat(F.lit("EXPOSURE_CONTEXT="), F.coalesce(F.col("mde_exposure_level"), F.lit("NULL")))
)

qualys_evidence = F.concat_ws(
    " | ",
    F.concat(F.lit("AGENT_ID="), F.coalesce(F.col("qualys_agent_id"), F.lit("NULL"))),
    F.lit("CONFIGURATION_SOURCE=NOT_MODELED")
)

mde_frame = build_tool_evaluation(
    "MDE",
    "PERSISTENT_ENDPOINT_CONTROL",
    mde_applicability,
    "Field-validated baseline: MDE applies to end-user endpoints; non-end-user devices are excluded. Servers and unknown device classes require policy confirmation.",
    SOURCE_STATUS["MDE"],
    "mde_record_present",
    "mde_last_seen",
    MDE_CURRENT_DAYS,
    mde_configuration,
    mde_evidence,
    "MDE onboarding and sensor health are direct evidence. Inactive, unsupported, insufficient-info, or null values remain review states. Exposure is prioritization context only."
)

intune_frame = build_tool_evaluation(
    "INTUNE",
    "PERSISTENT_ENDPOINT_MANAGEMENT_CONTROL",
    intune_applicability,
    "Field-validated baseline: Intune applies to OpenNet end-user endpoints and does not apply to NEN endpoints. Enterprise Wi-Fi, servers, and unknown classifications remain review.",
    SOURCE_STATUS["INTUNE"],
    "intune_record_present",
    "intune_last_contact",
    INTUNE_CURRENT_DAYS,
    intune_configuration,
    intune_evidence,
    "Reported Intune compliance, health, and encryption are source evidence. They do not independently prove every required policy was assigned or every volume is protected."
)

qualys_frame = build_tool_evaluation(
    "QUALYS",
    "PERSISTENT_NEN_ENDPOINT_CONTROL",
    qualys_applicability,
    "Field-validated baseline: Qualys applies to NEN endpoint devices and does not apply to OpenNet endpoints. Other network or device scopes remain review.",
    SOURCE_STATUS["QUALYS"],
    "qualys_record_present",
    "qualys_inventory_last_updated_on",
    QUALYS_CURRENT_DAYS,
    F.lit("UNKNOWN_NOT_MODELED"),
    qualys_evidence,
    "QUALYS_NORM currently supports presence and reporting evidence, not policy or agent-configuration compliance."
)

umbrella_frame = build_tool_evaluation(
    "UMBRELLA",
    "NEN_AGENT_OR_NETWORK_EDGE_CONTROL",
    umbrella_applicability,
    "Field-validated baseline: Umbrella protects NEN through endpoint-agent or network-edge enforcement. Asset-level method and ingestion remain unresolved.",
    SOURCE_STATUS["UMBRELLA"],
    None,
    None,
    30,
    F.lit("UNKNOWN_SOURCE_UNAVAILABLE"),
    F.lit("SOURCE_UNAVAILABLE"),
    "Umbrella endpoint absence must not be called a control gap when network-edge enforcement may satisfy the requirement. Current runtime source evidence is unavailable."
)

tenable_frame = build_tool_evaluation(
    "TENABLE",
    "AUDIT_AND_VISIBILITY_EVIDENCE",
    tenable_applicability,
    "Field-validated baseline: Tenable is audit, freshness, location, vulnerability, and identity-corroboration evidence, not a mandatory persistent endpoint-control denominator.",
    SOURCE_STATUS["TENABLE"],
    "tenable_record_present",
    "tenable_event_time",
    TENABLE_CURRENT_DAYS,
    F.lit("NOT_APPLICABLE"),
    F.concat(F.lit("ACR_BAND="), F.coalesce(F.col("acr_band"), F.lit("NULL"))),
    "Tenable evidence may support visibility and prioritization. Absence is not automatically a missing endpoint agent or Post control failure.",
    evidence_only=True
)

evaluation_df = (
    mde_frame
    .unionByName(intune_frame)
    .unionByName(qualys_frame)
    .unionByName(umbrella_frame)
    .unionByName(tenable_frame)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

expected_evaluation_rows = feature_input_count * 5
evaluation_row_count = evaluation_df.count()

if evaluation_row_count != expected_evaluation_rows:
    raise RuntimeError(
        "Five-tool evaluation row-count reconciliation failed. "
        f"Expected={expected_evaluation_rows}; actual={evaluation_row_count}"
    )


# ---------------------------------------------------------------------------
# 7. Certify calibration denominators before durable writes
# ---------------------------------------------------------------------------

evaluation_profile = (
    evaluation_df
    .agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(F.concat_ws("|", "asset_key", "tool_name")).alias("distinct_relationship_count"),
        F.sum(F.when(F.col("applicability_state").startswith("APPLICABLE"), 1).otherwise(0)).alias("applicable_count"),
        F.sum(F.when(F.col("applicability_state").contains("REVIEW_REQUIRED"), 1).otherwise(0)).alias("review_count"),
        F.sum(F.when(F.col("active_unresolved_ip_candidate"), 1).otherwise(0)).alias("unresolved_relationship_count")
    )
    .first()
)

applicable_relationship_count = int(evaluation_profile["applicable_count"] or 0)
review_relationship_count = int(evaluation_profile["review_count"] or 0)
distinct_relationship_count = int(evaluation_profile["distinct_relationship_count"] or 0)

active_unresolved_asset_count = (
    asset_base_df
    .filter(F.col("active_unresolved_ip_candidate"))
    .select("asset_key")
    .distinct()
    .count()
)

prewrite_failures = []
if distinct_relationship_count != evaluation_row_count:
    prewrite_failures.append("duplicate asset/tool relationships")

# Optional reference metrics are configuration/evidence data, never analytical
# code. If a certified runtime source publishes RCAIP_IP006_REFERENCE_METRICS,
# compare those values with the current product. Otherwise the analytical
# product proceeds without a historical-reference comparison.
#
# Supported runtime shape:
#
#     RCAIP_IP006_REFERENCE_METRICS = {
#         "metric_name": integer_reference_value,
#         ...
#     }
#
# Reference metrics are diagnostic only. Structural parity and the
# one-row-per-asset/tool-relationship contract remain the hard gates.
REFERENCE_BASELINE_STATUS = "NOT_CONFIGURED"
REFERENCE_BASELINE_DRIFT_COUNT = 0
reference_baseline_rows = []

current_reference_metric_values = {
    "asset_count": int(feature_input_count),
    "five_tool_relationship_count": int(evaluation_row_count),
    "applicable_relationship_count": int(applicable_relationship_count),
    "review_required_relationship_count": int(review_relationship_count),
    "active_unresolved_ip_asset_count": int(active_unresolved_asset_count),
}

configured_reference_metrics = globals().get(
    "RCAIP_IP006_REFERENCE_METRICS",
    None
)

if configured_reference_metrics is not None:
    if not isinstance(configured_reference_metrics, dict):
        raise TypeError(
            "RCAIP_IP006_REFERENCE_METRICS must be a dictionary when provided."
        )

    unknown_reference_metrics = sorted(
        set(configured_reference_metrics.keys())
        -
        set(current_reference_metric_values.keys())
    )

    if unknown_reference_metrics:
        raise RuntimeError(
            "RCAIP_IP006_REFERENCE_METRICS contains unsupported metric(s): "
            + " | ".join(unknown_reference_metrics)
        )

    reference_metrics = []

    for metric_name in sorted(configured_reference_metrics.keys()):
        reference_value = configured_reference_metrics[metric_name]

        if isinstance(reference_value, bool):
            raise TypeError(
                f"Reference metric {metric_name} must be an integer, not boolean."
            )

        try:
            normalized_reference_value = int(reference_value)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Reference metric {metric_name} must be integer-compatible."
            ) from exc

        if normalized_reference_value < 0:
            raise ValueError(
                f"Reference metric {metric_name} cannot be negative."
            )

        reference_metrics.append(
            (
                metric_name,
                normalized_reference_value,
                current_reference_metric_values[metric_name],
            )
        )

    reference_baseline_rows = [
        (
            metric_name,
            int(reference_value),
            int(current_value),
            int(current_value) - int(reference_value),
            int(current_value) == int(reference_value),
        )
        for metric_name, reference_value, current_value in reference_metrics
    ]

    REFERENCE_BASELINE_DRIFT_COUNT = sum(
        1
        for _, _, _, _, exact_match in reference_baseline_rows
        if not exact_match
    )

    REFERENCE_BASELINE_STATUS = (
        "PASS_EXACT_REFERENCE"
        if REFERENCE_BASELINE_DRIFT_COUNT == 0
        else "PASS_WITH_LIVE_DATA_DRIFT"
    )

    if reference_baseline_rows:
        reference_baseline_schema = T.StructType([
            T.StructField("metric_name", T.StringType(), False),
            T.StructField("reference_value", T.LongType(), False),
            T.StructField("current_value", T.LongType(), False),
            T.StructField("delta", T.LongType(), False),
            T.StructField("exact_reference_match", T.BooleanType(), False),
        ])

        reference_baseline_df = spark.createDataFrame(
            reference_baseline_rows,
            schema=reference_baseline_schema,
        )

        display(reference_baseline_df.orderBy("metric_name"))
    else:
        REFERENCE_BASELINE_STATUS = "CONFIGURED_EMPTY"

if prewrite_failures:
    raise RuntimeError(
        "IP-006 v2.3 pre-write structural gate failed: "
        + " | ".join(prewrite_failures)
    )

print("IP-006 v2.3 applicability and visibility structural gate: PASS")
print("Reference baseline status:", REFERENCE_BASELINE_STATUS)
print("Reference metrics with live-data drift:", REFERENCE_BASELINE_DRIFT_COUNT)
print("Applicable relationships:", applicable_relationship_count)
print("Review-required relationships:", review_relationship_count)
print("Active unresolved-IP assets:", active_unresolved_asset_count)


# ---------------------------------------------------------------------------
# 8. Build one wide control-assurance feature row per asset
# ---------------------------------------------------------------------------

assurance_aggregate_expressions = [
    F.sum(F.when(F.col("applicability_state").startswith("APPLICABLE"), 1).otherwise(0)).alias("applicable_control_count"),
    F.sum(F.when(F.col("applicability_state").contains("REVIEW_REQUIRED"), 1).otherwise(0)).alias("applicability_review_count"),
    F.sum(F.when(F.col("assurance_state") == "HEALTHY", 1).otherwise(0)).alias("healthy_control_count"),
    F.sum(F.when(F.col("assurance_state") == "MISSING", 1).otherwise(0)).alias("missing_control_count"),
    F.sum(F.when(F.col("assurance_state") == "PRESENT_NOT_REPORTING", 1).otherwise(0)).alias("not_reporting_control_count"),
    F.sum(F.when(F.col("assurance_state") == "REPORTING_STALE", 1).otherwise(0)).alias("stale_reporting_control_count"),
    F.sum(F.when(F.col("assurance_state") == "MISCONFIGURED", 1).otherwise(0)).alias("misconfigured_control_count"),
    F.sum(F.when(F.col("assurance_state") == "NONCOMPLIANT", 1).otherwise(0)).alias("noncompliant_control_count"),
    F.sum(F.when(F.col("assurance_state") == "PRESENT_REPORTING_CONFIGURATION_UNKNOWN", 1).otherwise(0)).alias("configuration_unknown_count"),
    F.sum(F.when(F.col("assurance_state") == "SOURCE_UNAVAILABLE", 1).otherwise(0)).alias("source_unavailable_count"),
    F.sum(F.when(F.col("assurance_state") == "IDENTITY_RECONCILIATION_REQUIRED", 1).otherwise(0)).alias("identity_reconciliation_count"),
    F.sum(F.when(F.col("assurance_state").startswith("EVIDENCE_"), 1).otherwise(0)).alias("evidence_only_relationship_count"),
    F.sum(F.when(F.col("adverse_state_flag"), 1).otherwise(0)).alias("adverse_relationship_count")
]

for tool_name, prefix in [
    ("MDE", "mde"),
    ("INTUNE", "intune"),
    ("QUALYS", "qualys"),
    ("UMBRELLA", "umbrella"),
    ("TENABLE", "tenable")
]:
    for field_name in [
        "source_status", "applicability_state", "presence_state",
        "reporting_state", "configuration_state", "assurance_state",
        "review_priority", "routing_scope", "configuration_evidence_value",
        "evidence_timestamp", "evidence_age_days", "evidence_limitation"
    ]:
        assurance_aggregate_expressions.append(
            F.max(
                F.when(F.col("tool_name") == tool_name, F.col(field_name))
            ).alias(f"{prefix}_{field_name}")
        )

asset_assurance_aggregate_df = (
    evaluation_df
    .groupBy("asset_key")
    .agg(*assurance_aggregate_expressions)
)

OUTPUT_BASE_COLUMNS = [
    "post_name", "post_slug", "rcaip_run_id", "asset_key", "host_key",
    "canonical_name", "canonical_serial", "canonical_ip",
    "device_family", "device_scope", "device_family_confidence",
    "network_scope", "operational_boundary", "operational_boundary_basis",
    "operational_boundary_confidence", "management_model", "management_owner",
    "identity_basis", "identity_strength", "source_mask_raw",
    "source_mask_active", "source_mask_stale", "current_observation_status",
    "visibility_evidence_count", "visibility_evidence_mask", "visibility_class",
    "post_visibility_state", "attribution_confidence",
    "attribution_conflict_flag", "conflict_count",
    "printer_signature", "infrastructure_signature", "acr_band",
    "qualys_risk_band", "mde_exposure_level",
    "active_unresolved_ip_candidate", "active_unresolved_ip_observation_class"
]

control_feature_df = (
    asset_base_df
    .select(*OUTPUT_BASE_COLUMNS)
    .join(asset_assurance_aggregate_df, on="asset_key", how="inner")
    .withColumn(
        "overall_assurance_state",
        F.when(F.col("noncompliant_control_count") > 0, "NONCOMPLIANT")
        .when(F.col("misconfigured_control_count") > 0, "MISCONFIGURED")
        .when(F.col("missing_control_count") > 0, "MISSING")
        .when(F.col("not_reporting_control_count") > 0, "PRESENT_NOT_REPORTING")
        .when(F.col("stale_reporting_control_count") > 0, "REPORTING_STALE")
        .when(F.col("identity_reconciliation_count") > 0, "IDENTITY_RECONCILIATION_REQUIRED")
        .when(F.col("applicability_review_count") > 0, "APPLICABILITY_REVIEW_REQUIRED")
        .when(F.col("configuration_unknown_count") > 0, "PRESENT_REPORTING_CONFIGURATION_UNKNOWN")
        .when(F.col("source_unavailable_count") > 0, "SOURCE_UNAVAILABLE")
        .when(
            (F.col("applicable_control_count") > 0)
            & (F.col("healthy_control_count") == F.col("applicable_control_count")),
            "HEALTHY"
        )
        .otherwise("NO_APPLICABLE_CONTROL_CONFIRMED")
    )
    .withColumn(
        "overall_review_priority",
        F.when(F.col("overall_assurance_state").isin("NONCOMPLIANT", "MISCONFIGURED", "MISSING"), "HIGH")
        .when(F.col("overall_assurance_state").isin(
            "PRESENT_NOT_REPORTING", "REPORTING_STALE",
            "IDENTITY_RECONCILIATION_REQUIRED", "APPLICABILITY_REVIEW_REQUIRED",
            "PRESENT_REPORTING_CONFIGURATION_UNKNOWN", "SOURCE_UNAVAILABLE"
        ), "MEDIUM")
        .otherwise("NONE")
    )
    .withColumn(
        "primary_routing_scope",
        F.when(F.col("overall_review_priority") == "HIGH", "JOINT_POST_ENTERPRISE_VALIDATION")
        .when(F.col("overall_assurance_state") == "SOURCE_UNAVAILABLE", "ENTERPRISE_SERVICE_OWNER")
        .when(F.col("overall_review_priority") == "MEDIUM", "RCSO_ANALYSIS")
        .otherwise("DEFER_LOW_SIGNAL")
    )
    .withColumn("control_assurance_contract_version", F.lit(CONTROL_ASSURANCE_CONTRACT_VERSION))
    .withColumn("control_assurance_build_version", F.lit(CONTROL_ASSURANCE_BUILD_VERSION))
    .withColumn("control_assurance_build_mode", F.lit(CONTROL_ASSURANCE_BUILD_MODE))
    .withColumn("generated_at_utc", F.current_timestamp())
    .persist(StorageLevel.MEMORY_AND_DISK)
)

control_feature_count = control_feature_df.count()
if control_feature_count != feature_input_count:
    raise RuntimeError(
        "Wide control-assurance feature row-count reconciliation failed. "
        f"Expected={feature_input_count}; actual={control_feature_count}"
    )


# ---------------------------------------------------------------------------
# 9. Build deterministic adverse-state review sample
# ---------------------------------------------------------------------------

sample_priority_rank = (
    F.when(F.col("review_priority") == "HIGH", 1)
    .when(F.col("review_priority") == "MEDIUM", 2)
    .when(F.col("review_priority") == "LOW", 3)
    .otherwise(4)
)

sample_identity_rank = (
    F.when(F.col("identity_strength") == "STRONG", 1)
    .when(F.col("identity_strength") == "MODERATE", 2)
    .when(F.col("identity_strength") == "WEAK", 3)
    .otherwise(4)
)

sample_window = (
    Window
    .partitionBy("tool_name", "assurance_state")
    .orderBy(sample_priority_rank, sample_identity_rank, "asset_key")
)

review_sample_df = (
    evaluation_df
    .filter(F.col("adverse_state_flag"))
    .withColumn("sample_rank", F.row_number().over(sample_window))
    .filter(F.col("sample_rank") <= F.lit(REVIEW_SAMPLE_SIZE_PER_STATE))
    .select(
        "post_name", "post_slug", "rcaip_run_id", "asset_key", "host_key",
        "canonical_name", "canonical_ip", "device_family", "device_scope",
        "network_scope", "operational_boundary", "identity_basis",
        "identity_strength", "visibility_class", "tool_name", "control_role",
        "source_status", "applicability_state", "presence_state",
        "reporting_state", "configuration_state", "assurance_state",
        "configuration_evidence_value", "evidence_timestamp",
        "evidence_age_days", "evidence_limitation", "review_priority",
        "routing_scope", "validation_question", "sample_rank",
        "active_unresolved_ip_candidate",
        "active_unresolved_ip_observation_class",
        "control_assurance_contract_version", "control_assurance_build_version",
        "control_assurance_build_mode", "generated_at_utc"
    )
)


# ---------------------------------------------------------------------------
# 10. Protected overwrite of the three Post-scoped shadow products
# ---------------------------------------------------------------------------


def validate_existing_output_contract(table_name):
    if not spark.catalog.tableExists(table_name):
        return
    existing_columns = set(spark.table(table_name).columns)
    if existing_columns and not {"post_slug", "rcaip_run_id", "asset_key"}.issubset(existing_columns):
        raise RuntimeError(
            "Existing output has an unknown contract; refusing overwrite: "
            + table_name
        )


for table_name in [EVALUATION_OUTPUT_TABLE, FEATURE_OUTPUT_TABLE, SAMPLE_OUTPUT_TABLE]:
    validate_existing_output_contract(table_name)

(
    evaluation_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(EVALUATION_OUTPUT_TABLE)
)

(
    control_feature_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FEATURE_OUTPUT_TABLE)
)

(
    review_sample_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SAMPLE_OUTPUT_TABLE)
)

print("IP-006 v2.3 durable shadow writes: PASS")


# ---------------------------------------------------------------------------
# 11. Post-write certification and review displays
# ---------------------------------------------------------------------------

written_evaluation_df = spark.table(EVALUATION_OUTPUT_TABLE)
written_feature_df = spark.table(FEATURE_OUTPUT_TABLE)
written_sample_df = spark.table(SAMPLE_OUTPUT_TABLE)

written_evaluation_count = written_evaluation_df.count()
written_feature_count = written_feature_df.count()
written_sample_count = written_sample_df.count()

per_tool_counts = {
    row["tool_name"]: int(row["asset_count"] or 0)
    for row in (
        written_evaluation_df
        .groupBy("tool_name")
        .agg(F.count(F.lit(1)).alias("asset_count"))
        .collect()
    )
}

sample_max_rank = (
    written_sample_df
    .agg(F.max("sample_rank").alias("max_sample_rank"))
    .first()["max_sample_rank"]
)
sample_max_rank = int(sample_max_rank or 0)

postwrite_checks = [
    ("Evaluation rows", str(expected_evaluation_rows), str(written_evaluation_count), written_evaluation_count == expected_evaluation_rows),
    ("Feature rows", str(feature_input_count), str(written_feature_count), written_feature_count == feature_input_count),
    ("MDE rows", str(feature_input_count), str(per_tool_counts.get("MDE", 0)), per_tool_counts.get("MDE", 0) == feature_input_count),
    ("INTUNE rows", str(feature_input_count), str(per_tool_counts.get("INTUNE", 0)), per_tool_counts.get("INTUNE", 0) == feature_input_count),
    ("QUALYS rows", str(feature_input_count), str(per_tool_counts.get("QUALYS", 0)), per_tool_counts.get("QUALYS", 0) == feature_input_count),
    ("UMBRELLA rows", str(feature_input_count), str(per_tool_counts.get("UMBRELLA", 0)), per_tool_counts.get("UMBRELLA", 0) == feature_input_count),
    ("TENABLE rows", str(feature_input_count), str(per_tool_counts.get("TENABLE", 0)), per_tool_counts.get("TENABLE", 0) == feature_input_count),
    ("Review sample max per state", f"<= {REVIEW_SAMPLE_SIZE_PER_STATE}", str(sample_max_rank), sample_max_rank <= REVIEW_SAMPLE_SIZE_PER_STATE),
    ("Active rules", "0", str(active_rule_count), active_rule_count == 0)
]

check_schema = T.StructType([
    T.StructField("validation_check", T.StringType(), False),
    T.StructField("expected_value", T.StringType(), False),
    T.StructField("actual_value", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False)
])

postwrite_checks_df = spark.createDataFrame(postwrite_checks, schema=check_schema)
display(postwrite_checks_df.orderBy("validation_check"))

failed_checks = [
    check_name
    for check_name, expected, actual, passed in postwrite_checks
    if not passed
]

if failed_checks:
    raise RuntimeError(
        "IP-006 v2.3 post-write certification failed: "
        + " | ".join(failed_checks)
    )

control_summary_df = (
    written_evaluation_df
    .groupBy("tool_name", "applicability_state", "assurance_state")
    .agg(
        F.count(F.lit(1)).alias("asset_count"),
        F.sum(F.when(F.col("review_priority") == "HIGH", 1).otherwise(0)).alias("high_priority_count"),
        F.sum(F.when(F.col("review_priority") == "MEDIUM", 1).otherwise(0)).alias("medium_priority_count")
    )
    .orderBy("tool_name", F.desc("asset_count"), "assurance_state")
)

overall_summary_df = (
    written_feature_df
    .groupBy("overall_assurance_state", "overall_review_priority", "primary_routing_scope")
    .agg(F.count(F.lit(1)).alias("asset_count"))
    .orderBy(F.desc("asset_count"), "overall_assurance_state")
)

unresolved_ip_summary_df = (
    written_feature_df
    .groupBy("active_unresolved_ip_observation_class", "network_scope", "source_mask_active")
    .agg(F.count(F.lit(1)).alias("asset_count"))
    .orderBy(F.desc("asset_count"), "active_unresolved_ip_observation_class")
)

display(control_summary_df)
display(overall_summary_df)
display(unresolved_ip_summary_df)
display(
    written_sample_df
    .orderBy("tool_name", "assurance_state", "sample_rank", "asset_key")
)

# Release compact cached intermediates only after every write and display action.
evaluation_df.unpersist()
control_feature_df.unpersist()
asset_base_df.unpersist()

print("=" * 100)
print("RCAIP IP-006 v2.3 CONTROL ASSURANCE FEATURE CERTIFICATION")
print("=" * 100)
print("Status:", "PASS" if REFERENCE_BASELINE_STATUS != "PASS_WITH_LIVE_DATA_DRIFT" else "PASS_WITH_LIVE_DATA_DRIFT")
print("Contract version:", CONTROL_ASSURANCE_CONTRACT_VERSION)
print("Build version:", CONTROL_ASSURANCE_BUILD_VERSION)
print("Mode:", CONTROL_ASSURANCE_BUILD_MODE)
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Feature Mart input assets:", feature_input_count)
print("Five-tool evaluation rows:", written_evaluation_count)
print("Wide control-assurance feature rows:", written_feature_count)
print("Review sample rows:", written_sample_count)
print("Applicable control relationships:", applicable_relationship_count)
print("Applicability review-required relationships:", review_relationship_count)
print("Active unresolved-IP assets:", active_unresolved_asset_count)
print("Reference baseline status:", REFERENCE_BASELINE_STATUS)
print("Reference metrics with live-data drift:", REFERENCE_BASELINE_DRIFT_COUNT)
print("Reporting thresholds - Intune/MDE/Qualys:", INTUNE_CURRENT_DAYS, MDE_CURRENT_DAYS, QUALYS_CURRENT_DAYS)
print("Reporting threshold - Tenable evidence:", TENABLE_CURRENT_DAYS)
print("Raw enterprise source scans:", 0)
print("Delta tables written:", 3)
print("Existing rules changed:", 0)
print("Rules activated:", active_rule_count)
print("Findings created:", 0)
print("Tickets created:", 0)
print("Remediation performed:", 0)
print("IP-005 modified:", "NO")
print("Report 001 modified:", "NO")
print("Next gate: review adverse-state distributions and representative samples before adding or activating any v2.3 control rule.")
print("=" * 100)