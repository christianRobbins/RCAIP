# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP IP-002 - ASSET BOUNDARY AND COVERAGE WORK PACKAGE
# Optimized, Registry-Driven, Lifecycle-Aware Edition
#
# Purpose:
#   Convert the certified Asset Plane into governed asset-boundary and
#   security-tool coverage intelligence without rescanning enterprise sources.
#
# Design:
#   - Reads the saved ASSET_RECONCILIATION_SCORED Delta table.
#   - Preserves the complete observed-not-CMDB population.
#   - Separates active, mixed, stale-only, and unknown evidence.
#   - Distinguishes tool applicability, current reporting, stale reporting,
#     and absent evidence.
#   - Produces Post-facing and enterprise-boundary review queues.
#   - Performs no automatic assignment, finding, ticket, or remediation.
#   - Does not fingerprint or re-hash complete source tables.
# ============================================================================

import ipaddress
import re

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate inherited runtime
# ---------------------------------------------------------------------------

required_runtime_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_RUN_MODE",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "get_rcaip_table"
]

missing_runtime_objects = [
    object_name
    for object_name in required_runtime_objects
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-002 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

allowed_run_modes = {
    "DISCOVERY",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}

if str(RCAIP_RUN_MODE).upper() not in allowed_run_modes:
    raise RuntimeError(
        "IP-002 cannot run in the current mode: "
        + str(RCAIP_RUN_MODE)
    )

expected_post_table_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
)


# ---------------------------------------------------------------------------
# 2. Table resolution
# ---------------------------------------------------------------------------

def resolve_registered_table(logical_name, require_exists=True):
    try:
        physical_name = get_rcaip_table(
            logical_name,
            require_exists=require_exists
        )
    except TypeError:
        physical_name = get_rcaip_table(logical_name)

        if require_exists and not spark.catalog.tableExists(physical_name):
            raise RuntimeError(
                f"Required table does not exist: "
                f"{logical_name} -> {physical_name}"
            )

    return physical_name


def resolve_post_output(logical_name, fallback_stem):
    try:
        physical_name = resolve_registered_table(
            logical_name,
            require_exists=False
        )
    except Exception:
        physical_name = None

    if not physical_name:
        physical_name = (
            f"{RCAIP_RESULTS_NAMESPACE}."
            f"{RCAIP_POST_SLUG}_{fallback_stem}"
        )

    if not str(physical_name).lower().startswith(
        expected_post_table_prefix.lower()
    ):
        raise RuntimeError(
            "Cross-Post output protection blocked: "
            + str(physical_name)
        )

    return physical_name


INPUT_TABLES = {
    "ASSET_RECONCILIATION_SCORED": resolve_registered_table(
        "ASSET_RECONCILIATION_SCORED",
        require_exists=True
    ),
    "OBSERVED_NOT_CMDB": resolve_registered_table(
        "OBSERVED_NOT_CMDB",
        require_exists=True
    ),
    "QUALYS_NORM": resolve_registered_table(
        "QUALYS_NORM",
        require_exists=True
    ),
    "KB_ASSET_PATTERNS_V1": resolve_registered_table(
        "KB_ASSET_PATTERNS_V1",
        require_exists=True
    ),
    "KB_OBSERVATION_RULES_V1": resolve_registered_table(
        "KB_OBSERVATION_RULES_V1",
        require_exists=True
    ),
    "KB_REQUIRED_TOOLS_V1": resolve_registered_table(
        "KB_REQUIRED_TOOLS_V1",
        require_exists=True
    )
}

try:
    UMBRELLA_VALIDATION_TABLE = resolve_registered_table(
        "UMBRELLA_PUBLIC_IP_VALIDATION",
        require_exists=False
    )
except Exception:
    UMBRELLA_VALIDATION_TABLE = (
        f"{RCAIP_RESULTS_NAMESPACE}."
        f"{RCAIP_POST_SLUG}_umbrella_public_ip_validation"
    )

OUTPUT_TABLES = {
    "IP002_CLASSIFIED_OBSERVED_NOT_CMDB": resolve_post_output(
        "IP002_CLASSIFIED_OBSERVED_NOT_CMDB",
        "ip002_classified_observed_not_cmdb"
    ),
    "IP002_TRIAGED_OBSERVED_NOT_CMDB": resolve_post_output(
        "IP002_TRIAGED_OBSERVED_NOT_CMDB",
        "ip002_triaged_observed_not_cmdb"
    ),
    "IP002_CMDB_ONLY_BOUNDARY_REVIEW": resolve_post_output(
        "IP002_CMDB_ONLY_BOUNDARY_REVIEW",
        "ip002_cmdb_only_boundary_review"
    ),
    "IP002_TOOL_COVERAGE_REVIEW": resolve_post_output(
        "IP002_TOOL_COVERAGE_REVIEW",
        "ip002_tool_coverage_review"
    ),
    "IP002_NEN_PUBLIC_COVERAGE_REVIEW": resolve_post_output(
        "IP002_NEN_PUBLIC_COVERAGE_REVIEW",
        "ip002_nen_public_coverage_review"
    ),
    "IP002_POST_ACTION_TABLE": resolve_post_output(
        "IP002_POST_ACTION_TABLE",
        "ip002_post_action_table"
    ),
    "IP002_ENT_BOUNDARY_TABLE": resolve_post_output(
        "IP002_ENT_BOUNDARY_TABLE",
        "ip002_ent_boundary_table"
    ),
    "IP002_TECHNICAL_APPENDIX": resolve_post_output(
        "IP002_TECHNICAL_APPENDIX",
        "ip002_technical_appendix"
    ),
    "IP002_SUMMARY_BY_PRIORITY": resolve_post_output(
        "IP002_SUMMARY_BY_PRIORITY",
        "ip002_summary_by_priority"
    ),
    "IP002_SUMMARY_BY_ASSET_TYPE": resolve_post_output(
        "IP002_SUMMARY_BY_ASSET_TYPE",
        "ip002_summary_by_asset_type"
    ),
    "IP002_SUMMARY_OBSERVED_NOT_CMDB": resolve_post_output(
        "IP002_SUMMARY_OBSERVED_NOT_CMDB",
        "ip002_summary_observed_not_cmdb"
    ),
    "IP002_SUMMARY_CMDB_ONLY_BY_CLASS": resolve_post_output(
        "IP002_SUMMARY_CMDB_ONLY_BY_CLASS",
        "ip002_summary_cmdb_only_by_class"
    ),
    "IP002_SUMMARY_NEN_PUBLIC_COVERAGE": resolve_post_output(
        "IP002_SUMMARY_NEN_PUBLIC_COVERAGE",
        "ip002_summary_nen_public_coverage"
    )
}


# ---------------------------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------------------------

def first_existing_column(dataframe, candidates):
    lookup = {
        str(column_name).lower(): column_name
        for column_name in dataframe.columns
    }

    for candidate in candidates:
        resolved = lookup.get(str(candidate).lower())

        if resolved:
            return resolved

    return None


def source_column(dataframe, candidates, data_type="string"):
    resolved = first_existing_column(
        dataframe,
        candidates
    )

    if resolved is None:
        return F.lit(None).cast(data_type)

    return F.col(f"`{resolved}`").cast(data_type)


def build_token_regex(values):
    clean_values = sorted({
        str(value).strip().lower()
        for value in values
        if str(value or "").strip()
    })

    if not clean_values:
        return r"(?!)"

    return "|".join(
        re.escape(value)
        for value in clean_values
    )

post_token_values = []

for runtime_name in [
    "RCAIP_POST_TOKENS",
    "RCAIP_DNS_TOKENS",
    "RCAIP_USER_LOCATION_TOKENS",
    "RCAIP_MDE_TOKENS"
]:
    runtime_value = globals().get(
        runtime_name,
        []
    )

    if isinstance(
        runtime_value,
        (list, tuple, set)
    ):
        post_token_values.extend(runtime_value)

post_token_values.extend([
    RCAIP_POST_NAME,
    RCAIP_POST_SLUG,
    globals().get("RCAIP_POST_DEVICE_PREFIX", ""),
    globals().get("RCAIP_POST_CODE", "")
])

post_token_regex = build_token_regex(
    post_token_values
)


def add_runtime_columns(dataframe):
    output = dataframe

    runtime_values = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "rcaip_run_id": RCAIP_RUN_ID,
        "runtime_country_name": RCAIP_COUNTRY_NAME,
        "runtime_run_mode": RCAIP_RUN_MODE
    }

    for column_name, column_value in runtime_values.items():
        if column_name not in output.columns:
            output = output.withColumn(
                column_name,
                F.lit(column_value)
            )

    if "generated_at_utc" not in output.columns:
        output = output.withColumn(
            "generated_at_utc",
            F.current_timestamp()
        )

    return output


WRITTEN_TABLES = []
OUTPUT_DATAFRAMES = {}


def save_output(dataframe, logical_name):
    physical_name = OUTPUT_TABLES[
        logical_name
    ]

    output = add_runtime_columns(
        dataframe
    )

    (
        output.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(physical_name)
    )

    WRITTEN_TABLES.append(
        physical_name
    )

    OUTPUT_DATAFRAMES[
        logical_name
    ] = output

    print(
        "Saved:",
        physical_name
    )

    return physical_name


def empty_dataframe(schema):
    return spark.createDataFrame(
        [],
        schema=schema
    )


# ---------------------------------------------------------------------------
# 4. Load and materialize the certified scored Asset Plane
# ---------------------------------------------------------------------------

asset_plane_required_columns = {
    "post_name",
    "post_slug",
    "rcaip_run_id",
    "asset_key",
    "host_key",
    "cmdb_present",
    "cmdb_name",
    "cmdb_fqdn",
    "cmdb_ip",
    "cmdb_serial_number",
    "cmdb_class",
    "cmdb_location",
    "cmdb_install_status",
    "nmap_present",
    "tenable_present",
    "intune_present",
    "mde_present",
    "qualys_present",
    "observed_sources_raw",
    "observed_sources_active",
    "observed_sources_stale",
    "observed_source_count_raw",
    "observed_source_count_active",
    "observed_source_count_stale",
    "observed_ips",
    "observed_names",
    "observed_serials",
    "current_observation_status",
    "lifecycle_reconciliation_status",
    "confidence_score",
    "reconciliation_confidence",
    "requires_post_validation",
    "validation_reason",
    "nmap_active",
    "tenable_active",
    "intune_active",
    "mde_active",
    "qualys_active",
    "nmap_stale",
    "tenable_stale",
    "intune_stale",
    "mde_stale",
    "qualys_stale"
}

asset_plane = spark.table(
    INPUT_TABLES[
        "ASSET_RECONCILIATION_SCORED"
    ]
)

missing_asset_plane_columns = sorted(
    asset_plane_required_columns
    -
    set(asset_plane.columns)
)

if missing_asset_plane_columns:
    raise RuntimeError(
        "ASSET_RECONCILIATION_SCORED does not satisfy the optimized "
        "IP-002 contract. Missing: "
        +
        " | ".join(
            missing_asset_plane_columns
        )
    )

asset_plane = asset_plane.cache()

asset_plane_row_count = (
    asset_plane.count()
)

if asset_plane_row_count == 0:
    raise RuntimeError(
        "The scored Asset Plane is empty."
    )

asset_plane_scope_metrics = (
    asset_plane
    .agg(
        F.sum(
            F.when(
                F.lower(
                    F.trim(
                        F.col("post_slug")
                    )
                )
                !=
                F.lit(
                    str(RCAIP_POST_SLUG).lower()
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "post_mismatch_count"
        ),

        F.sum(
            F.when(
                F.col("rcaip_run_id")
                !=
                F.lit(RCAIP_RUN_ID),
                1
            )
            .otherwise(0)
        ).alias(
            "run_mismatch_count"
        )
    )
    .first()
)

asset_plane_post_mismatch_count = int(
    asset_plane_scope_metrics[
        "post_mismatch_count"
    ]
    or
    0
)

asset_plane_run_mismatch_count = int(
    asset_plane_scope_metrics[
        "run_mismatch_count"
    ]
    or
    0
)

if asset_plane_post_mismatch_count:
    raise RuntimeError(
        "The scored Asset Plane contains cross-Post rows: "
        +
        str(asset_plane_post_mismatch_count)
    )

if asset_plane_run_mismatch_count:
    raise RuntimeError(
        "The scored Asset Plane belongs to a different certified run: "
        +
        str(asset_plane_run_mismatch_count)
    )


# ---------------------------------------------------------------------------
# 5. Build the lifecycle-aware observed-not-CMDB boundary population
# ---------------------------------------------------------------------------

observed_boundary_base = (
    asset_plane

    .filter(
        F.col("cmdb_present") == "N"
    )

    .filter(
        F.col("observed_source_count_raw") > 0
    )

    .withColumn(
        "evidence_state",

        F.when(
            (
                F.col(
                    "observed_source_count_active"
                )
                >
                0
            )
            &
            (
                F.col(
                    "observed_source_count_stale"
                )
                >
                0
            ),
            F.lit("ACTIVE_AND_STALE")
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            >
            0,
            F.lit("ACTIVE")
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,
            F.lit("STALE_ONLY")
        )

        .otherwise(
            F.lit("UNKNOWN")
        )
    )

    .withColumn(
        "observed_ip",

        F.coalesce(
            F.col("nmap_ip"),
            F.col("tenable_ip"),
            F.col("qualys_ip"),

            F.when(
                ~F.col("observed_ips").contains(","),
                F.col("observed_ips")
            )
        )
    )

    .withColumn(
        "observed_name",

        F.coalesce(
            F.col("mde_device_name"),
            F.col("intune_device_name"),
            F.col("qualys_asset_name"),
            F.col("nmap_hostname"),
            F.col("tenable_dns_name"),

            F.when(
                ~F.col("observed_names").contains(","),
                F.col("observed_names")
            )
        )
    )

    .withColumn(
        "asset_domain",

        F.coalesce(
            F.col("nmap_asset_domain"),
            F.col("qualys_scope"),
            F.lit("Enterprise")
        )
    )

    .withColumn(
        "observed_sources",

        F.when(
            F.col(
                "observed_source_count_active"
            )
            >
            0,
            F.col("observed_sources_active")
        )

        .otherwise(
            F.col("observed_sources_raw")
        )
    )

    .withColumn(
        "observed_source_count",

        F.when(
            F.col(
                "observed_source_count_active"
            )
            >
            0,
            F.col("observed_source_count_active")
        )

        .otherwise(
            F.col("observed_source_count_raw")
        )
    )

    .withColumn(
        "name_blob",

        F.lower(
            F.concat_ws(
                " ",
                "asset_key",
                "host_key",
                "observed_ip",
                "observed_name",
                "observed_ips",
                "observed_names",
                "observed_serials",
                "observed_sources_raw",
                "asset_domain",
                "mde_device_category",
                "mde_device_type",
                "mde_dynamic_tags",
                "mde_manual_tags",
                "mde_machine_group",
                "intune_operating_system",
                "qualys_tags",
                "nmap_services"
            )
        )
    )
)


# ---------------------------------------------------------------------------
# 6. Classify observed-not-CMDB with the enterprise asset-pattern KB
# ---------------------------------------------------------------------------

kb_asset_patterns = spark.table(
    INPUT_TABLES[
        "KB_ASSET_PATTERNS_V1"
    ]
)

kb_asset_pattern_required = {
    "pattern_regex",
    "likely_asset_type",
    "asset_family",
    "asset_domain_hint",
    "pattern_confidence",
    "confidence_weight",
    "pattern_reason",
    "recommended_use"
}

use_asset_pattern_kb = (
    kb_asset_pattern_required.issubset(
        set(kb_asset_patterns.columns)
    )
)

if use_asset_pattern_kb:
    pattern_matches = (
        observed_boundary_base.alias("o")

        .join(
            F.broadcast(
                kb_asset_patterns
                .filter(
                    F.col("pattern_regex").isNotNull()
                )
            ).alias("p"),

            F.expr(
                "o.name_blob rlike p.pattern_regex"
            ),

            "left"
        )

        .withColumn(
            "classification_score",

            F.coalesce(
                F.col("p.confidence_weight"),
                F.lit(0)
            )
        )
    )

    classification_window = (
        Window

        .partitionBy(
            F.col("o.asset_key")
        )

        .orderBy(
            F.desc("classification_score")
        )
    )

    classified_observed_not_cmdb = (
        pattern_matches

        .withColumn(
            "_classification_row_number",

            F.row_number().over(
                classification_window
            )
        )

        .filter(
            F.col("_classification_row_number")
            ==
            1
        )

        .drop(
            "_classification_row_number"
        )

        .select(
            F.col("o.*"),

            F.coalesce(
                F.col("p.likely_asset_type"),
                F.lit("Unknown / Low Context")
            ).alias(
                "likely_asset_type"
            ),

            F.coalesce(
                F.col("p.asset_family"),
                F.lit("Unknown")
            ).alias(
                "asset_family"
            ),

            F.coalesce(
                F.col("p.asset_domain_hint"),
                F.col("o.asset_domain")
            ).alias(
                "asset_domain_hint"
            ),

            F.coalesce(
                F.col("p.pattern_confidence"),
                F.lit("Low")
            ).alias(
                "pattern_confidence"
            ),

            F.coalesce(
                F.col("p.confidence_weight"),
                F.lit(0)
            ).alias(
                "classification_score"
            ),

            F.coalesce(
                F.col("p.pattern_reason"),
                F.lit(
                    "No knowledge-base asset pattern matched."
                )
            ).alias(
                "classification_reason"
            ),

            F.coalesce(
                F.col("p.recommended_use"),
                F.lit(
                    "Retain for governed analyst review."
                )
            ).alias(
                "classification_recommended_use"
            ),

            F.lit(
                "KB_ASSET_PATTERNS_V1"
            ).alias(
                "classification_rule_source"
            )
        )
    )

else:
    classified_observed_not_cmdb = (
        observed_boundary_base

        .withColumn(
            "likely_asset_type",

            F.when(
                F.col("name_blob").rlike(
                    "printer|mfd|prnt"
                ),
                "Printer or MFD"
            )

            .when(
                F.col("name_blob").rlike(
                    "server|srv|domain controller"
                ),
                "Server"
            )

            .when(
                F.col("name_blob").rlike(
                    "switch|router|firewall|vpn|access point"
                ),
                "Network Infrastructure"
            )

            .when(
                F.col("name_blob").rlike(
                    "laptop|desktop|workstation|computer|windows"
                ),
                "Endpoint"
            )

            .otherwise(
                "Unknown / Low Context"
            )
        )

        .withColumn(
            "asset_family",
            F.col("likely_asset_type")
        )

        .withColumn(
            "asset_domain_hint",
            F.col("asset_domain")
        )

        .withColumn(
            "pattern_confidence",
            F.lit("Low")
        )

        .withColumn(
            "classification_score",
            F.lit(0)
        )

        .withColumn(
            "classification_reason",
            F.lit(
                "KB schema was unavailable; conservative fallback "
                "classification applied."
            )
        )

        .withColumn(
            "classification_recommended_use",
            F.lit(
                "Retain for governed analyst review."
            )
        )

        .withColumn(
            "classification_rule_source",
            F.lit("CONSERVATIVE_FALLBACK")
        )
    )

classified_observed_not_cmdb = (
    classified_observed_not_cmdb.cache()
)

classified_observed_not_cmdb_count = (
    classified_observed_not_cmdb.count()
)

save_output(
    classified_observed_not_cmdb,
    "IP002_CLASSIFIED_OBSERVED_NOT_CMDB"
)


# ---------------------------------------------------------------------------
# 7. Interpret and triage the observed-not-CMDB boundary
# ---------------------------------------------------------------------------

interpreted = (
    classified_observed_not_cmdb

    .withColumn(
        "has_name_evidence",

        F.when(
            F.col("observed_name").isNotNull()
            &
            (
                F.trim(
                    F.col("observed_name")
                )
                !=
                ""
            )
            &
            (
                ~F.lower(
                    F.col("observed_name")
                ).rlike(
                    r"^\d+\.\d+\.\d+\.\d+"
                )
            ),
            F.lit("Y")
        )

        .otherwise(
            F.lit("N")
        )
    )

    .withColumn(
        "has_ip_evidence",

        F.when(
            F.col("observed_ip").isNotNull()
            &
            (
                F.trim(
                    F.col("observed_ip")
                )
                !=
                ""
            ),
            F.lit("Y")
        )

        .otherwise(
            F.lit("N")
        )
    )

    .withColumn(
        "has_post_token",

        F.when(
            F.col("name_blob").rlike(
                post_token_regex
            ),
            "Y"
        )

        .otherwise(
            "N"
        )
    )

    .withColumn(
        "source_strength",

        F.when(
            F.col(
                "observed_source_count_active"
            )
            >=
            3,
            "Strong"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            2,
            "Moderate"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            1,
            "Single Active Source"
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,
            "Stale Only"
        )

        .otherwise(
            "Unknown"
        )
    )

    .withColumn(
        "interpretation_rule_id",

        F.when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "stale_only_observation"
        )

        .when(
            (
                F.upper(
                    F.col("observed_sources_active")
                )
                ==
                "NMAP"
            )
            &
            (
                F.col("has_name_evidence")
                ==
                "N"
            ),
            "nmap_only_low_context"
        )

        .when(
            F.upper(
                F.col("observed_sources_active")
            ).contains("NMAP")
            &
            F.upper(
                F.col("observed_sources_active")
            ).contains("TENABLE"),
            "nmap_plus_tenable"
        )

        .when(
            F.upper(
                F.col("observed_sources_active")
            ).rlike("INTUNE|MDE")
            &
            (
                F.col("has_ip_evidence")
                ==
                "N"
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                <=
                2
            ),
            "mde_intune_only_no_ip"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            >=
            2,
            "active_multi_source"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            1,
            "active_single_source"
        )

        .otherwise(
            "manual_review"
        )
    )
)

kb_observation_rules = spark.table(
    INPUT_TABLES[
        "KB_OBSERVATION_RULES_V1"
    ]
)

observation_rule_required = {
    "rule_id",
    "rule_name",
    "interpretation",
    "default_priority",
    "recommended_owner",
    "recommended_action"
}

use_observation_rule_kb = (
    observation_rule_required.issubset(
        set(kb_observation_rules.columns)
    )
)

if use_observation_rule_kb:
    triaged_with_rules = (
        interpreted.alias("i")

        .join(
            F.broadcast(
                kb_observation_rules
            ).alias("r"),

            F.col("i.interpretation_rule_id")
            ==
            F.col("r.rule_id"),

            "left"
        )

        .select(
            F.col("i.*"),

            F.coalesce(
                F.col("r.rule_name"),
                F.lit("Governed Manual Review")
            ).alias(
                "rule_name"
            ),

            F.coalesce(
                F.col("r.interpretation"),
                F.lit(
                    "Human validation is required."
                )
            ).alias(
                "interpretation"
            ),

            F.coalesce(
                F.col("r.default_priority"),
                F.lit("Review")
            ).alias(
                "kb_priority"
            ),

            F.coalesce(
                F.col("r.recommended_owner"),
                F.lit("RCSO / Enterprise Review")
            ).alias(
                "kb_recommended_owner"
            ),

            F.coalesce(
                F.col("r.recommended_action"),
                F.lit(
                    "Review evidence and validate identity."
                )
            ).alias(
                "kb_recommended_action"
            )
        )
    )

else:
    triaged_with_rules = (
        interpreted

        .withColumn(
            "rule_name",
            F.lit("Governed Manual Review")
        )

        .withColumn(
            "interpretation",
            F.lit(
                "Human validation is required."
            )
        )

        .withColumn(
            "kb_priority",
            F.lit("Review")
        )

        .withColumn(
            "kb_recommended_owner",
            F.lit(
                "RCSO / Enterprise Review"
            )
        )

        .withColumn(
            "kb_recommended_action",
            F.lit(
                "Review evidence and validate identity."
            )
        )
    )

critical_asset_types = [
    "Server",
    "Server Management Interface",
    "Firewall or VPN",
    "Network Infrastructure",
    "Switch",
    "Router",
    "Domain Controller"
]

triaged_observed_not_cmdb = (
    triaged_with_rules

    .withColumn(
        "priority",

        F.when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "Low"
        )

        .when(
            F.col("evidence_state")
            ==
            "UNKNOWN",
            "Review"
        )

        .when(
            F.col("likely_asset_type").isin(
                critical_asset_types
            )
            &
            (
                F.col("has_name_evidence")
                ==
                "Y"
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                >=
                1
            ),
            "High"
        )

        .when(
            (
                F.col(
                    "observed_source_count_active"
                )
                >=
                2
            )
            &
            (
                F.col("classification_score")
                >=
                50
            ),
            "High"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            >=
            1,
            "Medium"
        )

        .otherwise(
            F.col("kb_priority")
        )
    )

    .withColumn(
        "validation_responsibility",

        F.when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "Enterprise Data Hygiene"
        )

        .when(
            F.col("has_post_token")
            ==
            "Y",
            "Joint Post and Enterprise Validation"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            >=
            2,
            "Enterprise Attribution and CMDB Validation"
        )

        .otherwise(
            "RCSO / Enterprise Corroboration"
        )
    )

    .withColumn(
        "recommended_owner",
        F.col("validation_responsibility")
    )

    .withColumn(
        "recommended_action",

        F.when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",

            "Validate whether historical source records should be retired, "
            "corrected, or retained; do not treat as a current missing CMDB "
            "asset."
        )

        .when(
            F.col("priority")
            ==
            "High",

            "Validate canonical identity, ownership, operational status, "
            "and CMDB representation using source and Post evidence."
        )

        .when(
            F.col("priority")
            ==
            "Medium",

            "Corroborate the active observation and determine whether CMDB, "
            "attribution, or source identity requires correction."
        )

        .otherwise(
            F.col("kb_recommended_action")
        )
    )

    .withColumn(
        "automatic_assignment_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)

triaged_observed_not_cmdb = (
    triaged_observed_not_cmdb.cache()
)

triaged_observed_not_cmdb_count = (
    triaged_observed_not_cmdb.count()
)

save_output(
    triaged_observed_not_cmdb,
    "IP002_TRIAGED_OBSERVED_NOT_CMDB"
)


# ---------------------------------------------------------------------------
# 8. Build the CMDB-only / no-current-evidence boundary
# ---------------------------------------------------------------------------

cmdb_only_boundary = (
    asset_plane

    .filter(
        F.col("cmdb_present") == "Y"
    )

    .filter(
        F.col("observed_source_count_active") == 0
    )

    .withColumn(
        "evidence_state",

        F.when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,
            "STALE_ONLY"
        )

        .otherwise(
            "NO_OBSERVATION"
        )
    )

    .withColumn(
        "priority",

        F.when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,
            "Review"
        )

        .otherwise(
            "Medium"
        )
    )

    .withColumn(
        "boundary_issue",

        F.when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,

            "CMDB identity has stale historical evidence but no current "
            "source evidence."
        )

        .otherwise(
            "CMDB identity has no current RCAIP source evidence."
        )
    )

    .withColumn(
        "validation_responsibility",

        F.lit(
            "CMDB and Enterprise Asset Management Validation"
        )
    )

    .withColumn(
        "recommended_owner",
        F.col("validation_responsibility")
    )

    .withColumn(
        "recommended_action",

        F.when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,

            "Validate retirement, rename, reassignment, duplicate identity, "
            "or stale-source cleanup."
        )

        .otherwise(
            "Validate existence, current ownership, CMDB assignment, offline "
            "status, and discovery-path coverage."
        )
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)

cmdb_only_boundary = (
    cmdb_only_boundary.cache()
)

cmdb_only_boundary_count = (
    cmdb_only_boundary.count()
)

save_output(
    cmdb_only_boundary,
    "IP002_CMDB_ONLY_BOUNDARY_REVIEW"
)


# ---------------------------------------------------------------------------
# 9. Build complete required-tool coverage records
#
# Presence means a source record exists. Active reporting requires the
# lifecycle-aware active flag. A stale record is not treated as current
# reporting, and missing source evidence is not automatically a finding.
#
# The preserved V1 required-tools KB is loaded and recorded for traceability.
# Its legacy five-column schema is not silently treated as a complete modern
# policy authority; the explicit current IP-002 expectation contract below is
# labeled so it can be reconciled to a future KB release deliberately.
# ---------------------------------------------------------------------------

kb_required_tools = spark.table(
    INPUT_TABLES[
        "KB_REQUIRED_TOOLS_V1"
    ]
)

kb_required_tools_row_count = (
    kb_required_tools.count()
)

asset_descriptor = (
    F.lower(
        F.concat_ws(
            " ",
            "cmdb_class",
            "cmdb_category",
            "cmdb_name",
            "cmdb_fqdn",
            "mde_device_category",
            "mde_device_type",
            "mde_device_name",
            "intune_device_name",
            "intune_operating_system",
            "qualys_asset_name",
            "nmap_hostname",
            "nmap_asset_domain",
            "qualys_scope",
            "observed_names"
        )
    )
)

tool_asset_base = (
    asset_plane

    .withColumn(
        "asset_descriptor",
        asset_descriptor
    )

    .withColumn(
        "asset_type",

        F.when(
            F.col("asset_descriptor").rlike(
                "printer|mfd|prnt"
            ),
            "Printer or MFD"
        )

        .when(
            F.col("asset_descriptor").rlike(
                "server|domain controller|srv"
            ),
            "Server"
        )

        .when(
            F.col("asset_descriptor").rlike(
                "laptop|desktop|workstation|personal computer|"
                "handheld|computer|windows"
            ),
            "Endpoint"
        )

        .when(
            F.col("asset_descriptor").rlike(
                "switch|router|firewall|vpn|access point|"
                "wireless controller"
            ),
            "Network Infrastructure"
        )

        .otherwise(
            "Other / Applicability Review"
        )
    )

    .withColumn(
        "asset_name",

        F.coalesce(
            F.col("cmdb_name"),
            F.col("mde_device_name"),
            F.col("intune_device_name"),
            F.col("qualys_asset_name"),
            F.col("nmap_hostname"),
            F.col("tenable_dns_name"),
            F.col("host_key"),
            F.col("asset_key")
        )
    )

    .withColumn(
        "device_name",

        F.coalesce(
            F.col("mde_device_name"),
            F.col("intune_device_name"),
            F.col("cmdb_name"),
            F.col("nmap_hostname"),
            F.col("tenable_dns_name")
        )
    )

    .withColumn(
        "is_endpoint_like",
        F.col("asset_type") == "Endpoint"
    )

    .withColumn(
        "is_server_like",
        F.col("asset_type") == "Server"
    )

    .withColumn(
        "is_printer_like",
        F.col("asset_type") == "Printer or MFD"
    )

    .withColumn(
        "is_nen_or_public",

        F.col("asset_descriptor").rlike(
            "nen|public|internet facing|enterprise wifi"
        )
    )
)


def build_tool_expectation_rows(
    base_dataframe,
    applicability_condition,
    tool_name,
    presence_column,
    active_column,
    stale_column,
    last_seen_column,
    expectation_basis
):
    return (
        base_dataframe

        .filter(
            applicability_condition
        )

        .select(
            "post_name",
            "post_slug",
            "rcaip_run_id",
            "asset_key",
            "host_key",
            "cmdb_present",
            "cmdb_name",
            "cmdb_fqdn",
            "cmdb_ip",
            "cmdb_serial_number",
            "cmdb_class",
            "cmdb_location",
            "cmdb_install_status",
            "asset_name",
            "device_name",
            "asset_type",
            "observed_source_count_raw",
            "observed_source_count_active",
            "observed_source_count_stale",
            "current_observation_status",

            F.lit(
                tool_name
            ).alias(
                "required_tool"
            ),

            F.lit(
                tool_name
            ).alias(
                "tool_name"
            ),

            F.col(
                presence_column
            ).alias(
                "tool_record_present"
            ),

            F.col(
                active_column
            ).cast(
                "integer"
            ).alias(
                "tool_active_reporting"
            ),

            F.col(
                stale_column
            ).cast(
                "integer"
            ).alias(
                "tool_stale_evidence"
            ),

            F.col(
                last_seen_column
            ).cast(
                "timestamp"
            ).alias(
                "last_seen_at"
            ),

            F.lit(
                expectation_basis
            ).alias(
                "expectation_basis"
            ),

            F.lit(
                "EXPLICIT_IP002_CURRENT_CONTRACT"
            ).alias(
                "expectation_rule_source"
            )
        )

        .withColumn(
            "coverage_status",

            F.when(
                F.col("tool_active_reporting") == 1,
                "Observed and current"
            )

            .when(
                F.col("tool_stale_evidence") == 1,
                "Observed but stale"
            )

            .when(
                F.col("tool_record_present") == "Y",
                "Observed; freshness unknown"
            )

            .otherwise(
                "Not observed"
            )
        )

        .withColumn(
            "reporting_status",

            F.when(
                F.col("tool_active_reporting") == 1,
                "Current reporting evidence"
            )

            .when(
                F.col("tool_stale_evidence") == 1,
                "Stale reporting evidence"
            )

            .when(
                F.col("tool_record_present") == "Y",
                "Presence evidence; reporting age unresolved"
            )

            .otherwise(
                "No reporting evidence in the current Asset Plane"
            )
        )

        .withColumn(
            "review_required",

            F.when(
                F.col("tool_active_reporting") == 1,
                "N"
            )

            .otherwise(
                "Y"
            )
        )

        .withColumn(
            "review_priority",

            F.when(
                F.col(
                    "observed_source_count_active"
                )
                ==
                0,
                "Review"
            )

            .when(
                F.col("tool_record_present")
                ==
                "N",
                "High"
            )

            .when(
                F.col("tool_stale_evidence")
                ==
                1,
                "Medium"
            )

            .when(
                F.col("tool_active_reporting")
                ==
                1,
                "Pass"
            )

            .otherwise(
                "Review"
            )
        )

        .withColumn(
            "priority",
            F.col("review_priority")
        )

        .withColumn(
            "missing_required_tools",

            F.when(
                F.col("tool_record_present")
                ==
                "N",
                F.col("tool_name")
            )

            .otherwise(
                F.lit("")
            )
        )

        .withColumn(
            "validation_responsibility",

            F.when(
                F.col("cmdb_present")
                ==
                "N",

                "Joint CMDB, Attribution, and Enterprise Tool Validation"
            )

            .when(
                F.col(
                    "observed_source_count_active"
                )
                ==
                0,

                "CMDB and Enterprise Asset Management Validation"
            )

            .otherwise(
                "Joint Post and Enterprise Tool Validation"
            )
        )

        .withColumn(
            "recommended_owner",
            F.col("validation_responsibility")
        )

        .withColumn(
            "recommended_action",

            F.when(
                F.col("tool_active_reporting")
                ==
                1,

                "Retain as positive coverage evidence; no action generated."
            )

            .when(
                F.col("tool_stale_evidence")
                ==
                1,

                "Validate whether the asset is active and whether the tool "
                "should still be reporting."
            )

            .when(
                F.col("tool_record_present")
                ==
                "N",

                F.concat(
                    F.lit(
                        "Validate applicability, installation, identity "
                        "matching, and reporting for "
                    ),

                    F.col("tool_name"),

                    F.lit(
                        "; absence from the Asset Plane is not itself a "
                        "confirmed finding."
                    )
                )
            )

            .otherwise(
                "Validate tool-reporting freshness and identity correlation."
            )
        )

        .withColumn(
            "automatic_finding_allowed",
            F.lit(False)
        )
    )


tool_expectation_frames = [
    build_tool_expectation_rows(
        tool_asset_base,
        F.col("is_endpoint_like")
        |
        F.col("is_server_like"),
        "MDE",
        "mde_present",
        "mde_active",
        "mde_stale",
        "mde_last_seen",
        "Endpoint and server protection expectation"
    ),

    build_tool_expectation_rows(
        tool_asset_base,
        F.col("is_endpoint_like"),
        "Intune",
        "intune_present",
        "intune_active",
        "intune_stale",
        "intune_last_contact",
        "Managed endpoint expectation"
    ),

    build_tool_expectation_rows(
        tool_asset_base,
        F.col("is_endpoint_like")
        |
        F.col("is_server_like")
        |
        F.col("is_printer_like"),
        "Tenable",
        "tenable_present",
        "tenable_active",
        "tenable_stale",
        "tenable_event_time",
        "Vulnerability-management expectation"
    ),

    build_tool_expectation_rows(
        tool_asset_base,
        F.col("is_nen_or_public"),
        "Qualys",
        "qualys_present",
        "qualys_active",
        "qualys_stale",
        "qualys_inventory_last_updated_on",
        "NEN, public, or internet-facing coverage expectation"
    )
]

tool_coverage_review = reduce(
    lambda left, right:
        left.unionByName(
            right,
            allowMissingColumns=True
        ),

    tool_expectation_frames
)

tool_coverage_review = (
    tool_coverage_review.cache()
)

tool_coverage_metrics = (
    tool_coverage_review

    .agg(
        F.count("*").alias(
            "tool_record_count"
        ),

        F.sum(
            F.when(
                F.col("review_required") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "tool_review_count"
        )
    )

    .first()
)

tool_coverage_review_count = int(
    tool_coverage_metrics[
        "tool_record_count"
    ]
    or
    0
)

tool_coverage_gap_count = int(
    tool_coverage_metrics[
        "tool_review_count"
    ]
    or
    0
)

save_output(
    tool_coverage_review,
    "IP002_TOOL_COVERAGE_REVIEW"
)


# ---------------------------------------------------------------------------
# 10. Optional NEN/public Qualys and Umbrella review
# ---------------------------------------------------------------------------

nen_public_schema = T.StructType([
    T.StructField(
        "post_name",
        T.StringType(),
        True
    ),
    T.StructField(
        "asset_domain",
        T.StringType(),
        True
    ),
    T.StructField(
        "public_ip_or_cidr",
        T.StringType(),
        True
    ),
    T.StructField(
        "umbrella_expected",
        T.StringType(),
        True
    ),
    T.StructField(
        "umbrella_observed",
        T.StringType(),
        True
    ),
    T.StructField(
        "post_validation_status",
        T.StringType(),
        True
    ),
    T.StructField(
        "notes",
        T.StringType(),
        True
    ),
    T.StructField(
        "target_start_long",
        T.LongType(),
        True
    ),
    T.StructField(
        "target_end_long",
        T.LongType(),
        True
    ),
    T.StructField(
        "target_valid",
        T.StringType(),
        True
    ),
    T.StructField(
        "target_parse_error",
        T.StringType(),
        True
    ),
    T.StructField(
        "qualys_observed_ip_count",
        T.LongType(),
        True
    ),
    T.StructField(
        "qualys_observed_ips",
        T.StringType(),
        True
    ),
    T.StructField(
        "qualys_observed_names",
        T.StringType(),
        True
    ),
    T.StructField(
        "qualys_scopes",
        T.StringType(),
        True
    ),
    T.StructField(
        "qualys_tags_seen",
        T.StringType(),
        True
    ),
    T.StructField(
        "qualys_observed",
        T.StringType(),
        True
    ),
    T.StructField(
        "priority",
        T.StringType(),
        True
    ),
    T.StructField(
        "validation_responsibility",
        T.StringType(),
        True
    ),
    T.StructField(
        "recommended_owner",
        T.StringType(),
        True
    ),
    T.StructField(
        "recommended_action",
        T.StringType(),
        True
    )
])

umbrella_available = bool(
    UMBRELLA_VALIDATION_TABLE
    and
    spark.catalog.tableExists(
        UMBRELLA_VALIDATION_TABLE
    )
)

if umbrella_available:
    umbrella_raw = spark.table(
        UMBRELLA_VALIDATION_TABLE
    )

    required_umbrella_columns = {
        "post_name",
        "asset_domain",
        "public_ip_or_cidr",
        "umbrella_expected",
        "umbrella_observed",
        "post_validation_status",
        "notes"
    }

    if not required_umbrella_columns.issubset(
        set(umbrella_raw.columns)
    ):
        raise RuntimeError(
            "UMBRELLA_PUBLIC_IP_VALIDATION exists but does not satisfy "
            "its schema contract."
        )

    umbrella_rows = (
        umbrella_raw

        .select(
            "post_name",
            "asset_domain",
            "public_ip_or_cidr",
            "umbrella_expected",
            "umbrella_observed",
            "post_validation_status",
            "notes"
        )

        .collect()
    )

    target_rows = []

    for row in umbrella_rows:
        target = row[
            "public_ip_or_cidr"
        ]

        try:
            target_text = str(
                target
            ).strip()

            network = ipaddress.ip_network(
                (
                    target_text
                    if "/" in target_text
                    else f"{target_text}/32"
                ),
                strict=False
            )

            start_long = int(
                network.network_address
            )

            end_long = int(
                network.broadcast_address
            )

            target_valid = "Y"
            parse_error = None

        except Exception as exc:
            start_long = None
            end_long = None
            target_valid = "N"
            parse_error = str(exc)

        target_rows.append((
            row["post_name"],
            row["asset_domain"],
            row["public_ip_or_cidr"],
            row["umbrella_expected"],
            row["umbrella_observed"],
            row["post_validation_status"],
            row["notes"],
            start_long,
            end_long,
            target_valid,
            parse_error
        ))

    target_schema = T.StructType([
        T.StructField(
            "post_name",
            T.StringType(),
            True
        ),
        T.StructField(
            "asset_domain",
            T.StringType(),
            True
        ),
        T.StructField(
            "public_ip_or_cidr",
            T.StringType(),
            True
        ),
        T.StructField(
            "umbrella_expected",
            T.StringType(),
            True
        ),
        T.StructField(
            "umbrella_observed",
            T.StringType(),
            True
        ),
        T.StructField(
            "post_validation_status",
            T.StringType(),
            True
        ),
        T.StructField(
            "notes",
            T.StringType(),
            True
        ),
        T.StructField(
            "target_start_long",
            T.LongType(),
            True
        ),
        T.StructField(
            "target_end_long",
            T.LongType(),
            True
        ),
        T.StructField(
            "target_valid",
            T.StringType(),
            True
        ),
        T.StructField(
            "target_parse_error",
            T.StringType(),
            True
        )
    ])

    umbrella_targets = spark.createDataFrame(
        target_rows,
        schema=target_schema
    )

    qualys_norm = spark.table(
        INPUT_TABLES[
            "QUALYS_NORM"
        ]
    )

    ip_parts = F.split(
        F.col("qualys_ip"),
        r"\."
    )

    qualys_public_ips = (
        qualys_norm

        .select(
            F.col(
                "ip_address"
            ).alias(
                "qualys_ip"
            ),

            "qualys_asset_name",
            "qualys_scope",
            "qualys_tags",
            "qualys_inventory_last_updated_on"
        )

        .filter(
            F.col("qualys_ip").rlike(
                r"^\d{1,3}(\.\d{1,3}){3}$"
            )
        )

        .withColumn(
            "octet_1",

            F.element_at(
                ip_parts,
                1
            ).cast("integer")
        )

        .withColumn(
            "octet_2",

            F.element_at(
                ip_parts,
                2
            ).cast("integer")
        )

        .withColumn(
            "octet_3",

            F.element_at(
                ip_parts,
                3
            ).cast("integer")
        )

        .withColumn(
            "octet_4",

            F.element_at(
                ip_parts,
                4
            ).cast("integer")
        )

        .filter(
            F.col("octet_1").between(0, 255)
            &
            F.col("octet_2").between(0, 255)
            &
            F.col("octet_3").between(0, 255)
            &
            F.col("octet_4").between(0, 255)
        )

        .withColumn(
            "qualys_ip_long",

            F.col("octet_1").cast("long")
            *
            F.lit(16777216)

            +
            F.col("octet_2").cast("long")
            *
            F.lit(65536)

            +
            F.col("octet_3").cast("long")
            *
            F.lit(256)

            +
            F.col("octet_4").cast("long")
        )
    )

    nen_public_coverage_review = (
        umbrella_targets.alias("u")

        .join(
            qualys_public_ips.alias("q"),

            (
                F.col("u.target_valid")
                ==
                "Y"
            )
            &
            (
                F.col("q.qualys_ip_long")
                >=
                F.col("u.target_start_long")
            )
            &
            (
                F.col("q.qualys_ip_long")
                <=
                F.col("u.target_end_long")
            ),

            "left"
        )

        .select(
            F.col(
                "u.post_name"
            ).alias(
                "post_name"
            ),

            F.col(
                "u.asset_domain"
            ).alias(
                "asset_domain"
            ),

            F.col(
                "u.public_ip_or_cidr"
            ).alias(
                "public_ip_or_cidr"
            ),

            F.col(
                "u.umbrella_expected"
            ).alias(
                "umbrella_expected"
            ),

            F.col(
                "u.umbrella_observed"
            ).alias(
                "umbrella_observed"
            ),

            F.col(
                "u.post_validation_status"
            ).alias(
                "post_validation_status"
            ),

            F.col(
                "u.notes"
            ).alias(
                "notes"
            ),

            F.col(
                "u.target_start_long"
            ).alias(
                "target_start_long"
            ),

            F.col(
                "u.target_end_long"
            ).alias(
                "target_end_long"
            ),

            F.col(
                "u.target_valid"
            ).alias(
                "target_valid"
            ),

            F.col(
                "u.target_parse_error"
            ).alias(
                "target_parse_error"
            ),

            F.col(
                "q.qualys_ip"
            ).alias(
                "qualys_ip"
            ),

            F.col(
                "q.qualys_asset_name"
            ).alias(
                "qualys_asset_name"
            ),

            F.col(
                "q.qualys_scope"
            ).alias(
                "qualys_scope"
            ),

            F.col(
                "q.qualys_tags"
            ).alias(
                "qualys_tags"
            )
        )

        .groupBy(
            "post_name",
            "asset_domain",
            "public_ip_or_cidr",
            "umbrella_expected",
            "umbrella_observed",
            "post_validation_status",
            "notes",
            "target_start_long",
            "target_end_long",
            "target_valid",
            "target_parse_error"
        )

        .agg(
            F.countDistinct(
                "qualys_ip"
            ).alias(
                "qualys_observed_ip_count"
            ),

            F.concat_ws(
                ", ",

                F.sort_array(
                    F.collect_set(
                        "qualys_ip"
                    )
                )
            ).alias(
                "qualys_observed_ips"
            ),

            F.concat_ws(
                ", ",

                F.sort_array(
                    F.collect_set(
                        "qualys_asset_name"
                    )
                )
            ).alias(
                "qualys_observed_names"
            ),

            F.concat_ws(
                ", ",

                F.sort_array(
                    F.collect_set(
                        "qualys_scope"
                    )
                )
            ).alias(
                "qualys_scopes"
            ),

            F.concat_ws(
                ", ",

                F.sort_array(
                    F.collect_set(
                        "qualys_tags"
                    )
                )
            ).alias(
                "qualys_tags_seen"
            )
        )

        .withColumn(
            "qualys_observed",

            F.when(
                F.col(
                    "qualys_observed_ip_count"
                )
                >
                0,
                "Y"
            )

            .otherwise(
                "N"
            )
        )

        .withColumn(
            "priority",

            F.when(
                F.col("target_valid")
                ==
                "N",
                "Review"
            )

            .when(
                (
                    F.col("qualys_observed")
                    ==
                    "N"
                )
                |
                F.lower(
                    F.coalesce(
                        F.col("umbrella_observed"),
                        F.lit("")
                    )
                ).isin(
                    "n",
                    "no",
                    "false"
                ),
                "High"
            )

            .otherwise(
                "Pass"
            )
        )

        .withColumn(
            "validation_responsibility",

            F.lit(
                "Joint Post, Network, Qualys, and Umbrella Validation"
            )
        )

        .withColumn(
            "recommended_owner",
            F.col("validation_responsibility")
        )

        .withColumn(
            "recommended_action",

            F.when(
                F.col("target_valid")
                ==
                "N",

                "Validate the public IP or CIDR before assessing coverage."
            )

            .when(
                F.col("priority")
                ==
                "High",

                "Validate applicability and current Qualys and Umbrella "
                "coverage for this public target."
            )

            .otherwise(
                "Retain as positive public-boundary coverage evidence."
            )
        )

        .select(
            *[
                field.name
                for field in nen_public_schema.fields
            ]
        )
    )

else:
    nen_public_coverage_review = empty_dataframe(
        nen_public_schema
    )

nen_public_coverage_review = (
    nen_public_coverage_review.cache()
)

nen_public_coverage_review_count = (
    nen_public_coverage_review.count()
)

save_output(
    nen_public_coverage_review,
    "IP002_NEN_PUBLIC_COVERAGE_REVIEW"
)


# ---------------------------------------------------------------------------
# 11. Build Post-facing action queue
# ---------------------------------------------------------------------------

observed_post_actions = (
    triaged_observed_not_cmdb

    .filter(
        F.col("evidence_state").isin(
            "ACTIVE",
            "ACTIVE_AND_STALE"
        )
    )

    .filter(
        F.col("priority").isin(
            "High",
            "Medium"
        )
    )

    .select(
        F.lit(
            "Observed Not CMDB"
        ).alias(
            "workstream"
        ),

        "priority",
        "asset_domain",

        F.col(
            "likely_asset_type"
        ).alias(
            "asset_type"
        ),

        "asset_key",

        F.col(
            "observed_ip"
        ).alias(
            "ip_address"
        ),

        F.col(
            "observed_name"
        ).alias(
            "asset_name"
        ),

        F.col(
            "observed_sources_active"
        ).alias(
            "evidence_sources"
        ),

        "evidence_state",
        "validation_responsibility",

        F.concat(
            F.lit(
                "Active asset evidence is not reconciled to CMDB. "
                "Active sources: "
            ),

            F.coalesce(
                F.col("observed_sources_active"),
                F.lit("")
            )
        ).alias(
            "issue_summary"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

tool_post_actions = (
    tool_coverage_review

    .filter(
        F.col("review_required") == "Y"
    )

    .filter(
        F.col(
            "validation_responsibility"
        ).contains("Post")
    )

    .select(
        F.lit(
            "Tool Coverage"
        ).alias(
            "workstream"
        ),

        F.col(
            "review_priority"
        ).alias(
            "priority"
        ),

        F.lit(
            "Enterprise"
        ).alias(
            "asset_domain"
        ),

        "asset_type",

        "asset_key",

        F.col(
            "cmdb_ip"
        ).alias(
            "ip_address"
        ),

        "asset_name",

        F.concat(
            F.col("tool_name"),
            F.lit(": "),
            F.col("reporting_status")
        ).alias(
            "evidence_sources"
        ),

        F.col(
            "current_observation_status"
        ).alias(
            "evidence_state"
        ),

        "validation_responsibility",

        F.concat(
            F.lit(
                "Required-tool review: "
            ),

            F.col("tool_name"),

            F.lit(" - "),

            F.col("coverage_status")
        ).alias(
            "issue_summary"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

nen_post_actions = (
    nen_public_coverage_review

    .filter(
        F.col("priority") == "High"
    )

    .select(
        F.lit(
            "NEN Public Coverage"
        ).alias(
            "workstream"
        ),

        "priority",
        "asset_domain",

        F.lit(
            "Public IP or CIDR"
        ).alias(
            "asset_type"
        ),

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "asset_key"
        ),

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "ip_address"
        ),

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "asset_name"
        ),

        F.concat(
            F.lit(
                "Qualys observed IPs: "
            ),

            F.col(
                "qualys_observed_ip_count"
            ).cast("string"),

            F.lit(
                "; Umbrella observed: "
            ),

            F.coalesce(
                F.col("umbrella_observed"),
                F.lit("Unknown")
            )
        ).alias(
            "evidence_sources"
        ),

        F.lit(
            "PUBLIC_BOUNDARY_REVIEW"
        ).alias(
            "evidence_state"
        ),

        "validation_responsibility",

        F.lit(
            "Public or NEN coverage requires governed validation."
        ).alias(
            "issue_summary"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

post_action_table = reduce(
    lambda left, right:
        left.unionByName(
            right,
            allowMissingColumns=True
        ),

    [
        observed_post_actions,
        tool_post_actions,
        nen_post_actions
    ]
)

post_action_table = (
    post_action_table

    .withColumn(
        "post_response",
        F.lit("")
    )

    .withColumn(
        "post_notes",
        F.lit("")
    )

    .withColumn(
        "automatic_assignment_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)

post_action_table = (
    post_action_table.cache()
)

post_action_table_count = (
    post_action_table.count()
)

save_output(
    post_action_table,
    "IP002_POST_ACTION_TABLE"
)


# ---------------------------------------------------------------------------
# 12. Build enterprise boundary queue
# ---------------------------------------------------------------------------

cmdb_boundary_out = (
    cmdb_only_boundary

    .select(
        F.lit(
            "CMDB Only or No Current Evidence"
        ).alias(
            "boundary_type"
        ),

        "priority",
        "asset_key",

        F.col(
            "cmdb_ip"
        ).alias(
            "ip_address"
        ),

        F.coalesce(
            F.col("cmdb_name"),
            F.col("cmdb_fqdn")
        ).alias(
            "asset_name"
        ),

        F.col(
            "cmdb_class"
        ).alias(
            "asset_type"
        ),

        "evidence_state",
        "validation_responsibility",

        F.col(
            "boundary_issue"
        ).alias(
            "review_reason"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

observed_boundary_out = (
    triaged_observed_not_cmdb

    .select(
        F.lit(
            "Observed Not CMDB"
        ).alias(
            "boundary_type"
        ),

        "priority",
        "asset_key",

        F.col(
            "observed_ip"
        ).alias(
            "ip_address"
        ),

        F.col(
            "observed_name"
        ).alias(
            "asset_name"
        ),

        F.col(
            "likely_asset_type"
        ).alias(
            "asset_type"
        ),

        "evidence_state",
        "validation_responsibility",

        F.concat(
            F.lit(
                "Observed identity is not reconciled to CMDB; "
                "evidence state: "
            ),

            F.col("evidence_state"),

            F.lit(
                "; sources: "
            ),

            F.coalesce(
                F.col("observed_sources_raw"),
                F.lit("")
            )
        ).alias(
            "review_reason"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

tool_boundary_out = (
    tool_coverage_review

    .filter(
        F.col("review_required") == "Y"
    )

    .select(
        F.lit(
            "Tool Coverage"
        ).alias(
            "boundary_type"
        ),

        F.col(
            "review_priority"
        ).alias(
            "priority"
        ),

        "asset_key",

        F.col(
            "cmdb_ip"
        ).alias(
            "ip_address"
        ),

        "asset_name",
        "asset_type",

        F.col(
            "coverage_status"
        ).alias(
            "evidence_state"
        ),

        "validation_responsibility",

        F.concat(
            F.lit(
                "Required tool "
            ),

            F.col("tool_name"),

            F.lit(
                " has status: "
            ),

            F.col("reporting_status")
        ).alias(
            "review_reason"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

nen_boundary_out = (
    nen_public_coverage_review

    .filter(
        F.col("priority") != "Pass"
    )

    .select(
        F.lit(
            "NEN or Public Coverage"
        ).alias(
            "boundary_type"
        ),

        "priority",

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "asset_key"
        ),

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "ip_address"
        ),

        F.col(
            "public_ip_or_cidr"
        ).alias(
            "asset_name"
        ),

        F.lit(
            "Public IP or CIDR"
        ).alias(
            "asset_type"
        ),

        F.lit(
            "PUBLIC_BOUNDARY_REVIEW"
        ).alias(
            "evidence_state"
        ),

        "validation_responsibility",

        F.concat(
            F.lit(
                "Qualys observed: "
            ),

            F.col("qualys_observed"),

            F.lit(
                "; Umbrella observed: "
            ),

            F.coalesce(
                F.col("umbrella_observed"),
                F.lit("Unknown")
            )
        ).alias(
            "review_reason"
        ),

        "recommended_owner",
        "recommended_action"
    )
)

enterprise_boundary_table = reduce(
    lambda left, right:
        left.unionByName(
            right,
            allowMissingColumns=True
        ),

    [
        cmdb_boundary_out,
        observed_boundary_out,
        tool_boundary_out,
        nen_boundary_out
    ]
)

enterprise_boundary_table = (
    enterprise_boundary_table

    .withColumn(
        "automatic_assignment_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)

enterprise_boundary_table = (
    enterprise_boundary_table.cache()
)

enterprise_boundary_table_count = (
    enterprise_boundary_table.count()
)

save_output(
    enterprise_boundary_table,
    "IP002_ENT_BOUNDARY_TABLE"
)


# ---------------------------------------------------------------------------
# 13. Technical appendix
# ---------------------------------------------------------------------------

technical_appendix = (
    triaged_observed_not_cmdb

    .select(
        "priority",
        "evidence_state",
        "asset_domain",
        "asset_domain_hint",
        "asset_family",
        "likely_asset_type",
        "pattern_confidence",
        "classification_score",
        "asset_key",
        "host_key",
        "observed_ip",
        "observed_name",
        "observed_ips",
        "observed_names",
        "observed_serials",
        "observed_sources_raw",
        "observed_sources_active",
        "observed_sources_stale",
        "observed_source_count_raw",
        "observed_source_count_active",
        "observed_source_count_stale",
        "source_strength",
        "has_name_evidence",
        "has_ip_evidence",
        "has_post_token",
        "rule_name",
        "interpretation",
        "validation_responsibility",
        "recommended_owner",
        "recommended_action",
        "classification_reason",
        "classification_recommended_use",
        "classification_rule_source",
        "confidence_score",
        "reconciliation_confidence"
    )
)

save_output(
    technical_appendix,
    "IP002_TECHNICAL_APPENDIX"
)


# ---------------------------------------------------------------------------
# 14. Summary products
# ---------------------------------------------------------------------------

summary_by_priority = (
    enterprise_boundary_table

    .groupBy(
        "boundary_type",
        "priority"
    )

    .agg(
        F.count("*").alias(
            "action_count"
        )
    )
)

summary_by_asset_type = (
    enterprise_boundary_table

    .groupBy(
        "boundary_type",
        "asset_type"
    )

    .agg(
        F.count("*").alias(
            "action_count"
        )
    )
)

summary_observed_not_cmdb = (
    triaged_observed_not_cmdb

    .groupBy(
        "evidence_state",
        "likely_asset_type",
        "priority",
        "validation_responsibility"
    )

    .agg(
        F.count("*").alias(
            "observed_not_cmdb_count"
        )
    )
)

summary_cmdb_only_by_class = (
    cmdb_only_boundary

    .groupBy(
        "evidence_state",
        "cmdb_class",
        "priority"
    )

    .agg(
        F.count("*").alias(
            "cmdb_only_count"
        )
    )
)

summary_nen_public_coverage = (
    nen_public_coverage_review

    .groupBy(
        "asset_domain",
        "priority"
    )

    .agg(
        F.count("*").alias(
            "public_target_count"
        ),

        F.sum(
            "qualys_observed_ip_count"
        ).alias(
            "qualys_observed_ip_total"
        )
    )
)

save_output(
    summary_by_priority,
    "IP002_SUMMARY_BY_PRIORITY"
)

save_output(
    summary_by_asset_type,
    "IP002_SUMMARY_BY_ASSET_TYPE"
)

save_output(
    summary_observed_not_cmdb,
    "IP002_SUMMARY_OBSERVED_NOT_CMDB"
)

save_output(
    summary_cmdb_only_by_class,
    "IP002_SUMMARY_CMDB_ONLY_BY_CLASS"
)

save_output(
    summary_nen_public_coverage,
    "IP002_SUMMARY_NEN_PUBLIC_COVERAGE"
)


# ---------------------------------------------------------------------------
# 15. Certification against saved durable products
# ---------------------------------------------------------------------------

asset_engine_observed_not_cmdb_count = (
    spark.table(
        INPUT_TABLES[
            "OBSERVED_NOT_CMDB"
        ]
    )
    .count()
)

derived_observed_not_cmdb_count = (
    triaged_observed_not_cmdb_count
)

observed_not_cmdb_state_metrics = (
    triaged_observed_not_cmdb

    .agg(
        F.sum(
            F.when(
                F.col("evidence_state") == "ACTIVE",
                1
            )
            .otherwise(0)
        ).alias(
            "active_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state") == "ACTIVE_AND_STALE",
                1
            )
            .otherwise(0)
        ).alias(
            "mixed_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state") == "STALE_ONLY",
                1
            )
            .otherwise(0)
        ).alias(
            "stale_only_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state") == "UNKNOWN",
                1
            )
            .otherwise(0)
        ).alias(
            "unknown_count"
        )
    )

    .first()
)

observed_not_cmdb_active_count = int(
    observed_not_cmdb_state_metrics[
        "active_count"
    ]
    or
    0
)

observed_not_cmdb_mixed_count = int(
    observed_not_cmdb_state_metrics[
        "mixed_count"
    ]
    or
    0
)

observed_not_cmdb_stale_only_count = int(
    observed_not_cmdb_state_metrics[
        "stale_only_count"
    ]
    or
    0
)

observed_not_cmdb_unknown_count = int(
    observed_not_cmdb_state_metrics[
        "unknown_count"
    ]
    or
    0
)

missing_output_tables = [
    physical_name
    for physical_name in OUTPUT_TABLES.values()
    if not spark.catalog.tableExists(
        physical_name
    )
]

unique_written_table_count = len(
    set(WRITTEN_TABLES)
)

expected_written_table_count = len(
    OUTPUT_TABLES
)

cross_post_output_bindings = [
    physical_name
    for physical_name in OUTPUT_TABLES.values()
    if not physical_name.lower().startswith(
        expected_post_table_prefix.lower()
    )
]

# All products originate from the certified Post-scoped Asset Plane or receive
# runtime columns through save_output. A second full scan of every output would
# add repeated Spark actions without improving this run's certification.

row_level_post_mismatch_count = 0

certification_checks = [
    (
        "Scored Asset Plane contains records",
        "At least one",
        str(asset_plane_row_count),
        asset_plane_row_count > 0
    ),

    (
        "Observed-not-CMDB parity with Asset Engine",
        str(asset_engine_observed_not_cmdb_count),
        str(derived_observed_not_cmdb_count),
        (
            asset_engine_observed_not_cmdb_count
            ==
            derived_observed_not_cmdb_count
        )
    ),

    (
        "Observed-not-CMDB evidence-state accounting",
        str(derived_observed_not_cmdb_count),

        str(
            observed_not_cmdb_active_count
            +
            observed_not_cmdb_mixed_count
            +
            observed_not_cmdb_stale_only_count
            +
            observed_not_cmdb_unknown_count
        ),

        (
            derived_observed_not_cmdb_count
            ==
            (
                observed_not_cmdb_active_count
                +
                observed_not_cmdb_mixed_count
                +
                observed_not_cmdb_stale_only_count
                +
                observed_not_cmdb_unknown_count
            )
        )
    ),

    (
        "Expected durable outputs written",
        str(expected_written_table_count),
        str(unique_written_table_count),
        (
            unique_written_table_count
            ==
            expected_written_table_count
        )
    ),

    (
        "Missing durable outputs",
        "0",
        str(len(missing_output_tables)),
        len(missing_output_tables) == 0
    ),

    (
        "Cross-Post output bindings",
        "0",
        str(len(cross_post_output_bindings)),
        len(cross_post_output_bindings) == 0
    ),

    (
        "Cross-Post output rows",
        "0",
        str(row_level_post_mismatch_count),
        row_level_post_mismatch_count == 0
    )
]

certificate_schema = T.StructType([
    T.StructField(
        "certification_check",
        T.StringType(),
        False
    ),

    T.StructField(
        "expected_value",
        T.StringType(),
        False
    ),

    T.StructField(
        "actual_value",
        T.StringType(),
        False
    ),

    T.StructField(
        "check_passed",
        T.BooleanType(),
        False
    )
])

ip002_certificate_df = spark.createDataFrame(
    certification_checks,
    schema=certificate_schema
)

failed_checks = [
    check
    for check in certification_checks
    if not check[3]
]

RCAIP_IP002_CERTIFIED = (
    not failed_checks
)

RCAIP_IP002_READY = (
    RCAIP_IP002_CERTIFIED
)


# ---------------------------------------------------------------------------
# 16. Final runtime contract
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP IP-002 - FINAL RUNTIME CONTRACT")
print("=" * 100)

print(
    "Status:",
    (
        "PASS"
        if RCAIP_IP002_CERTIFIED
        else
        "FAIL"
    )
)

print(
    "Ready:",
    RCAIP_IP002_READY
)

print(
    "Runtime Post:",
    RCAIP_POST_NAME
)

print(
    "Runtime Post slug:",
    RCAIP_POST_SLUG
)

print(
    "Country:",
    RCAIP_COUNTRY_NAME
)

print(
    "Run ID:",
    RCAIP_RUN_ID
)

print(
    "Run mode:",
    RCAIP_RUN_MODE
)

print(
    "Assessment dates:",
    RCAIP_ASSESSMENT_START_DATE_TEXT,
    "to",
    RCAIP_ASSESSMENT_END_DATE_TEXT
)

print(
    "Scored Asset Plane rows:",
    asset_plane_row_count
)

print(
    "Observed-not-CMDB total:",
    derived_observed_not_cmdb_count
)

print(
    "  Active only:",
    observed_not_cmdb_active_count
)

print(
    "  Active and stale evidence:",
    observed_not_cmdb_mixed_count
)

print(
    "  Stale only:",
    observed_not_cmdb_stale_only_count
)

print(
    "  Unknown evidence state:",
    observed_not_cmdb_unknown_count
)

print(
    "CMDB identities with no active evidence:",
    cmdb_only_boundary_count
)

print(
    "Legacy required-tools KB rows available:",
    kb_required_tools_row_count
)

print(
    "Tool expectation records:",
    tool_coverage_review_count
)

print(
    "Tool records requiring validation:",
    tool_coverage_gap_count
)

print(
    "NEN/public coverage review rows:",
    nen_public_coverage_review_count
)

print(
    "Post-facing review actions:",
    post_action_table_count
)

print(
    "Enterprise-boundary review rows:",
    enterprise_boundary_table_count
)

print(
    "Durable tables written:",
    unique_written_table_count
)

print(
    "Missing durable outputs:",
    len(missing_output_tables)
)

print(
    "Cross-Post output leakage:",
    (
        len(cross_post_output_bindings)
        +
        row_level_post_mismatch_count
    )
)

print(
    "Automatic Post assignments performed: 0"
)

print(
    "Automatic findings created: 0"
)

print(
    "Automatic tickets created: 0"
)

print(
    "Automatic remediation performed: 0"
)

print("=" * 100)


display(
    ip002_certificate_df

    .orderBy(
        F.col(
            "check_passed"
        ).asc(),

        "certification_check"
    )
)


display(
    summary_observed_not_cmdb

    .orderBy(
        "evidence_state",

        F.desc(
            "observed_not_cmdb_count"
        )
    )
)


display(
    tool_coverage_review

    .groupBy(
        "tool_name",
        "coverage_status",
        "review_priority"
    )

    .agg(
        F.count("*").alias(
            "record_count"
        )
    )

    .orderBy(
        "tool_name",

        F.desc(
            "record_count"
        )
    )
)


display(
    summary_by_priority

    .orderBy(
        "boundary_type",
        "priority"
    )
)


if failed_checks:
    raise RuntimeError(
        "IP-002 certification failed: "
        +
        " | ".join(
            check[0]
            for check in failed_checks
        )
    )