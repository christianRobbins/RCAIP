# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %run ./RCAIP_Config_001_Table_Registry
# MAGIC

# COMMAND ----------

# ============================================================================
# RCAIP IP-003 - ASSET LIFECYCLE AND HYGIENE
# Cell 2 - Certified lifecycle foundation
#
# Purpose:
#   Build lifecycle intelligence from the saved scored Asset Plane without
#   rescanning enterprise source systems or rebuilding Asset Engine lineage.
#
# Design:
#   - Preserves one lifecycle rollup row per Asset Plane identity.
#   - Produces one source-lifecycle row per asset/source relationship.
#   - Derives age-in-days from authoritative timestamps at runtime.
#   - Distinguishes current, stale, unknown, and absent evidence.
#   - Writes durable Post-scoped Delta products.
#   - Performs no retirement, deletion, finding, ticket, or remediation.
# ============================================================================

import re

from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import functions as F


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
        "IP-003 did not receive the complete Config 001 runtime: "
        +
        " | ".join(
            missing_runtime_objects
        )
    )


if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready runtime."
    )


allowed_run_modes = {
    "DISCOVERY",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}


if str(RCAIP_RUN_MODE).upper() not in allowed_run_modes:
    raise RuntimeError(
        "IP-003 cannot run in the current mode: "
        +
        str(RCAIP_RUN_MODE)
    )


expected_post_table_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    f"{RCAIP_POST_SLUG}_"
).lower()


# ---------------------------------------------------------------------------
# 2. Resolve registered inputs and protected Post outputs
# ---------------------------------------------------------------------------

def resolve_registered_table(
    logical_name,
    require_exists=True
):
    try:
        physical_name = get_rcaip_table(
            logical_name,
            require_exists=require_exists
        )

    except TypeError:
        physical_name = get_rcaip_table(
            logical_name
        )

        if (
            require_exists
            and
            not spark.catalog.tableExists(
                physical_name
            )
        ):
            raise RuntimeError(
                "Required table does not exist: "
                f"{logical_name} -> {physical_name}"
            )

    return physical_name


def resolve_post_table(
    logical_name,
    fallback_stem,
    require_exists=False
):
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

    if not physical_name.lower().startswith(
        expected_post_table_prefix
    ):
        raise RuntimeError(
            "Cross-Post table protection blocked: "
            +
            physical_name
        )

    if (
        require_exists
        and
        not spark.catalog.tableExists(
            physical_name
        )
    ):
        raise RuntimeError(
            "Required IP-003 input does not exist: "
            +
            physical_name
        )

    return physical_name


IP003_INPUT_TABLES = {
    "ASSET_RECONCILIATION_SCORED":
        resolve_registered_table(
            "ASSET_RECONCILIATION_SCORED",
            require_exists=True
        ),

    "IP002_TRIAGED_OBSERVED_NOT_CMDB":
        resolve_post_table(
            "IP002_TRIAGED_OBSERVED_NOT_CMDB",
            "ip002_triaged_observed_not_cmdb",
            require_exists=True
        ),

    "IP002_CMDB_ONLY_BOUNDARY_REVIEW":
        resolve_post_table(
            "IP002_CMDB_ONLY_BOUNDARY_REVIEW",
            "ip002_cmdb_only_boundary_review",
            require_exists=True
        )
}


IP003_OUTPUT_TABLES = {
    "IP003_SOURCE_LIFECYCLE_OBSERVATIONS":
        resolve_post_table(
            "IP003_SOURCE_LIFECYCLE_OBSERVATIONS",
            "ip003_source_lifecycle_observations"
        ),

    "IP003_ASSET_LIFECYCLE_ROLLUP":
        resolve_post_table(
            "IP003_ASSET_LIFECYCLE_ROLLUP",
            "ip003_asset_lifecycle_rollup"
        ),

    "IP003_STALE_OVER_90":
        resolve_post_table(
            "IP003_STALE_OVER_90",
            "ip003_stale_over_90"
        ),

    "IP003_STALE_MDE_ONLY":
        resolve_post_table(
            "IP003_STALE_MDE_ONLY",
            "ip003_stale_mde_only"
        ),

    "IP003_STALE_MULTI_SOURCE":
        resolve_post_table(
            "IP003_STALE_MULTI_SOURCE",
            "ip003_stale_multi_source"
        ),

    "IP003_UNKNOWN_FRESHNESS":
        resolve_post_table(
            "IP003_UNKNOWN_FRESHNESS",
            "ip003_unknown_freshness"
        ),

    "IP003_LIKELY_STALE_INVENTORY":
        resolve_post_table(
            "IP003_LIKELY_STALE_INVENTORY",
            "ip003_likely_stale_inventory"
        ),

    "IP003_POST_VALIDATION_TABLE":
        resolve_post_table(
            "IP003_POST_VALIDATION_TABLE",
            "ip003_post_validation_table"
        ),

    "IP003_ENT_CLEANUP_TABLE":
        resolve_post_table(
            "IP003_ENT_CLEANUP_TABLE",
            "ip003_ent_cleanup_table"
        ),

    "IP003_SUMMARY_BY_AGE":
        resolve_post_table(
            "IP003_SUMMARY_BY_AGE",
            "ip003_summary_by_age"
        ),

    "IP003_SUMMARY_BY_STALE_CATEGORY":
        resolve_post_table(
            "IP003_SUMMARY_BY_STALE_CATEGORY",
            "ip003_summary_by_stale_category"
        ),

    "IP003_SUMMARY_STALE_BY_SOURCE_COUNT":
        resolve_post_table(
            "IP003_SUMMARY_STALE_BY_SOURCE_COUNT",
            "ip003_summary_stale_by_source_count"
        ),

    "IP003_SUMMARY_MDE_AGE":
        resolve_post_table(
            "IP003_SUMMARY_MDE_AGE",
            "ip003_summary_mde_age"
        ),

    "IP003_SUMMARY_BY_SOURCE_STATUS":
        resolve_post_table(
            "IP003_SUMMARY_BY_SOURCE_STATUS",
            "ip003_summary_by_source_status"
        )
}


# ---------------------------------------------------------------------------
# 3. Output helpers
# ---------------------------------------------------------------------------

IP003_WRITTEN_TABLES = []


def add_runtime_columns(
    dataframe
):
    output = dataframe

    runtime_columns = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "rcaip_run_id": RCAIP_RUN_ID,
        "runtime_country_name": RCAIP_COUNTRY_NAME,
        "runtime_run_mode": RCAIP_RUN_MODE
    }

    for column_name, value in runtime_columns.items():
        if column_name not in output.columns:
            output = output.withColumn(
                column_name,
                F.lit(value)
            )

    if "generated_at_utc" not in output.columns:
        output = output.withColumn(
            "generated_at_utc",
            F.current_timestamp()
        )

    return output


def save_ip003(
    dataframe,
    logical_name
):
    physical_name = IP003_OUTPUT_TABLES[
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
        .saveAsTable(
            physical_name
        )
    )

    IP003_WRITTEN_TABLES.append(
        physical_name
    )

    print(
        "Saved:",
        physical_name
    )

    return physical_name


# ---------------------------------------------------------------------------
# 4. General column and lifecycle helpers
# ---------------------------------------------------------------------------

def first_existing_column(
    dataframe,
    candidates
):
    lookup = {
        str(column_name).lower(): column_name
        for column_name in dataframe.columns
    }

    for candidate in candidates:
        resolved = lookup.get(
            str(candidate).lower()
        )

        if resolved:
            return resolved

    return None


def optional_column(
    dataframe,
    candidates,
    data_type="string"
):
    resolved = first_existing_column(
        dataframe,
        candidates
    )

    if resolved is None:
        return (
            F.lit(None).cast(
                data_type
            ),
            "UNAVAILABLE"
        )

    return (
        F.col(
            f"`{resolved}`"
        ).cast(
            data_type
        ),
        resolved
    )


def age_bucket(
    days_column
):
    return (
        F.when(
            days_column.isNull(),
            F.lit("Unknown")
        )

        .when(
            days_column <= 30,
            F.lit("0-30 Days")
        )

        .when(
            days_column <= 60,
            F.lit("31-60 Days")
        )

        .when(
            days_column <= 90,
            F.lit("61-90 Days")
        )

        .when(
            days_column <= 180,
            F.lit("91-180 Days")
        )

        .otherwise(
            F.lit("Over 180 Days")
        )
    )


# ---------------------------------------------------------------------------
# 5. Load the saved scored Asset Plane
# ---------------------------------------------------------------------------

asset_plane_base = spark.table(
    IP003_INPUT_TABLES[
        "ASSET_RECONCILIATION_SCORED"
    ]
)


required_asset_plane_columns = {
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
    "cmdb_category",
    "cmdb_location",
    "cmdb_install_status",

    "nmap_present",
    "tenable_present",
    "intune_present",
    "mde_present",
    "qualys_present",

    "nmap_active",
    "tenable_active",
    "intune_active",
    "mde_active",
    "qualys_active",

    "nmap_stale",
    "tenable_stale",
    "intune_stale",
    "mde_stale",
    "qualys_stale",

    "observed_sources_raw",
    "observed_sources_active",
    "observed_sources_stale",
    "observed_source_count_raw",
    "observed_source_count_active",
    "observed_source_count_stale",

    "observed_ips",
    "observed_names",
    "observed_serials",

    "most_recent_record_timestamp",
    "most_recent_active_timestamp",

    "current_observation_status",
    "lifecycle_reconciliation_status",
    "confidence_score",
    "reconciliation_confidence"
}


missing_asset_plane_columns = sorted(
    required_asset_plane_columns
    -
    set(
        asset_plane_base.columns
    )
)


if missing_asset_plane_columns:
    raise RuntimeError(
        "The scored Asset Plane does not satisfy the "
        "IP-003 contract. Missing: "
        +
        " | ".join(
            missing_asset_plane_columns
        )
    )


# ---------------------------------------------------------------------------
# 6. Add nullable optional fields used for descriptive enrichment
#
# These fields improve classification when available but do not determine
# whether the core lifecycle contract can run.
# ---------------------------------------------------------------------------

optional_asset_plane_columns = {
    "mde_device_name": "string",
    "mde_device_category": "string",
    "mde_device_type": "string",
    "mde_machine_group": "string",
    "mde_dynamic_tags": "string",
    "mde_manual_tags": "string",
    "mde_public_ip": "string",
    "mde_ip": "string",
    "mde_last_seen": "timestamp",

    "intune_device_name": "string",
    "intune_operating_system": "string",
    "intune_serial_number": "string",
    "intune_last_contact": "timestamp",

    "qualys_asset_name": "string",
    "qualys_scope": "string",
    "qualys_ip": "string",
    "qualys_bios_serial_number": "string",
    "qualys_inventory_last_updated_on": "timestamp",

    "nmap_hostname": "string",
    "nmap_asset_domain": "string",
    "nmap_ip": "string",
    "nmap_last_observed": "timestamp",
    "nmap_scan_start_timestamp": "timestamp",
    "nmap_scan_end_timestamp": "timestamp",
    "nmap_scan_date": "timestamp",

    "tenable_dns_name": "string",
    "tenable_ip": "string",
    "tenable_event_time": "timestamp",

    "cmdb_updated": "timestamp",
    "cmdb_last_observed": "timestamp",
    "cmdb_sys_updated_on": "timestamp",
    "cmdb_created": "timestamp"
}


asset_plane_enriched = asset_plane_base


for column_name, data_type in optional_asset_plane_columns.items():
    if column_name not in asset_plane_enriched.columns:
        asset_plane_enriched = (
            asset_plane_enriched
            .withColumn(
                column_name,
                F.lit(None).cast(
                    data_type
                )
            )
        )


# ---------------------------------------------------------------------------
# 7. Derive Asset Plane age fields from authoritative timestamps
#
# The Asset Engine publishes the timestamps. IP-003 calculates age-in-days
# dynamically rather than requiring redundant calculated fields upstream.
# ---------------------------------------------------------------------------

asset_plane = (
    asset_plane_enriched

    .withColumn(
        "days_since_most_recent_record",

        F.when(
            F.col(
                "most_recent_record_timestamp"
            ).isNotNull(),

            F.greatest(
                F.lit(0),

                F.datediff(
                    F.current_date(),

                    F.to_date(
                        F.col(
                            "most_recent_record_timestamp"
                        )
                    )
                )
            )
        )

        .otherwise(
            F.lit(None).cast(
                "integer"
            )
        )
    )

    .withColumn(
        "days_since_most_recent_active",

        F.when(
            F.col(
                "most_recent_active_timestamp"
            ).isNotNull(),

            F.greatest(
                F.lit(0),

                F.datediff(
                    F.current_date(),

                    F.to_date(
                        F.col(
                            "most_recent_active_timestamp"
                        )
                    )
                )
            )
        )

        .otherwise(
            F.lit(None).cast(
                "integer"
            )
        )
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 8. Certify the Asset Plane population and runtime isolation
# ---------------------------------------------------------------------------

asset_plane_metrics = (
    asset_plane

    .agg(
        F.count("*").alias(
            "asset_plane_rows"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        ),

        F.sum(
            F.when(
                F.lower(
                    F.trim(
                        F.col("post_slug")
                    )
                )
                !=
                F.lit(
                    str(
                        RCAIP_POST_SLUG
                    ).lower()
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "post_mismatch_rows"
        ),

        F.sum(
            F.when(
                F.col("rcaip_run_id")
                !=
                F.lit(
                    RCAIP_RUN_ID
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "run_mismatch_rows"
        ),

        F.sum(
            F.when(
                F.col("cmdb_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "cmdb_rows"
        ),

        F.sum(
            F.when(
                F.col("nmap_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "nmap_rows"
        ),

        F.sum(
            F.when(
                F.col("tenable_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "tenable_rows"
        ),

        F.sum(
            F.when(
                F.col("intune_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "intune_rows"
        ),

        F.sum(
            F.when(
                F.col("mde_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "mde_rows"
        ),

        F.sum(
            F.when(
                F.col("qualys_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "qualys_rows"
        )
    )

    .first()
)


asset_plane_row_count = int(
    asset_plane_metrics[
        "asset_plane_rows"
    ]
    or
    0
)


asset_plane_distinct_key_count = int(
    asset_plane_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)


asset_plane_post_mismatch_count = int(
    asset_plane_metrics[
        "post_mismatch_rows"
    ]
    or
    0
)


asset_plane_run_mismatch_count = int(
    asset_plane_metrics[
        "run_mismatch_rows"
    ]
    or
    0
)


if asset_plane_row_count == 0:
    raise RuntimeError(
        "The scored Asset Plane is empty."
    )


if (
    asset_plane_row_count
    !=
    asset_plane_distinct_key_count
):
    raise RuntimeError(
        "The scored Asset Plane is not one row "
        "per asset_key."
    )


if asset_plane_post_mismatch_count > 0:
    raise RuntimeError(
        "The scored Asset Plane contains cross-Post rows: "
        +
        str(
            asset_plane_post_mismatch_count
        )
    )


if asset_plane_run_mismatch_count > 0:
    raise RuntimeError(
        "The scored Asset Plane contains rows from "
        "another certified run: "
        +
        str(
            asset_plane_run_mismatch_count
        )
    )


# ---------------------------------------------------------------------------
# 9. Build the runtime Post-token expression
# ---------------------------------------------------------------------------

post_tokens = []


for object_name in [
    "RCAIP_POST_TOKENS",
    "RCAIP_DNS_TOKENS",
    "RCAIP_USER_LOCATION_TOKENS",
    "RCAIP_MDE_TOKENS"
]:
    runtime_value = globals().get(
        object_name,
        []
    )

    if isinstance(
        runtime_value,
        (
            list,
            tuple,
            set
        )
    ):
        post_tokens.extend(
            runtime_value
        )


post_tokens.extend([
    RCAIP_POST_NAME,
    RCAIP_POST_SLUG,

    globals().get(
        "RCAIP_POST_CODE",
        ""
    ),

    globals().get(
        "RCAIP_POST_DEVICE_PREFIX",
        ""
    )
])


post_tokens = sorted({
    str(value).strip().lower()
    for value in post_tokens
    if str(
        value or ""
    ).strip()
})


post_token_regex = (
    "|".join(
        re.escape(
            value
        )
        for value in post_tokens
    )
    if post_tokens
    else
    r"(?!)"
)


# ---------------------------------------------------------------------------
# 10. Resolve source-specific timestamps
# ---------------------------------------------------------------------------

cmdb_timestamp, cmdb_timestamp_basis = optional_column(
    asset_plane,
    [
        "cmdb_updated",
        "cmdb_last_observed",
        "cmdb_sys_updated_on",
        "cmdb_created"
    ],
    "timestamp"
)


nmap_timestamp, nmap_timestamp_basis = optional_column(
    asset_plane,
    [
        "nmap_last_observed",
        "nmap_scan_end_timestamp",
        "nmap_scan_start_timestamp",
        "nmap_scan_date"
    ],
    "timestamp"
)


tenable_timestamp, tenable_timestamp_basis = optional_column(
    asset_plane,
    [
        "tenable_event_time"
    ],
    "timestamp"
)


intune_timestamp, intune_timestamp_basis = optional_column(
    asset_plane,
    [
        "intune_last_contact"
    ],
    "timestamp"
)


mde_timestamp, mde_timestamp_basis = optional_column(
    asset_plane,
    [
        "mde_last_seen"
    ],
    "timestamp"
)


qualys_timestamp, qualys_timestamp_basis = optional_column(
    asset_plane,
    [
        "qualys_inventory_last_updated_on"
    ],
    "timestamp"
)


# ---------------------------------------------------------------------------
# 11. Build one lifecycle row per asset-source relationship
# ---------------------------------------------------------------------------

def source_frame(
    source_name,
    source_role,
    presence_column,
    active_column,
    stale_column,
    timestamp_expression,
    timestamp_basis,
    ip_candidates,
    name_candidates,
    serial_candidates,
    domain_candidates
):
    source_ip, _ = optional_column(
        asset_plane,
        ip_candidates,
        "string"
    )

    source_name_expression, _ = optional_column(
        asset_plane,
        name_candidates,
        "string"
    )

    source_serial, _ = optional_column(
        asset_plane,
        serial_candidates,
        "string"
    )

    source_asset_domain, _ = optional_column(
        asset_plane,
        domain_candidates,
        "string"
    )

    days_since_observed = (
        F.when(
            timestamp_expression.isNotNull(),

            F.greatest(
                F.lit(0),

                F.datediff(
                    F.current_date(),

                    F.to_date(
                        timestamp_expression
                    )
                )
            )
        )

        .otherwise(
            F.lit(None).cast(
                "integer"
            )
        )
    )

    if source_name == "CMDB":
        active_flag = (
            F.when(
                timestamp_expression.isNotNull()
                &
                (
                    days_since_observed
                    <=
                    90
                ),
                F.lit(1)
            )

            .otherwise(
                F.lit(0)
            )
        )

        stale_flag = (
            F.when(
                timestamp_expression.isNotNull()
                &
                (
                    days_since_observed
                    >
                    90
                ),
                F.lit(1)
            )

            .otherwise(
                F.lit(0)
            )
        )

    else:
        active_flag = F.col(
            active_column
        ).cast(
            "integer"
        )

        stale_flag = F.col(
            stale_column
        ).cast(
            "integer"
        )

    return (
        asset_plane

        .filter(
            F.col(
                presence_column
            )
            ==
            "Y"
        )

        .select(
            "post_name",
            "post_slug",
            "rcaip_run_id",

            F.coalesce(
                source_asset_domain,
                F.lit("Enterprise")
            ).alias(
                "asset_domain"
            ),

            "asset_key",
            "host_key",

            F.lit(
                source_name
            ).alias(
                "observed_source"
            ),

            F.lit(
                source_role
            ).alias(
                "source_role"
            ),

            source_ip.alias(
                "observed_ip"
            ),

            source_name_expression.alias(
                "observed_name"
            ),

            source_serial.alias(
                "serial_number"
            ),

            timestamp_expression.alias(
                "last_observed_timestamp"
            ),

            F.lit(
                timestamp_basis
            ).alias(
                "timestamp_basis"
            ),

            active_flag.alias(
                "source_active_flag"
            ),

            stale_flag.alias(
                "source_stale_flag"
            ),

            days_since_observed.alias(
                "days_since_observed"
            ),

            age_bucket(
                days_since_observed
            ).alias(
                "observation_age_bucket"
            )
        )

        .withColumn(
            "source_freshness_status",

            F.when(
                F.col(
                    "source_active_flag"
                )
                ==
                1,
                "CURRENT"
            )

            .when(
                F.col(
                    "source_stale_flag"
                )
                ==
                1,
                "STALE"
            )

            .when(
                F.col(
                    "last_observed_timestamp"
                ).isNotNull()
                &
                (
                    F.col(
                        "days_since_observed"
                    )
                    <=
                    90
                ),
                "CURRENT_BY_TIMESTAMP"
            )

            .when(
                F.col(
                    "last_observed_timestamp"
                ).isNotNull()
                &
                (
                    F.col(
                        "days_since_observed"
                    )
                    >
                    90
                ),
                "STALE_BY_TIMESTAMP"
            )

            .otherwise(
                "UNKNOWN"
            )
        )

        .withColumn(
            "is_stale_90",

            F.when(
                F.col(
                    "source_freshness_status"
                ).isin(
                    "STALE",
                    "STALE_BY_TIMESTAMP"
                ),
                "Y"
            )

            .when(
                F.col(
                    "source_freshness_status"
                )
                ==
                "UNKNOWN",
                "Unknown"
            )

            .otherwise(
                "N"
            )
        )

        .withColumn(
            "is_stale_180",

            F.when(
                F.col(
                    "days_since_observed"
                )
                >
                180,
                "Y"
            )

            .when(
                F.col(
                    "days_since_observed"
                ).isNull(),
                "Unknown"
            )

            .otherwise(
                "N"
            )
        )
    )


source_frames = [
    source_frame(
        source_name="CMDB",
        source_role="Authoritative Inventory",
        presence_column="cmdb_present",
        active_column=None,
        stale_column=None,
        timestamp_expression=cmdb_timestamp,
        timestamp_basis=cmdb_timestamp_basis,
        ip_candidates=[
            "cmdb_ip"
        ],
        name_candidates=[
            "cmdb_name",
            "cmdb_fqdn"
        ],
        serial_candidates=[
            "cmdb_serial_number"
        ],
        domain_candidates=[
            "cmdb_location"
        ]
    ),

    source_frame(
        source_name="NMAP",
        source_role="Network Observation",
        presence_column="nmap_present",
        active_column="nmap_active",
        stale_column="nmap_stale",
        timestamp_expression=nmap_timestamp,
        timestamp_basis=nmap_timestamp_basis,
        ip_candidates=[
            "nmap_ip"
        ],
        name_candidates=[
            "nmap_hostname"
        ],
        serial_candidates=[],
        domain_candidates=[
            "nmap_asset_domain"
        ]
    ),

    source_frame(
        source_name="Tenable",
        source_role="Vulnerability Management",
        presence_column="tenable_present",
        active_column="tenable_active",
        stale_column="tenable_stale",
        timestamp_expression=tenable_timestamp,
        timestamp_basis=tenable_timestamp_basis,
        ip_candidates=[
            "tenable_ip"
        ],
        name_candidates=[
            "tenable_dns_name"
        ],
        serial_candidates=[],
        domain_candidates=[]
    ),

    source_frame(
        source_name="Intune",
        source_role="Endpoint Management",
        presence_column="intune_present",
        active_column="intune_active",
        stale_column="intune_stale",
        timestamp_expression=intune_timestamp,
        timestamp_basis=intune_timestamp_basis,
        ip_candidates=[],
        name_candidates=[
            "intune_device_name"
        ],
        serial_candidates=[
            "intune_serial_number"
        ],
        domain_candidates=[]
    ),

    source_frame(
        source_name="MDE",
        source_role="Endpoint Protection",
        presence_column="mde_present",
        active_column="mde_active",
        stale_column="mde_stale",
        timestamp_expression=mde_timestamp,
        timestamp_basis=mde_timestamp_basis,
        ip_candidates=[
            "mde_public_ip",
            "mde_ip"
        ],
        name_candidates=[
            "mde_device_name"
        ],
        serial_candidates=[],
        domain_candidates=[
            "mde_machine_group"
        ]
    ),

    source_frame(
        source_name="Qualys",
        source_role="External and Vulnerability Management",
        presence_column="qualys_present",
        active_column="qualys_active",
        stale_column="qualys_stale",
        timestamp_expression=qualys_timestamp,
        timestamp_basis=qualys_timestamp_basis,
        ip_candidates=[
            "qualys_ip"
        ],
        name_candidates=[
            "qualys_asset_name"
        ],
        serial_candidates=[
            "qualys_bios_serial_number"
        ],
        domain_candidates=[
            "qualys_scope"
        ]
    )
]


source_lifecycle_observations = (
    reduce(
        lambda left, right:
            left.unionByName(
                right,
                allowMissingColumns=True
            ),

        source_frames
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


source_lifecycle_observation_count = (
    source_lifecycle_observations.count()
)


expected_source_lifecycle_observation_count = sum([
    int(
        asset_plane_metrics[
            "cmdb_rows"
        ]
        or
        0
    ),

    int(
        asset_plane_metrics[
            "nmap_rows"
        ]
        or
        0
    ),

    int(
        asset_plane_metrics[
            "tenable_rows"
        ]
        or
        0
    ),

    int(
        asset_plane_metrics[
            "intune_rows"
        ]
        or
        0
    ),

    int(
        asset_plane_metrics[
            "mde_rows"
        ]
        or
        0
    ),

    int(
        asset_plane_metrics[
            "qualys_rows"
        ]
        or
        0
    )
])


if (
    source_lifecycle_observation_count
    !=
    expected_source_lifecycle_observation_count
):
    raise RuntimeError(
        "Source lifecycle observation accounting failed. "
        f"Expected {expected_source_lifecycle_observation_count}; "
        f"created {source_lifecycle_observation_count}."
    )


save_ip003(
    source_lifecycle_observations,
    "IP003_SOURCE_LIFECYCLE_OBSERVATIONS"
)


# ---------------------------------------------------------------------------
# 12. Build one lifecycle record per Asset Plane identity
# ---------------------------------------------------------------------------

asset_name_expression = F.coalesce(
    F.col("cmdb_name"),
    F.col("mde_device_name"),
    F.col("intune_device_name"),
    F.col("qualys_asset_name"),
    F.col("nmap_hostname"),
    F.col("tenable_dns_name"),
    F.col("host_key"),
    F.col("asset_key")
)


device_name_expression = F.coalesce(
    F.col("mde_device_name"),
    F.col("intune_device_name"),
    F.col("cmdb_name"),
    F.col("nmap_hostname"),
    F.col("tenable_dns_name")
)


asset_descriptor = F.lower(
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


asset_lifecycle_rollup = (
    asset_plane

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
            "ACTIVE_AND_STALE"
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            >
            0,
            "ACTIVE"
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >
            0,
            "STALE_ONLY"
        )

        .when(
            F.col(
                "observed_source_count_raw"
            )
            >
            0,
            "UNKNOWN"
        )

        .otherwise(
            "NO_OBSERVATION"
        )
    )

    .withColumn(
        "lifecycle_state",

        F.when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col("evidence_state") == "ACTIVE"
            ),
            "OBSERVED_NOT_CMDB_CURRENT"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col("evidence_state")
                ==
                "ACTIVE_AND_STALE"
            ),
            "OBSERVED_NOT_CMDB_CURRENT_WITH_STALE_RESIDUE"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col("evidence_state")
                ==
                "STALE_ONLY"
            ),
            "OBSERVED_NOT_CMDB_STALE_ONLY"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col("evidence_state")
                ==
                "UNKNOWN"
            ),
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS"
        )

        .when(
            (
                F.col("cmdb_present") == "Y"
            )
            &
            (
                F.col("evidence_state") == "ACTIVE"
            ),
            "CMDB_WITH_CURRENT_EVIDENCE"
        )

        .when(
            (
                F.col("cmdb_present") == "Y"
            )
            &
            (
                F.col("evidence_state")
                ==
                "ACTIVE_AND_STALE"
            ),
            "CMDB_WITH_CURRENT_AND_STALE_EVIDENCE"
        )

        .when(
            (
                F.col("cmdb_present") == "Y"
            )
            &
            (
                F.col("evidence_state")
                ==
                "STALE_ONLY"
            ),
            "CMDB_WITH_STALE_ONLY_EVIDENCE"
        )

        .when(
            (
                F.col("cmdb_present") == "Y"
            )
            &
            (
                F.col("evidence_state")
                ==
                "UNKNOWN"
            ),
            "CMDB_WITH_UNKNOWN_FRESHNESS"
        )

        .when(
            (
                F.col("cmdb_present") == "Y"
            )
            &
            (
                F.col("evidence_state")
                ==
                "NO_OBSERVATION"
            ),
            "CMDB_WITHOUT_OBSERVATION"
        )

        .otherwise(
            "UNRESOLVED_LIFECYCLE_STATE"
        )
    )

    .withColumn(
        "asset_name",
        asset_name_expression
    )

    .withColumn(
        "device_name",
        device_name_expression
    )

    .withColumn(
        "asset_type",

        F.when(
            asset_descriptor.rlike(
                "printer|mfd|prnt"
            ),
            "Printer or MFD"
        )

        .when(
            asset_descriptor.rlike(
                "server|domain controller|srv"
            ),
            "Server"
        )

        .when(
            asset_descriptor.rlike(
                "laptop|desktop|workstation|computer|windows"
            ),
            "Endpoint"
        )

        .when(
            asset_descriptor.rlike(
                "switch|router|firewall|vpn|"
                "access point|wireless"
            ),
            "Network Infrastructure"
        )

        .otherwise(
            "Other / Unknown"
        )
    )

    .withColumn(
        "asset_domain",

        F.coalesce(
            F.col("nmap_asset_domain"),
            F.col("qualys_scope"),
            F.col("cmdb_location"),
            F.lit("Enterprise")
        )
    )

    .withColumn(
        "days_since_last_seen",

        F.col(
            "days_since_most_recent_record"
        ).cast(
            "integer"
        )
    )

    .withColumn(
        "record_age_bucket",

        age_bucket(
            F.col(
                "days_since_last_seen"
            )
        )
    )

    .withColumn(
        "active_age_bucket",

        age_bucket(
            F.col(
                "days_since_most_recent_active"
            ).cast(
                "integer"
            )
        )
    )

    .withColumn(
        "stale_evidence_indicator",

        F.when(
            F.col(
                "observed_source_count_stale"
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
        "unknown_freshness_indicator",

        F.when(
            F.col("evidence_state") == "UNKNOWN",
            "Y"
        )

        .otherwise(
            "N"
        )
    )

    .withColumn(
        "has_post_token",

        F.lower(
            F.concat_ws(
                " ",
                "asset_key",
                "host_key",
                "asset_name",
                "device_name",
                "cmdb_location",
                "observed_names",
                "observed_ips",
                "mde_machine_group",
                "mde_dynamic_tags",
                "mde_manual_tags"
            )
        ).rlike(
            post_token_regex
        )
    )

    .withColumn(
        "lifecycle_status",

        F.when(
            F.col("evidence_state") == "ACTIVE",
            "Current"
        )

        .when(
            F.col("evidence_state")
            ==
            "ACTIVE_AND_STALE",
            "Current with stale residue"
        )

        .when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "Stale only"
        )

        .when(
            F.col("evidence_state")
            ==
            "UNKNOWN",
            "Unknown freshness"
        )

        .otherwise(
            "No observation"
        )
    )

    .withColumn(
        "freshness_status",
        F.col(
            "lifecycle_status"
        )
    )

    .withColumn(
        "review_priority",

        F.when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                >=
                2
            ),
            "High"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                1
            ),
            "Medium"
        )

        .when(
            F.col(
                "lifecycle_state"
            ).isin(
                "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS",
                "CMDB_WITH_UNKNOWN_FRESHNESS",
                "CMDB_WITHOUT_OBSERVATION",
                "CMDB_WITH_STALE_ONLY_EVIDENCE"
            ),
            "Medium"
        )

        .when(
            F.col("evidence_state")
            ==
            "ACTIVE_AND_STALE",
            "Review"
        )

        .when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "Low"
        )

        .otherwise(
            "Pass"
        )
    )

    .withColumn(
        "validation_responsibility",

        F.when(
            F.col(
                "lifecycle_state"
            ).startswith(
                "OBSERVED_NOT_CMDB_CURRENT"
            ),
            "CMDB, Enterprise Attribution, and Post Corroboration"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_STALE_ONLY",
            "Enterprise Source Owner and Data Hygiene"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS",
            "Enterprise Source Owner and Ingestion Governance"
        )

        .when(
            F.col(
                "lifecycle_state"
            ).isin(
                "CMDB_WITHOUT_OBSERVATION",
                "CMDB_WITH_STALE_ONLY_EVIDENCE",
                "CMDB_WITH_UNKNOWN_FRESHNESS"
            ),
            "CMDB and Enterprise Asset Management"
        )

        .when(
            F.col("evidence_state")
            ==
            "ACTIVE_AND_STALE",
            "Enterprise Source Owner Lifecycle Cleanup"
        )

        .otherwise(
            "Assurance Evidence Only"
        )
    )

    .withColumn(
        "recommended_action",

        F.when(
            F.col(
                "lifecycle_state"
            ).startswith(
                "OBSERVED_NOT_CMDB_CURRENT"
            ),
            "Validate canonical identity, ownership, operational status, "
            "and CMDB representation; do not create a finding solely from "
            "the reconciliation gap."
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_STALE_ONLY",
            "Validate whether historical source objects should be retained, "
            "retired, merged, or corrected; do not treat the record as a "
            "current missing CMDB asset."
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS",
            "Validate source timestamp availability, ingestion freshness, "
            "identity correlation, and present operational status."
        )

        .when(
            F.col(
                "lifecycle_state"
            ).isin(
                "CMDB_WITHOUT_OBSERVATION",
                "CMDB_WITH_STALE_ONLY_EVIDENCE",
                "CMDB_WITH_UNKNOWN_FRESHNESS"
            ),
            "Validate existence, current ownership, lifecycle status, "
            "duplicate or renamed identity, and source-discovery coverage."
        )

        .when(
            F.col("evidence_state")
            ==
            "ACTIVE_AND_STALE",
            "Retain current evidence and review stale source residue for "
            "cleanup or reconciliation."
        )

        .otherwise(
            "Retain as current lifecycle assurance evidence."
        )
    )

    .withColumn(
        "automatic_retirement_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_deletion_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 13. Certify and save the asset lifecycle rollup
# ---------------------------------------------------------------------------

asset_lifecycle_metrics = (
    asset_lifecycle_rollup

    .agg(
        F.count("*").alias(
            "row_count"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_key_count"
        )
    )

    .first()
)


asset_lifecycle_rollup_count = int(
    asset_lifecycle_metrics[
        "row_count"
    ]
    or
    0
)


asset_lifecycle_distinct_key_count = int(
    asset_lifecycle_metrics[
        "distinct_key_count"
    ]
    or
    0
)


if (
    asset_lifecycle_rollup_count
    !=
    asset_plane_row_count
):
    raise RuntimeError(
        "Lifecycle rollup did not preserve the "
        "complete Asset Plane. "
        f"Asset Plane rows: {asset_plane_row_count}; "
        f"lifecycle rows: {asset_lifecycle_rollup_count}."
    )


if (
    asset_lifecycle_distinct_key_count
    !=
    asset_lifecycle_rollup_count
):
    raise RuntimeError(
        "Lifecycle rollup is not one row per asset_key. "
        f"Rows: {asset_lifecycle_rollup_count}; "
        f"distinct keys: {asset_lifecycle_distinct_key_count}."
    )


save_ip003(
    asset_lifecycle_rollup,
    "IP003_ASSET_LIFECYCLE_ROLLUP"
)


# ---------------------------------------------------------------------------
# 14. Foundation result
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP IP-003 FOUNDATION")
print("=" * 100)

print(
    "Status: PASS"
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
    "Run ID:",
    RCAIP_RUN_ID
)

print(
    "Scored Asset Plane rows:",
    asset_plane_row_count
)

print(
    "Source lifecycle observations:",
    source_lifecycle_observation_count
)

print(
    "Expected source lifecycle observations:",
    expected_source_lifecycle_observation_count
)

print(
    "Asset lifecycle rollup rows:",
    asset_lifecycle_rollup_count
)

print(
    "Distinct lifecycle asset keys:",
    asset_lifecycle_distinct_key_count
)

print(
    "Foundation tables written:",
    len(
        IP003_WRITTEN_TABLES
    )
)

print(
    "Automatic retirement performed: 0"
)

print(
    "Automatic deletion performed: 0"
)

print(
    "Automatic findings created: 0"
)

print(
    "Automatic remediation performed: 0"
)

print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-003 - ASSET LIFECYCLE AND HYGIENE
# Cell 3 - Lifecycle review and cleanup work package
#
# Reads saved IP-003 products so downstream products do not rebuild the
# Asset Engine lineage.
# ============================================================================

from pyspark import StorageLevel
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# 1. Reload saved foundation and IP-002 context
# ---------------------------------------------------------------------------

lifecycle_rollup = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_ASSET_LIFECYCLE_ROLLUP"
        ]
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


source_lifecycle = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_SOURCE_LIFECYCLE_OBSERVATIONS"
        ]
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


ip002_triaged = (
    spark.table(
        IP003_INPUT_TABLES[
            "IP002_TRIAGED_OBSERVED_NOT_CMDB"
        ]
    )

    .select(
        "asset_key",

        F.col(
            "priority"
        ).alias(
            "ip002_priority"
        ),

        F.col(
            "has_post_token"
        ).alias(
            "ip002_has_post_token"
        ),

        F.col(
            "source_strength"
        ).alias(
            "ip002_source_strength"
        ),

        F.col(
            "interpretation"
        ).alias(
            "ip002_interpretation"
        ),

        F.col(
            "validation_responsibility"
        ).alias(
            "ip002_validation_responsibility"
        )
    )
)


ip002_cmdb_only = (
    spark.table(
        IP003_INPUT_TABLES[
            "IP002_CMDB_ONLY_BOUNDARY_REVIEW"
        ]
    )

    .select(
        "asset_key",

        F.col(
            "boundary_issue"
        ).alias(
            "ip002_cmdb_boundary_issue"
        ),

        F.col(
            "priority"
        ).alias(
            "ip002_cmdb_priority"
        )
    )
)


# ---------------------------------------------------------------------------
# 2. Stale and unknown-freshness populations
# ---------------------------------------------------------------------------

stale_over_90 = (
    lifecycle_rollup

    .filter(
        (
            F.col("evidence_state")
            ==
            "STALE_ONLY"
        )
        |
        (
            F.col("days_since_last_seen")
            >
            90
        )
    )

    .withColumn(
        "stale_90_basis",

        F.when(
            F.col("evidence_state")
            ==
            "STALE_ONLY",
            "Asset Plane stale-only source classification"
        )

        .otherwise(
            "Most recent record timestamp exceeds 90 days"
        )
    )

    .withColumn(
        "priority",

        F.when(
            F.col(
                "observed_source_count_raw"
            )
            >=
            2,
            "Medium"
        )

        .when(
            F.col("cmdb_present") == "Y",
            "Medium"
        )

        .otherwise(
            "Low"
        )
    )

    .withColumn(
        "recommended_owner",

        F.when(
            F.col("cmdb_present") == "Y",
            "CMDB and Enterprise Asset Management"
        )

        .when(
            F.col(
                "observed_source_count_raw"
            )
            >=
            2,
            "Joint Post and Enterprise Source Owners"
        )

        .otherwise(
            "Enterprise Source Owner and Data Hygiene"
        )
    )

    .withColumn(
        "recommended_action",

        F.when(
            F.col(
                "observed_source_count_raw"
            )
            >=
            2,
            "Validate retirement, reassignment, rename, extended offline "
            "status, and cleanup across all contributing sources."
        )

        .when(
            F.col("cmdb_present") == "Y",
            "Validate whether the CMDB identity remains active, was renamed "
            "or reassigned, or requires lifecycle correction."
        )

        .otherwise(
            "Validate the source object before retirement, deletion, or "
            "continued retention."
        )
    )

    .withColumn(
        "automatic_retirement_allowed",
        F.lit(False)
    )
)


stale_mde_only = (
    lifecycle_rollup

    .filter(
        F.col("lifecycle_state")
        ==
        "OBSERVED_NOT_CMDB_STALE_ONLY"
    )

    .filter(
        F.col(
            "observed_source_count_raw"
        )
        ==
        1
    )

    .filter(
        F.upper(
            F.trim(
                F.col("observed_sources_raw")
            )
        )
        ==
        "MDE"
    )

    .withColumn(
        "priority",
        F.lit("Low")
    )

    .withColumn(
        "lifecycle_interpretation",

        F.lit(
            "MDE-only identity with stale evidence and no CMDB match. "
            "This may represent retired, renamed, duplicate, or retained "
            "Defender inventory."
        )
    )

    .withColumn(
        "recommended_owner",

        F.lit(
            "Enterprise MDE and Asset Data Hygiene"
        )
    )

    .withColumn(
        "recommended_action",

        F.lit(
            "Validate MDE device state and identity before retiring, "
            "merging, or retaining the object."
        )
    )

    .withColumn(
        "automatic_retirement_allowed",
        F.lit(False)
    )
)


stale_multi_source = (
    lifecycle_rollup

    .filter(
        F.col("evidence_state")
        ==
        "STALE_ONLY"
    )

    .filter(
        F.col(
            "observed_source_count_raw"
        )
        >=
        2
    )

    .withColumn(
        "priority",
        F.lit("Medium")
    )

    .withColumn(
        "lifecycle_interpretation",

        F.lit(
            "The identity is historically corroborated by multiple sources "
            "but has no current observation."
        )
    )

    .withColumn(
        "recommended_owner",

        F.lit(
            "Joint Post, CMDB, and Enterprise Source-Owner Validation"
        )
    )

    .withColumn(
        "recommended_action",

        F.lit(
            "Determine whether the asset was retired, reassigned, renamed, "
            "offline, duplicated, or retained incorrectly across sources."
        )
    )

    .withColumn(
        "automatic_retirement_allowed",
        F.lit(False)
    )
)


unknown_freshness = (
    lifecycle_rollup

    .filter(
        F.col("evidence_state")
        ==
        "UNKNOWN"
    )

    .withColumn(
        "priority",
        F.lit("Medium")
    )

    .withColumn(
        "unknown_freshness_reason",

        F.lit(
            "One or more source records exist, but current-versus-stale "
            "status cannot be established from the available timestamp and "
            "lifecycle evidence."
        )
    )

    .withColumn(
        "recommended_owner",

        F.when(
            F.col("cmdb_present") == "Y",
            "CMDB and Enterprise Source-Ingestion Governance"
        )

        .otherwise(
            "Enterprise Source Owner and Ingestion Governance"
        )
    )

    .withColumn(
        "recommended_action",

        F.lit(
            "Validate source timestamp semantics, ingestion freshness, "
            "identity correlation, and present operational status."
        )
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)


# ---------------------------------------------------------------------------
# 3. Lifecycle cleanup classification
# ---------------------------------------------------------------------------

likely_stale_inventory = (
    stale_over_90

    .withColumn(
        "stale_inventory_category",

        F.when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                1
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col("observed_sources_raw")
                    )
                )
                ==
                "MDE"
            ),
            "Likely Stale MDE Object"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                1
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col("observed_sources_raw")
                    )
                )
                ==
                "INTUNE"
            ),
            "Likely Stale Intune Object"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                1
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col("observed_sources_raw")
                    )
                )
                ==
                "TENABLE"
            ),
            "Likely Stale Tenable Object"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                1
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col("observed_sources_raw")
                    )
                )
                ==
                "NMAP"
            ),
            "Stale Nmap Observation"
        )

        .when(
            (
                F.col("cmdb_present") == "N"
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                1
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col("observed_sources_raw")
                    )
                )
                ==
                "QUALYS"
            ),
            "Likely Stale Qualys Object"
        )

        .when(
            F.col(
                "observed_source_count_raw"
            )
            >=
            2,
            "Multi-Source Historical Asset"
        )

        .when(
            F.col("cmdb_present") == "Y",
            "CMDB Lifecycle Review"
        )

        .otherwise(
            "Stale Single-Source Observation"
        )
    )

    .withColumn(
        "validation_responsibility",

        F.when(
            F.col("stale_inventory_category")
            ==
            "CMDB Lifecycle Review",
            "CMDB and Enterprise Asset Management"
        )

        .when(
            F.col("stale_inventory_category")
            ==
            "Multi-Source Historical Asset",
            "Joint Post and Enterprise Source Owners"
        )

        .otherwise(
            "Enterprise Source Owner and Data Hygiene"
        )
    )
)


# ---------------------------------------------------------------------------
# 4. Post-facing lifecycle-validation queue
# ---------------------------------------------------------------------------

post_validation_table = (
    lifecycle_rollup.alias("l")

    .join(
        ip002_triaged.alias("t"),

        F.col("l.asset_key")
        ==
        F.col("t.asset_key"),

        "left"
    )

    .join(
        ip002_cmdb_only.alias("c"),

        F.col("l.asset_key")
        ==
        F.col("c.asset_key"),

        "left"
    )

    .filter(
        (
            F.col("l.has_post_token") == True
        )
        |
        (
            F.upper(
                F.coalesce(
                    F.col(
                        "t.ip002_has_post_token"
                    ),
                    F.lit("N")
                )
            )
            ==
            "Y"
        )
    )

    .filter(
        F.col(
            "l.review_priority"
        ).isin(
            "High",
            "Medium",
            "Review"
        )
    )

    .filter(
        (
            F.col(
                "l.lifecycle_state"
            ).startswith(
                "OBSERVED_NOT_CMDB"
            )
        )
        |
        (
            F.col(
                "l.lifecycle_state"
            ).isin(
                "CMDB_WITHOUT_OBSERVATION",
                "CMDB_WITH_STALE_ONLY_EVIDENCE",
                "CMDB_WITH_UNKNOWN_FRESHNESS",
                "CMDB_WITH_CURRENT_AND_STALE_EVIDENCE"
            )
        )
    )

    .select(
        F.col(
            "l.review_priority"
        ).alias(
            "priority"
        ),

        F.col("l.asset_domain"),
        F.col("l.lifecycle_state"),
        F.col("l.asset_type"),
        F.col("l.asset_key"),
        F.col("l.host_key"),
        F.col("l.asset_name"),
        F.col("l.device_name"),
        F.col("l.cmdb_name"),
        F.col("l.cmdb_ip"),
        F.col("l.observed_names"),
        F.col("l.observed_ips"),
        F.col("l.observed_serials"),
        F.col("l.observed_sources_raw"),
        F.col("l.observed_sources_active"),
        F.col("l.observed_sources_stale"),
        F.col("l.observed_source_count_raw"),
        F.col("l.observed_source_count_active"),
        F.col("l.observed_source_count_stale"),
        F.col("l.most_recent_record_timestamp"),
        F.col("l.most_recent_active_timestamp"),
        F.col("l.days_since_most_recent_record"),
        F.col("l.days_since_most_recent_active"),
        F.col("l.record_age_bucket"),
        F.col("l.active_age_bucket"),

        F.coalesce(
            F.col(
                "t.ip002_source_strength"
            ),
            F.lit("Not applicable")
        ).alias(
            "source_strength"
        ),

        F.coalesce(
            F.col(
                "t.ip002_interpretation"
            ),
            F.col(
                "c.ip002_cmdb_boundary_issue"
            ),
            F.col(
                "l.lifecycle_status"
            )
        ).alias(
            "validation_context"
        ),

        F.col(
            "l.validation_responsibility"
        ),

        F.col(
            "l.recommended_action"
        )
    )

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


# ---------------------------------------------------------------------------
# 5. Enterprise lifecycle and source-hygiene queue
# ---------------------------------------------------------------------------

enterprise_cleanup_table = (
    lifecycle_rollup

    .filter(
        F.col("lifecycle_state").isin(
            "OBSERVED_NOT_CMDB_STALE_ONLY",
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS",
            "OBSERVED_NOT_CMDB_CURRENT_WITH_STALE_RESIDUE",
            "CMDB_WITH_STALE_ONLY_EVIDENCE",
            "CMDB_WITH_UNKNOWN_FRESHNESS",
            "CMDB_WITHOUT_OBSERVATION",
            "CMDB_WITH_CURRENT_AND_STALE_EVIDENCE"
        )
    )

    .withColumn(
        "cleanup_category",

        F.when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_STALE_ONLY",
            "Observed identity supported only by stale evidence"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS",
            "Observed identity with unresolved source freshness"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "OBSERVED_NOT_CMDB_CURRENT_WITH_STALE_RESIDUE",
            "Current observed identity with stale source residue"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "CMDB_WITH_STALE_ONLY_EVIDENCE",
            "CMDB identity supported only by stale evidence"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "CMDB_WITH_UNKNOWN_FRESHNESS",
            "CMDB identity with unresolved source freshness"
        )

        .when(
            F.col("lifecycle_state")
            ==
            "CMDB_WITHOUT_OBSERVATION",
            "CMDB identity without supporting observation"
        )

        .otherwise(
            "CMDB identity with current and stale source residue"
        )
    )

    .select(
        F.col(
            "review_priority"
        ).alias(
            "priority"
        ),

        "cleanup_category",
        "lifecycle_state",
        "asset_domain",
        "asset_type",
        "asset_key",
        "host_key",
        "asset_name",
        "device_name",
        "cmdb_name",
        "cmdb_ip",
        "cmdb_class",
        "cmdb_location",
        "cmdb_install_status",
        "observed_names",
        "observed_ips",
        "observed_serials",
        "observed_sources_raw",
        "observed_sources_active",
        "observed_sources_stale",
        "observed_source_count_raw",
        "observed_source_count_active",
        "observed_source_count_stale",
        "most_recent_record_timestamp",
        "most_recent_active_timestamp",
        "days_since_most_recent_record",
        "days_since_most_recent_active",
        "record_age_bucket",
        "active_age_bucket",
        "validation_responsibility",
        "recommended_action"
    )

    .withColumn(
        "automatic_retirement_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_deletion_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_finding_allowed",
        F.lit(False)
    )
)


# ---------------------------------------------------------------------------
# 6. Summary products
# ---------------------------------------------------------------------------

summary_by_age = (
    lifecycle_rollup

    .groupBy(
        "record_age_bucket",
        "lifecycle_state"
    )

    .agg(
        F.count("*").alias(
            "asset_count"
        )
    )
)


summary_by_stale_category = (
    likely_stale_inventory

    .groupBy(
        "stale_inventory_category",
        "priority",
        "validation_responsibility"
    )

    .agg(
        F.count("*").alias(
            "asset_count"
        )
    )
)


summary_stale_by_source_count = (
    stale_over_90

    .groupBy(
        "observed_source_count_raw",
        "observed_sources_raw"
    )

    .agg(
        F.count("*").alias(
            "stale_asset_count"
        )
    )
)


summary_mde_age = (
    source_lifecycle

    .filter(
        F.col("observed_source")
        ==
        "MDE"
    )

    .groupBy(
        "observation_age_bucket",
        "source_freshness_status"
    )

    .agg(
        F.countDistinct(
            "asset_key"
        ).alias(
            "mde_related_asset_count"
        )
    )
)


summary_by_source_status = (
    source_lifecycle

    .groupBy(
        "observed_source",
        "source_role",
        "source_freshness_status",
        "timestamp_basis"
    )

    .agg(
        F.count("*").alias(
            "source_record_count"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_count"
        )
    )
)


# ---------------------------------------------------------------------------
# 7. Save work-package products
# ---------------------------------------------------------------------------

review_products = {
    "IP003_STALE_OVER_90":
        stale_over_90,

    "IP003_STALE_MDE_ONLY":
        stale_mde_only,

    "IP003_STALE_MULTI_SOURCE":
        stale_multi_source,

    "IP003_UNKNOWN_FRESHNESS":
        unknown_freshness,

    "IP003_LIKELY_STALE_INVENTORY":
        likely_stale_inventory,

    "IP003_POST_VALIDATION_TABLE":
        post_validation_table,

    "IP003_ENT_CLEANUP_TABLE":
        enterprise_cleanup_table,

    "IP003_SUMMARY_BY_AGE":
        summary_by_age,

    "IP003_SUMMARY_BY_STALE_CATEGORY":
        summary_by_stale_category,

    "IP003_SUMMARY_STALE_BY_SOURCE_COUNT":
        summary_stale_by_source_count,

    "IP003_SUMMARY_MDE_AGE":
        summary_mde_age,

    "IP003_SUMMARY_BY_SOURCE_STATUS":
        summary_by_source_status
}


for logical_name, dataframe in review_products.items():
    save_ip003(
        dataframe,
        logical_name
    )


print("=" * 100)
print("RCAIP IP-003 WORK PACKAGE")
print("=" * 100)
print("Status: BUILT")
print(
    "Review products written:",
    len(review_products)
)
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-003 - ASSET LIFECYCLE AND HYGIENE
# Cell 4 - Final durable-product certification
#
# Reads saved Delta products. It performs no content hashing and does not
# rebuild Asset Engine or IP-002 lineages.
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Load saved foundation
# ---------------------------------------------------------------------------

cert_lifecycle = spark.table(
    IP003_OUTPUT_TABLES[
        "IP003_ASSET_LIFECYCLE_ROLLUP"
    ]
)


cert_source_lifecycle = spark.table(
    IP003_OUTPUT_TABLES[
        "IP003_SOURCE_LIFECYCLE_OBSERVATIONS"
    ]
)


expected_asset_plane_count = (
    spark.table(
        IP003_INPUT_TABLES[
            "ASSET_RECONCILIATION_SCORED"
        ]
    )
    .count()
)


# ---------------------------------------------------------------------------
# 2. Dynamically load the certified IP-002 baseline
# ---------------------------------------------------------------------------

ip002_expected_metrics = (
    spark.table(
        IP003_INPUT_TABLES[
            "IP002_TRIAGED_OBSERVED_NOT_CMDB"
        ]
    )

    .agg(
        F.count("*").alias(
            "observed_not_cmdb_total"
        ),

        F.sum(
            F.when(
                F.col("evidence_state")
                ==
                "ACTIVE",
                1
            )
            .otherwise(0)
        ).alias(
            "active_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state")
                ==
                "ACTIVE_AND_STALE",
                1
            )
            .otherwise(0)
        ).alias(
            "mixed_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state")
                ==
                "STALE_ONLY",
                1
            )
            .otherwise(0)
        ).alias(
            "stale_only_count"
        ),

        F.sum(
            F.when(
                F.col("evidence_state")
                ==
                "UNKNOWN",
                1
            )
            .otherwise(0)
        ).alias(
            "unknown_count"
        )
    )

    .first()
)


ip002_expected_cmdb_without_active = (
    spark.table(
        IP003_INPUT_TABLES[
            "IP002_CMDB_ONLY_BOUNDARY_REVIEW"
        ]
    )
    .count()
)


expected_observed_not_cmdb_total = int(
    ip002_expected_metrics[
        "observed_not_cmdb_total"
    ]
    or
    0
)


expected_observed_not_cmdb_active = int(
    ip002_expected_metrics[
        "active_count"
    ]
    or
    0
)


expected_observed_not_cmdb_mixed = int(
    ip002_expected_metrics[
        "mixed_count"
    ]
    or
    0
)


expected_observed_not_cmdb_stale_only = int(
    ip002_expected_metrics[
        "stale_only_count"
    ]
    or
    0
)


expected_observed_not_cmdb_unknown = int(
    ip002_expected_metrics[
        "unknown_count"
    ]
    or
    0
)


# ---------------------------------------------------------------------------
# 3. Consolidated lifecycle metrics
# ---------------------------------------------------------------------------

lifecycle_metrics = (
    cert_lifecycle

    .agg(
        F.count("*").alias(
            "lifecycle_rows"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        ),

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
            "post_mismatch_rows"
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
            "run_mismatch_rows"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "N"
                )
                &
                (
                    F.col(
                        "observed_source_count_raw"
                    )
                    >
                    0
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "observed_not_cmdb_total"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "N"
                )
                &
                (
                    F.col("evidence_state")
                    ==
                    "ACTIVE"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "observed_not_cmdb_active"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "N"
                )
                &
                (
                    F.col("evidence_state")
                    ==
                    "ACTIVE_AND_STALE"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "observed_not_cmdb_mixed"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "N"
                )
                &
                (
                    F.col("evidence_state")
                    ==
                    "STALE_ONLY"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "observed_not_cmdb_stale_only"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "N"
                )
                &
                (
                    F.col("evidence_state")
                    ==
                    "UNKNOWN"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "observed_not_cmdb_unknown"
        ),

        F.sum(
            F.when(
                (
                    F.col("cmdb_present") == "Y"
                )
                &
                (
                    F.col(
                        "observed_source_count_active"
                    )
                    ==
                    0
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "cmdb_without_active_evidence"
        ),

        F.sum(
            F.when(
                F.col("cmdb_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "cmdb_rows"
        ),

        F.sum(
            F.when(
                F.col("nmap_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "nmap_rows"
        ),

        F.sum(
            F.when(
                F.col("tenable_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "tenable_rows"
        ),

        F.sum(
            F.when(
                F.col("intune_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "intune_rows"
        ),

        F.sum(
            F.when(
                F.col("mde_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "mde_rows"
        ),

        F.sum(
            F.when(
                F.col("qualys_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "qualys_rows"
        )
    )

    .first()
)


source_metrics = (
    cert_source_lifecycle

    .agg(
        F.count("*").alias(
            "source_observation_rows"
        ),

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
            "post_mismatch_rows"
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
            "run_mismatch_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "source_freshness_status"
                ).isin(
                    "CURRENT",
                    "CURRENT_BY_TIMESTAMP"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "current_source_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "source_freshness_status"
                ).isin(
                    "STALE",
                    "STALE_BY_TIMESTAMP"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "stale_source_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "source_freshness_status"
                )
                ==
                "UNKNOWN",
                1
            )
            .otherwise(0)
        ).alias(
            "unknown_source_rows"
        )
    )

    .first()
)


lifecycle_row_count = int(
    lifecycle_metrics[
        "lifecycle_rows"
    ]
    or
    0
)


lifecycle_distinct_key_count = int(
    lifecycle_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)


observed_not_cmdb_total = int(
    lifecycle_metrics[
        "observed_not_cmdb_total"
    ]
    or
    0
)


observed_not_cmdb_active = int(
    lifecycle_metrics[
        "observed_not_cmdb_active"
    ]
    or
    0
)


observed_not_cmdb_mixed = int(
    lifecycle_metrics[
        "observed_not_cmdb_mixed"
    ]
    or
    0
)


observed_not_cmdb_stale_only = int(
    lifecycle_metrics[
        "observed_not_cmdb_stale_only"
    ]
    or
    0
)


observed_not_cmdb_unknown = int(
    lifecycle_metrics[
        "observed_not_cmdb_unknown"
    ]
    or
    0
)


cmdb_without_active_evidence = int(
    lifecycle_metrics[
        "cmdb_without_active_evidence"
    ]
    or
    0
)


source_observation_row_count = int(
    source_metrics[
        "source_observation_rows"
    ]
    or
    0
)


current_source_row_count = int(
    source_metrics[
        "current_source_rows"
    ]
    or
    0
)


stale_source_row_count = int(
    source_metrics[
        "stale_source_rows"
    ]
    or
    0
)


unknown_source_row_count = int(
    source_metrics[
        "unknown_source_rows"
    ]
    or
    0
)


expected_source_observation_count = sum([
    int(
        lifecycle_metrics[
            "cmdb_rows"
        ]
        or
        0
    ),

    int(
        lifecycle_metrics[
            "nmap_rows"
        ]
        or
        0
    ),

    int(
        lifecycle_metrics[
            "tenable_rows"
        ]
        or
        0
    ),

    int(
        lifecycle_metrics[
            "intune_rows"
        ]
        or
        0
    ),

    int(
        lifecycle_metrics[
            "mde_rows"
        ]
        or
        0
    ),

    int(
        lifecycle_metrics[
            "qualys_rows"
        ]
        or
        0
    )
])


# ---------------------------------------------------------------------------
# 4. Saved work-package counts
# ---------------------------------------------------------------------------

stale_over_90_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_STALE_OVER_90"
        ]
    )
    .count()
)


stale_mde_only_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_STALE_MDE_ONLY"
        ]
    )
    .count()
)


stale_multi_source_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_STALE_MULTI_SOURCE"
        ]
    )
    .count()
)


unknown_freshness_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_UNKNOWN_FRESHNESS"
        ]
    )
    .count()
)


post_validation_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_POST_VALIDATION_TABLE"
        ]
    )
    .count()
)


enterprise_cleanup_count = (
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_ENT_CLEANUP_TABLE"
        ]
    )
    .count()
)


# ---------------------------------------------------------------------------
# 5. Durable-output checks
# ---------------------------------------------------------------------------

missing_output_tables = [
    physical_name
    for physical_name
    in IP003_OUTPUT_TABLES.values()
    if not spark.catalog.tableExists(
        physical_name
    )
]


cross_post_output_bindings = [
    physical_name
    for physical_name
    in IP003_OUTPUT_TABLES.values()
    if not physical_name.lower().startswith(
        expected_post_table_prefix
    )
]


foundation_post_mismatch_count = (
    int(
        lifecycle_metrics[
            "post_mismatch_rows"
        ]
        or
        0
    )
    +
    int(
        source_metrics[
            "post_mismatch_rows"
        ]
        or
        0
    )
)


foundation_run_mismatch_count = (
    int(
        lifecycle_metrics[
            "run_mismatch_rows"
        ]
        or
        0
    )
    +
    int(
        source_metrics[
            "run_mismatch_rows"
        ]
        or
        0
    )
)


observed_not_cmdb_state_total = (
    observed_not_cmdb_active
    +
    observed_not_cmdb_mixed
    +
    observed_not_cmdb_stale_only
    +
    observed_not_cmdb_unknown
)


source_freshness_total = (
    current_source_row_count
    +
    stale_source_row_count
    +
    unknown_source_row_count
)


# ---------------------------------------------------------------------------
# 6. Certification checks
# ---------------------------------------------------------------------------

certification_checks = [
    (
        "Lifecycle rollup preserves Asset Plane population",
        str(expected_asset_plane_count),
        str(lifecycle_row_count),
        lifecycle_row_count
        ==
        expected_asset_plane_count
    ),

    (
        "Lifecycle rollup is one row per asset_key",
        str(lifecycle_row_count),
        str(lifecycle_distinct_key_count),
        lifecycle_row_count
        ==
        lifecycle_distinct_key_count
    ),

    (
        "Source lifecycle observation accounting",
        str(expected_source_observation_count),
        str(source_observation_row_count),
        source_observation_row_count
        ==
        expected_source_observation_count
    ),

    (
        "Source freshness-state accounting",
        str(source_observation_row_count),
        str(source_freshness_total),
        source_observation_row_count
        ==
        source_freshness_total
    ),

    (
        "Observed-not-CMDB parity with IP-002",
        str(expected_observed_not_cmdb_total),
        str(observed_not_cmdb_total),
        observed_not_cmdb_total
        ==
        expected_observed_not_cmdb_total
    ),

    (
        "Observed-not-CMDB evidence-state accounting",
        str(observed_not_cmdb_total),
        str(observed_not_cmdb_state_total),
        observed_not_cmdb_total
        ==
        observed_not_cmdb_state_total
    ),

    (
        "IP-002 active-only lifecycle parity",
        str(expected_observed_not_cmdb_active),
        str(observed_not_cmdb_active),
        observed_not_cmdb_active
        ==
        expected_observed_not_cmdb_active
    ),

    (
        "IP-002 active-and-stale lifecycle parity",
        str(expected_observed_not_cmdb_mixed),
        str(observed_not_cmdb_mixed),
        observed_not_cmdb_mixed
        ==
        expected_observed_not_cmdb_mixed
    ),

    (
        "IP-002 stale-only lifecycle parity",
        str(expected_observed_not_cmdb_stale_only),
        str(observed_not_cmdb_stale_only),
        observed_not_cmdb_stale_only
        ==
        expected_observed_not_cmdb_stale_only
    ),

    (
        "IP-002 unknown-freshness lifecycle parity",
        str(expected_observed_not_cmdb_unknown),
        str(observed_not_cmdb_unknown),
        observed_not_cmdb_unknown
        ==
        expected_observed_not_cmdb_unknown
    ),

    (
        "CMDB-without-active-evidence parity",
        str(ip002_expected_cmdb_without_active),
        str(cmdb_without_active_evidence),
        cmdb_without_active_evidence
        ==
        ip002_expected_cmdb_without_active
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
        "Cross-Post foundation rows",
        "0",
        str(foundation_post_mismatch_count),
        foundation_post_mismatch_count == 0
    ),

    (
        "Cross-run foundation rows",
        "0",
        str(foundation_run_mismatch_count),
        foundation_run_mismatch_count == 0
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


ip003_certificate_df = (
    spark.createDataFrame(
        certification_checks,
        schema=certificate_schema
    )
)


failed_checks = [
    check
    for check in certification_checks
    if not check[3]
]


RCAIP_IP003_CERTIFIED = (
    not failed_checks
)


RCAIP_IP003_READY = (
    RCAIP_IP003_CERTIFIED
)


# ---------------------------------------------------------------------------
# 7. Final runtime contract
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP IP-003 - FINAL RUNTIME CONTRACT")
print("=" * 100)

print(
    "Status:",
    (
        "PASS"
        if RCAIP_IP003_CERTIFIED
        else
        "FAIL"
    )
)

print(
    "Ready:",
    RCAIP_IP003_READY
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
    "Lifecycle rollup rows:",
    lifecycle_row_count
)

print(
    "Source lifecycle observations:",
    source_observation_row_count
)

print(
    "  Current source records:",
    current_source_row_count
)

print(
    "  Stale source records:",
    stale_source_row_count
)

print(
    "  Unknown-freshness source records:",
    unknown_source_row_count
)

print(
    "Observed-not-CMDB total:",
    observed_not_cmdb_total
)

print(
    "  Active only:",
    observed_not_cmdb_active
)

print(
    "  Active and stale:",
    observed_not_cmdb_mixed
)

print(
    "  Stale only:",
    observed_not_cmdb_stale_only
)

print(
    "  Unknown freshness:",
    observed_not_cmdb_unknown
)

print(
    "CMDB identities with no active evidence:",
    cmdb_without_active_evidence
)

print(
    "Assets stale over 90 days:",
    stale_over_90_count
)

print(
    "Stale MDE-only identities:",
    stale_mde_only_count
)

print(
    "Stale multi-source identities:",
    stale_multi_source_count
)

print(
    "Unknown-freshness identities:",
    unknown_freshness_count
)

print(
    "Post lifecycle validation rows:",
    post_validation_count
)

print(
    "Enterprise lifecycle cleanup rows:",
    enterprise_cleanup_count
)

print(
    "Durable tables written:",
    len(IP003_OUTPUT_TABLES)
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
        foundation_post_mismatch_count
    )
)

print(
    "Automatic retirement performed: 0"
)

print(
    "Automatic deletion performed: 0"
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
    ip003_certificate_df

    .orderBy(
        F.col(
            "check_passed"
        ).asc(),

        "certification_check"
    )
)


display(
    cert_lifecycle

    .groupBy(
        "lifecycle_state",
        "review_priority",
        "validation_responsibility"
    )

    .agg(
        F.count("*").alias(
            "asset_count"
        )
    )

    .orderBy(
        F.desc("asset_count")
    )
)


display(
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_SUMMARY_BY_SOURCE_STATUS"
        ]
    )

    .orderBy(
        "observed_source",
        "source_freshness_status"
    )
)


display(
    spark.table(
        IP003_OUTPUT_TABLES[
            "IP003_SUMMARY_BY_STALE_CATEGORY"
        ]
    )

    .orderBy(
        F.desc("asset_count")
    )
)


if failed_checks:
    raise RuntimeError(
        "IP-003 certification failed: "
        +
        " | ".join(
            check[0]
            for check in failed_checks
        )
    )