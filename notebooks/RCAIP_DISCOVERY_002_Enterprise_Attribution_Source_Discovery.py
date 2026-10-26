# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 002 - ENTERPRISE ATTRIBUTION SOURCE DISCOVERY
# Cell 2 - Certified candidate foundation and metadata-only source discovery
#
# Purpose:
#   - Load the current Post's IP-005 enterprise-attribution candidate queue.
#   - Preserve exactly one row per candidate asset.
#   - Extract attribution signals already materialized in RCAIP products.
#   - Inspect enterprise-source availability through metadata only.
#   - Treat Go Manager and network corroboration as optional.
#
# Safety:
#   - No full enterprise source scans.
#   - No automatic Post attribution.
#   - No CMDB, MDE, Intune, Go Manager, or network changes.
#   - Shared Discovery 002 tables retain other Posts and replace only the
#     current Post's prior rows.
# ============================================================================

import re

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)


# ---------------------------------------------------------------------------
# 1. Validate runtime
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
    name
    for name in required_runtime_objects
    if name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "Discovery 002 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready runtime."
    )


# ---------------------------------------------------------------------------
# 2. Resolve current Post inputs
# ---------------------------------------------------------------------------

def resolve_registered_or_fallback(
    logical_names,
    fallback_stem,
    require_exists=True
):
    attempted = []

    for logical_name in logical_names:
        try:
            table_name = get_rcaip_table(
                logical_name,
                require_exists=False
            )
        except TypeError:
            try:
                table_name = get_rcaip_table(logical_name)
            except Exception:
                table_name = None
        except Exception:
            table_name = None

        if table_name:
            attempted.append(table_name)

            if spark.catalog.tableExists(table_name):
                return table_name

    fallback_name = (
        f"{RCAIP_RESULTS_NAMESPACE}."
        f"{RCAIP_POST_SLUG}_{fallback_stem}"
    )

    attempted.append(fallback_name)

    if spark.catalog.tableExists(fallback_name):
        return fallback_name

    if require_exists:
        raise RuntimeError(
            "Required Discovery 002 input was not found. Attempted: "
            + " | ".join(attempted)
        )

    return fallback_name


CANDIDATE_TABLE = resolve_registered_or_fallback(
    [
        "ENTERPRISE_ATTRIBUTION_CANDIDATE_QUEUE",
        "IP005_ENTERPRISE_ATTRIBUTION_QUEUE"
    ],
    "enterprise_attribution_candidate_queue",
    require_exists=True
)

OPERATIONAL_VIEW_TABLE = resolve_registered_or_fallback(
    [
        "OPERATIONAL_ASSET_VIEW",
        "IP005_OPERATIONAL_ASSET_VIEW"
    ],
    "operational_asset_view",
    require_exists=True
)

ASSET_PLANE_TABLE = resolve_registered_or_fallback(
    [
        "ASSET_RECONCILIATION_SCORED"
    ],
    "asset_reconciliation_scored",
    require_exists=True
)


# ---------------------------------------------------------------------------
# 3. Shared output contract
# ---------------------------------------------------------------------------

DISCOVERY002_OUTPUTS = {
    "SOURCE_AVAILABILITY":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_source_availability",

    "CANDIDATE_FOUNDATION":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_candidate_foundation",

    "CANDIDATE_SOURCE_SIGNALS":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_candidate_source_signals",

    "CANDIDATE_ATTRIBUTION_EVIDENCE":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_candidate_attribution_evidence",

    "ATTRIBUTION_VALUE_PROFILE":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_attribution_value_profile",

    "ATTRIBUTION_CROSSWALK_SEED":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_attribution_crosswalk_seed",

    "ATTRIBUTION_ACTION_QUEUE":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_attribution_action_queue",

    "FINAL_DISPOSITION":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_enterprise_attribution_disposition",

    "SOURCE_GAPS":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_enterprise_attribution_source_gaps",

    "CLOSURE_CERTIFICATE":
        f"{RCAIP_RESULTS_NAMESPACE}."
        "rcaip_discovery_002_closure_certificate"
}


def sql_literal(value):
    return str(value).replace("'", "''")


def add_discovery_runtime_columns(dataframe):
    output = dataframe

    runtime_values = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "post_key": RCAIP_POST_SLUG,
        "runtime_post": RCAIP_POST_NAME,
        "runtime_post_slug": RCAIP_POST_SLUG,
        "country_name": RCAIP_COUNTRY_NAME,
        "rcaip_run_id": RCAIP_RUN_ID,
        "run_mode": RCAIP_RUN_MODE
    }

    for column_name, value in runtime_values.items():
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


DISCOVERY002_WRITTEN_TABLES = []


def write_shared_latest(
    dataframe,
    output_key
):
    table_name = DISCOVERY002_OUTPUTS[output_key]
    output = add_discovery_runtime_columns(dataframe)

    if spark.catalog.tableExists(table_name):
        existing_dataframe = spark.table(
            table_name
        )

        existing_lookup = {
            name.lower(): name
            for name in existing_dataframe.columns
        }

        filter_column = None

        for candidate in [
            "post_slug",
            "post_key",
            "runtime_post_slug",
            "post_name",
            "runtime_post"
        ]:
            if candidate in existing_lookup:
                filter_column = existing_lookup[candidate]
                break

        if filter_column is None:
            print(
                "Migrating legacy unscoped Discovery 002 table:",
                table_name
            )

            (
                output.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
            )

        else:
            runtime_tokens = sorted({
                str(RCAIP_POST_SLUG).lower(),
                str(RCAIP_POST_NAME).lower(),
                str(
                    globals().get(
                        "RCAIP_POST_CODE",
                        ""
                    )
                ).lower()
            } - {""})

            token_sql = ", ".join([
                "'" + sql_literal(value) + "'"
                for value in runtime_tokens
            ])

            spark.sql(
                f"DELETE FROM {table_name} "
                f"WHERE lower(trim(cast(`{filter_column}` as string))) "
                f"IN ({token_sql})"
            )

            existing_types = {
                field.name.lower(): field.dataType
                for field in existing_dataframe.schema.fields
            }

            aligned_output = output

            for output_column in output.columns:
                existing_type = existing_types.get(
                    output_column.lower()
                )

                if existing_type is not None:
                    aligned_output = (
                        aligned_output
                        .withColumn(
                            output_column,
                            F.col(
                                f"`{output_column}`"
                            ).cast(
                                existing_type
                            )
                        )
                    )

            (
                aligned_output.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_name)
            )

    else:
        (
            output.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    DISCOVERY002_WRITTEN_TABLES.append(table_name)

    print("Saved:", table_name)

    return table_name


# ---------------------------------------------------------------------------
# 4. Candidate helpers
# ---------------------------------------------------------------------------

def first_existing_column(
    dataframe,
    candidates
):
    lookup = {
        name.lower(): name
        for name in dataframe.columns
    }

    for candidate in candidates:
        resolved = lookup.get(candidate.lower())

        if resolved is not None:
            return resolved

    return None


def existing_columns(
    dataframe,
    candidates
):
    lookup = {
        name.lower(): name
        for name in dataframe.columns
    }

    return [
        lookup[candidate.lower()]
        for candidate in candidates
        if candidate.lower() in lookup
    ]


def coalesce_existing(
    dataframe,
    candidates,
    data_type="string"
):
    columns = existing_columns(
        dataframe,
        candidates
    )

    if not columns:
        return F.lit(None).cast(data_type)

    return F.coalesce(
        *[
            F.col(f"`{name}`").cast(data_type)
            for name in columns
        ]
    )


def joined_existing_text(
    dataframe,
    candidates
):
    columns = existing_columns(
        dataframe,
        candidates
    )

    if not columns:
        return F.lit("")

    return F.concat_ws(
        " | ",
        *[
            F.coalesce(
                F.col(f"`{name}`").cast("string"),
                F.lit("")
            )
            for name in columns
        ]
    )


def truthy_column(
    dataframe,
    candidates
):
    column_name = first_existing_column(
        dataframe,
        candidates
    )

    if column_name is None:
        return F.lit(False)

    normalized = F.upper(
        F.trim(
            F.col(f"`{column_name}`").cast("string")
        )
    )

    return normalized.isin(
        "Y",
        "YES",
        "TRUE",
        "1",
        "ACTIVE",
        "CURRENT",
        "PRESENT"
    )


# ---------------------------------------------------------------------------
# 5. Load and certify the candidate queue
# ---------------------------------------------------------------------------

candidate_raw = spark.table(CANDIDATE_TABLE)

if "asset_key" not in candidate_raw.columns:
    raise RuntimeError(
        "The IP-005 enterprise-attribution queue lacks asset_key."
    )

candidate_post_column = first_existing_column(
    candidate_raw,
    [
        "post_slug",
        "post_key",
        "post_code",
        "post_name"
    ]
)

candidate_run_column = first_existing_column(
    candidate_raw,
    [
        "rcaip_run_id",
        "run_id"
    ]
)

candidate_metrics_expressions = [
    F.count("*").alias("row_count"),
    F.countDistinct("asset_key").alias(
        "distinct_asset_keys"
    ),
    F.sum(
        F.when(
            F.col("asset_key").isNull()
            |
            (
                F.trim(
                    F.col("asset_key").cast("string")
                )
                ==
                ""
            ),
            1
        ).otherwise(0)
    ).alias("blank_asset_keys")
]

if candidate_post_column is not None:
    runtime_post_tokens = [
        str(RCAIP_POST_SLUG).lower(),
        str(RCAIP_POST_NAME).lower()
    ]

    runtime_post_code = globals().get(
        "RCAIP_POST_CODE"
    )

    if runtime_post_code:
        runtime_post_tokens.append(
            str(runtime_post_code).lower()
        )

    candidate_metrics_expressions.append(
        F.sum(
            F.when(
                ~F.lower(
                    F.trim(
                        F.col(
                            f"`{candidate_post_column}`"
                        ).cast("string")
                    )
                ).isin(runtime_post_tokens),
                1
            ).otherwise(0)
        ).alias("cross_post_rows")
    )
else:
    candidate_metrics_expressions.append(
        F.lit(0).cast("long").alias(
            "cross_post_rows"
        )
    )

if candidate_run_column is not None:
    candidate_metrics_expressions.append(
        F.sum(
            F.when(
                F.col(
                    f"`{candidate_run_column}`"
                ).cast("string")
                !=
                F.lit(RCAIP_RUN_ID),
                1
            ).otherwise(0)
        ).alias("cross_run_rows")
    )
else:
    candidate_metrics_expressions.append(
        F.lit(0).cast("long").alias(
            "cross_run_rows"
        )
    )

candidate_metrics = (
    candidate_raw
    .agg(*candidate_metrics_expressions)
    .first()
)

candidate_count = int(
    candidate_metrics["row_count"] or 0
)

candidate_distinct_count = int(
    candidate_metrics["distinct_asset_keys"] or 0
)

candidate_blank_count = int(
    candidate_metrics["blank_asset_keys"] or 0
)

candidate_cross_post_count = int(
    candidate_metrics["cross_post_rows"] or 0
)

candidate_cross_run_count = int(
    candidate_metrics["cross_run_rows"] or 0
)

if candidate_count != candidate_distinct_count:
    raise RuntimeError(
        "The IP-005 enterprise-attribution queue is not one row per "
        f"asset_key. Rows={candidate_count}; "
        f"distinct={candidate_distinct_count}."
    )

if candidate_blank_count != 0:
    raise RuntimeError(
        "The IP-005 enterprise-attribution queue contains blank asset keys."
    )

if candidate_cross_post_count != 0:
    raise RuntimeError(
        "The IP-005 enterprise-attribution queue contains cross-Post rows: "
        + str(candidate_cross_post_count)
    )

if candidate_cross_run_count != 0:
    raise RuntimeError(
        "The IP-005 enterprise-attribution queue contains cross-run rows: "
        + str(candidate_cross_run_count)
    )


# ---------------------------------------------------------------------------
# 6. Standardize the compact candidate foundation
# ---------------------------------------------------------------------------

active_source_count_expression = coalesce_existing(
    candidate_raw,
    [
        "observed_source_count_active",
        "lifecycle__observed_source_count_active"
    ],
    "integer"
)

if first_existing_column(
    candidate_raw,
    [
        "observed_source_count_active",
        "lifecycle__observed_source_count_active"
    ]
) is None:
    active_source_count_expression = (
        truthy_column(
            candidate_raw,
            ["nmap_active"]
        ).cast("integer")
        +
        truthy_column(
            candidate_raw,
            ["tenable_active"]
        ).cast("integer")
        +
        truthy_column(
            candidate_raw,
            ["intune_active"]
        ).cast("integer")
        +
        truthy_column(
            candidate_raw,
            ["mde_active"]
        ).cast("integer")
        +
        truthy_column(
            candidate_raw,
            ["qualys_active"]
        ).cast("integer")
    )

candidate_foundation = (
    candidate_raw

    .withColumn(
        "asset_key",
        F.trim(
            F.col("asset_key").cast("string")
        )
    )

    .withColumn(
        "candidate_asset_name",
        coalesce_existing(
            candidate_raw,
            [
                "lifecycle__asset_name",
                "lifecycle__device_name",
                "asset_name",
                "device_name",
                "mde_device_name",
                "intune_device_name",
                "qualys_asset_name",
                "nmap_hostname",
                "tenable_dns_name",
                "host_key",
                "asset_key"
            ]
        )
    )

    .withColumn(
        "candidate_observed_names_text",
        joined_existing_text(
            candidate_raw,
            [
                "lifecycle__observed_names",
                "observed_names",
                "mde_device_name",
                "intune_device_name",
                "qualys_asset_name",
                "nmap_hostname",
                "tenable_dns_name",
                "host_key"
            ]
        )
    )

    .withColumn(
        "candidate_observed_ips_text",
        joined_existing_text(
            candidate_raw,
            [
                "lifecycle__observed_ips",
                "observed_ips",
                "mde_public_ip",
                "mde_ip",
                "qualys_ip",
                "nmap_ip",
                "tenable_ip"
            ]
        )
    )

    .withColumn(
        "candidate_observed_serials_text",
        joined_existing_text(
            candidate_raw,
            [
                "lifecycle__observed_serials",
                "observed_serials",
                "intune_serial_number",
                "qualys_bios_serial_number"
            ]
        )
    )

    .withColumn(
        "active_source_count",
        F.coalesce(
            active_source_count_expression,
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "identity_evidence_present",
        (
            F.length(
                F.trim(
                    F.col(
                        "candidate_observed_names_text"
                    )
                )
            )
            >
            0
        )
        |
        (
            F.length(
                F.trim(
                    F.col(
                        "candidate_observed_ips_text"
                    )
                )
            )
            >
            0
        )
        |
        (
            F.length(
                F.trim(
                    F.col(
                        "candidate_observed_serials_text"
                    )
                )
            )
            >
            0
        )
    )

    .withColumn(
        "candidate_scope",
        F.lit(
            "CURRENT_OBSERVED_NOT_CMDB_WITHOUT_CONFIRMED_POST_ATTRIBUTION"
        )
    )

    .withColumn(
        "automatic_assignment_allowed",
        F.lit(False)
    )

    .withColumn(
        "post_name",
        F.lit(RCAIP_POST_NAME)
    )

    .withColumn(
        "post_slug",
        F.lit(RCAIP_POST_SLUG)
    )

    .withColumn(
        "post_key",
        F.lit(RCAIP_POST_SLUG)
    )

    .withColumn(
        "country_name",
        F.lit(RCAIP_COUNTRY_NAME)
    )

    .withColumn(
        "rcaip_run_id",
        F.lit(RCAIP_RUN_ID)
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)

write_shared_latest(
    candidate_foundation,
    "CANDIDATE_FOUNDATION"
)


# ---------------------------------------------------------------------------
# 7. Extract materialized source-attribution signals
#
# These are values already present in the scored Asset Plane / IP-005
# operational view. No raw enterprise table is queried.
# ---------------------------------------------------------------------------

SIGNAL_FIELD_CANDIDATES = [
    ("MDE", "MACHINE_GROUP", [
        "mde_machine_group"
    ]),

    ("MDE", "SITE", [
        "mde_site",
        "mde_site_name"
    ]),

    ("MDE", "DYNAMIC_TAGS", [
        "mde_dynamic_tags"
    ]),

    ("MDE", "MANUAL_TAGS", [
        "mde_manual_tags"
    ]),

    ("INTUNE", "SCOPE_TAGS", [
        "intune_scope_tags",
        "intune_role_scope_tags"
    ]),

    ("INTUNE", "ENROLLMENT_PROFILE", [
        "intune_enrollment_profile_name",
        "intune_profile_name"
    ]),

    ("INTUNE", "PRIMARY_USER", [
        "intune_user_principal_name",
        "intune_primary_user"
    ]),

    ("QUALYS", "SCOPE", [
        "qualys_scope"
    ]),

    ("QUALYS", "TAGS", [
        "qualys_tags",
        "qualys_tag_names"
    ]),

    ("NMAP", "ASSET_DOMAIN", [
        "nmap_asset_domain"
    ]),

    ("TENABLE", "REPOSITORY_OR_TAG", [
        "tenable_repository",
        "tenable_tags",
        "tenable_tag"
    ]),

    ("LIFECYCLE", "ASSET_DOMAIN", [
        "lifecycle__asset_domain",
        "asset_domain"
    ])
]

signal_frames = []

for (
    source_system,
    signal_type,
    field_candidates
) in SIGNAL_FIELD_CANDIDATES:

    actual_field = first_existing_column(
        candidate_foundation,
        field_candidates
    )

    if actual_field is None:
        continue

    frame = (
        candidate_foundation

        .select(
            "asset_key",
            "candidate_asset_name",
            "post_name",
            "post_slug",
            "post_key",
            "country_name",
            "rcaip_run_id",

            F.lit(source_system).alias(
                "source_system"
            ),

            F.lit(signal_type).alias(
                "signal_type"
            ),

            F.lit(actual_field).alias(
                "source_field"
            ),

            F.col(
                f"`{actual_field}`"
            ).cast("string").alias(
                "raw_signal_value"
            )
        )

        .filter(
            F.col(
                "raw_signal_value"
            ).isNotNull()
            &
            (
                F.length(
                    F.trim(
                        F.col(
                            "raw_signal_value"
                        )
                    )
                )
                >
                0
            )
        )

        .withColumn(
            "normalized_signal_value",

            F.regexp_replace(
                F.regexp_replace(
                    F.lower(
                        F.trim(
                            F.col(
                                "raw_signal_value"
                            )
                        )
                    ),
                    r"""[\[\]\{\}"']""",
                    " "
                ),
                r"\s+",
                " "
            )
        )

        .filter(
            ~F.col(
                "normalized_signal_value"
            ).isin(
                "",
                "null",
                "none",
                "unknown",
                "n/a",
                "na",
                "not available",
                "unassigned",
                "default",
                "all",
                "enterprise",
                "global"
            )
        )

        .withColumn(
            "signal_origin",
            F.lit(
                "MATERIALIZED_RCAIP_EVIDENCE"
            )
        )

        .withColumn(
            "automatic_assignment_allowed",
            F.lit(False)
        )
    )

    signal_frames.append(frame)


signal_schema = StructType([
    StructField("asset_key", StringType(), False),
    StructField("candidate_asset_name", StringType(), True),
    StructField("post_name", StringType(), False),
    StructField("post_slug", StringType(), False),
    StructField("post_key", StringType(), False),
    StructField("country_name", StringType(), True),
    StructField("rcaip_run_id", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("signal_type", StringType(), False),
    StructField("source_field", StringType(), False),
    StructField("raw_signal_value", StringType(), True),
    StructField("normalized_signal_value", StringType(), True),
    StructField("signal_origin", StringType(), False),
    StructField("automatic_assignment_allowed", BooleanType(), False)
])

if signal_frames:
    candidate_source_signals = signal_frames[0]

    for frame in signal_frames[1:]:
        candidate_source_signals = (
            candidate_source_signals
            .unionByName(
                frame,
                allowMissingColumns=True
            )
        )

    candidate_source_signals = (
        candidate_source_signals
        .dropDuplicates([
            "asset_key",
            "source_system",
            "signal_type",
            "normalized_signal_value"
        ])
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

else:
    candidate_source_signals = (
        spark.createDataFrame(
            [],
            signal_schema
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

candidate_signal_count = (
    candidate_source_signals.count()
)

write_shared_latest(
    candidate_source_signals,
    "CANDIDATE_SOURCE_SIGNALS"
)


# ---------------------------------------------------------------------------
# 8. Metadata-only source availability
# ---------------------------------------------------------------------------

def resolve_optional_registry_source(
    logical_names
):
    for logical_name in logical_names:
        try:
            table_name = get_rcaip_table(
                logical_name,
                require_exists=False
            )
        except TypeError:
            try:
                table_name = get_rcaip_table(
                    logical_name
                )
            except Exception:
                table_name = None
        except Exception:
            table_name = None

        if (
            table_name
            and
            spark.catalog.tableExists(table_name)
        ):
            return table_name, logical_name

    return None, None


go_manager_table, go_manager_logical_name = (
    resolve_optional_registry_source([
        "GO_MANAGER_DEVICE_INVENTORY",
        "GO_MANAGER_DEVICES",
        "GOMANAGER_DEVICE_INVENTORY",
        "GOMOBILE_DEVICE_INVENTORY"
    ])
)


source_candidates = [
    {
        "source_system": "RCAIP_ASSET_PLANE",
        "logical_name": "ASSET_RECONCILIATION_SCORED",
        "physical_table_name": ASSET_PLANE_TABLE,
        "required_for_discovery": True,
        "optional_source": False,
        "discovery_mode": "MATERIALIZED_POST_SCOPED",
        "queried_for_candidate_data": True
    },

    {
        "source_system": "RCAIP_OPERATIONAL_VIEW",
        "logical_name": "OPERATIONAL_ASSET_VIEW",
        "physical_table_name": OPERATIONAL_VIEW_TABLE,
        "required_for_discovery": True,
        "optional_source": False,
        "discovery_mode": "MATERIALIZED_POST_SCOPED",
        "queried_for_candidate_data": True
    },

    {
        "source_system": "MDE_DEVICEINFO",
        "logical_name": "MDE_DEVICEINFO_ENRICHED",
        "physical_table_name":
            "spark_catalog.o365."
            "azure_o365_mde_deviceinfo_silver_enriched",
        "required_for_discovery": False,
        "optional_source": True,
        "discovery_mode": "METADATA_ONLY",
        "queried_for_candidate_data": False
    },

    {
        "source_system": "INTUNE_MANAGED_DEVICES",
        "logical_name": "INTUNE_MANAGED_DEVICES",
        "physical_table_name":
            "spark_catalog.daa_ctad."
            "ms_365_intune_mobile_devices",
        "required_for_discovery": False,
        "optional_source": True,
        "discovery_mode": "METADATA_ONLY",
        "queried_for_candidate_data": False
    },

    {
        "source_system": "CMDB_BOUNDARIES",
        "logical_name": "CMDB_BOUNDARIES_ASSETS",
        "physical_table_name":
            "spark_catalog.purview."
            "cmdb_boundaries_assets",
        "required_for_discovery": False,
        "optional_source": True,
        "discovery_mode": "METADATA_ONLY",
        "queried_for_candidate_data": False
    },

    {
        "source_system": "GO_MANAGER",
        "logical_name":
            go_manager_logical_name
            or
            "GO_MANAGER_OPTIONAL",
        "physical_table_name": go_manager_table,
        "required_for_discovery": False,
        "optional_source": True,
        "discovery_mode": "OPTIONAL_METADATA_ONLY",
        "queried_for_candidate_data": False
    },

    {
        "source_system": "MDE_NETWORKINFO",
        "logical_name": "NETWORKINFO_CORROBORATION",
        "physical_table_name": None,
        "required_for_discovery": False,
        "optional_source": True,
        "discovery_mode":
            "DEFERRED_UNTIL_BOUNDED_ADAPTER_APPROVED",
        "queried_for_candidate_data": False
    }
]


source_availability_rows = []

for source_config in source_candidates:
    physical_table_name = source_config[
        "physical_table_name"
    ]

    table_exists = bool(
        physical_table_name
        and
        spark.catalog.tableExists(
            physical_table_name
        )
    )

    column_count = None
    metadata_error = None

    if table_exists:
        try:
            column_count = len(
                spark.table(
                    physical_table_name
                ).columns
            )
        except Exception as exc:
            metadata_error = (
                f"{type(exc).__name__}: {str(exc)[:500]}"
            )

    if (
        source_config["source_system"]
        ==
        "MDE_NETWORKINFO"
    ):
        availability_status = (
            "DEFERRED_NOT_QUERIED"
        )

    elif table_exists:
        availability_status = (
            "AVAILABLE"
        )

    elif source_config["optional_source"]:
        availability_status = (
            "OPTIONAL_NOT_AVAILABLE"
        )

    else:
        availability_status = (
            "REQUIRED_NOT_AVAILABLE"
        )

    source_availability_rows.append((
        source_config["source_system"],
        source_config["logical_name"],
        physical_table_name,
        availability_status,
        bool(
            source_config[
                "required_for_discovery"
            ]
        ),
        bool(
            source_config[
                "optional_source"
            ]
        ),
        source_config["discovery_mode"],
        bool(
            source_config[
                "queried_for_candidate_data"
            ]
        ),
        column_count,
        metadata_error
    ))


source_availability_schema = StructType([
    StructField("source_system", StringType(), False),
    StructField("logical_name", StringType(), True),
    StructField("physical_table_name", StringType(), True),
    StructField("availability_status", StringType(), False),
    StructField("required_for_discovery", BooleanType(), False),
    StructField("optional_source", BooleanType(), False),
    StructField("discovery_mode", StringType(), False),
    StructField("queried_for_candidate_data", BooleanType(), False),
    StructField("column_count", IntegerType(), True),
    StructField("metadata_error", StringType(), True)
])


source_availability = (
    spark.createDataFrame(
        source_availability_rows,
        source_availability_schema
    )

    .withColumn(
        "post_name",
        F.lit(RCAIP_POST_NAME)
    )

    .withColumn(
        "post_slug",
        F.lit(RCAIP_POST_SLUG)
    )

    .withColumn(
        "post_key",
        F.lit(RCAIP_POST_SLUG)
    )

    .withColumn(
        "country_name",
        F.lit(RCAIP_COUNTRY_NAME)
    )

    .withColumn(
        "rcaip_run_id",
        F.lit(RCAIP_RUN_ID)
    )
)


required_unavailable_count = (
    source_availability
    .filter(
        F.col("required_for_discovery")
        &
        (
            F.col("availability_status")
            !=
            "AVAILABLE"
        )
    )
    .count()
)

if required_unavailable_count != 0:
    raise RuntimeError(
        "A required materialized RCAIP source is unavailable."
    )

write_shared_latest(
    source_availability,
    "SOURCE_AVAILABILITY"
)


# ---------------------------------------------------------------------------
# 9. Cell result
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP DISCOVERY 002 - CANDIDATE FOUNDATION")
print("=" * 100)
print("Status: PASS")
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Enterprise-attribution candidates:", candidate_count)
print("Materialized attribution-signal rows:", candidate_signal_count)
print("Raw enterprise source scans performed: 0")
print(
    "Go Manager source:",
    (
        "AVAILABLE_METADATA_ONLY"
        if go_manager_table
        else
        "OPTIONAL_NOT_AVAILABLE"
    )
)
print(
    "Network corroboration:",
    "DEFERRED_UNTIL_BOUNDED_ADAPTER_APPROVED"
)
print("Foundation tables written:", len(DISCOVERY002_WRITTEN_TABLES))
print("Automatic Post assignments performed: 0")
print("Automatic findings, tickets, or remediation created: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 002 - ENTERPRISE ATTRIBUTION SOURCE DISCOVERY
# Cell 3 - Attribution evidence, crosswalk seeds, and validation routes
#
# Purpose:
#   - Convert materialized source signals into one evidence row per candidate.
#   - Identify consistent, conflicting, single-source, or missing attribution.
#   - Build source-value crosswalk seeds without assigning assets to a Post.
#   - Create one review action per attribution candidate.
#
# Safety:
#   - No raw enterprise source scans.
#   - No automatic Post attribution.
#   - No automatic findings, tickets, or remediation.
# ============================================================================

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# 1. Validate Cell 2 runtime
# ---------------------------------------------------------------------------

required_cell2_objects = [
    "DISCOVERY002_OUTPUTS",
    "write_shared_latest",
    "candidate_count"
]

missing_cell2_objects = [
    name
    for name in required_cell2_objects
    if name not in globals()
]

if missing_cell2_objects:
    raise RuntimeError(
        "Run Discovery 002 Cell 2 before Cell 3. Missing: "
        + " | ".join(missing_cell2_objects)
    )


candidate_foundation = (
    spark.table(
        DISCOVERY002_OUTPUTS[
            "CANDIDATE_FOUNDATION"
        ]
    )

    .filter(
        F.lower(
            F.trim(
                F.col("post_slug")
            )
        )
        ==
        F.lit(
            str(RCAIP_POST_SLUG).lower()
        )
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


candidate_source_signals = (
    spark.table(
        DISCOVERY002_OUTPUTS[
            "CANDIDATE_SOURCE_SIGNALS"
        ]
    )

    .filter(
        F.lower(
            F.trim(
                F.col("post_slug")
            )
        )
        ==
        F.lit(
            str(RCAIP_POST_SLUG).lower()
        )
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 2. Certify one-row-per-candidate foundation
# ---------------------------------------------------------------------------

candidate_foundation_metrics = (
    candidate_foundation

    .agg(
        F.count("*").alias(
            "row_count"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        ),

        F.sum(
            F.when(
                F.col("rcaip_run_id")
                !=
                F.lit(RCAIP_RUN_ID),
                1
            ).otherwise(0)
        ).alias(
            "cross_run_rows"
        )
    )

    .first()
)


foundation_count = int(
    candidate_foundation_metrics[
        "row_count"
    ]
    or
    0
)

foundation_distinct_count = int(
    candidate_foundation_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)

foundation_cross_run_count = int(
    candidate_foundation_metrics[
        "cross_run_rows"
    ]
    or
    0
)


if foundation_count != candidate_count:
    raise RuntimeError(
        "Candidate foundation count changed between Cells 2 and 3. "
        f"Cell 2={candidate_count}; Cell 3={foundation_count}."
    )


if foundation_count != foundation_distinct_count:
    raise RuntimeError(
        "Candidate foundation is not one row per asset_key."
    )


if foundation_cross_run_count != 0:
    raise RuntimeError(
        "Candidate foundation contains cross-run rows: "
        +
        str(
            foundation_cross_run_count
        )
    )


# ---------------------------------------------------------------------------
# 3. Build signal-value support profiles
# ---------------------------------------------------------------------------

signal_value_profile = (
    candidate_source_signals

    .groupBy(
        "source_system",
        "signal_type",
        "normalized_signal_value"
    )

    .agg(
        F.countDistinct(
            "asset_key"
        ).alias(
            "candidate_asset_count"
        ),

        F.count("*").alias(
            "signal_row_count"
        ),

        F.collect_set(
            "source_field"
        ).alias(
            "source_fields"
        ),

        F.sort_array(
            F.collect_set(
                "raw_signal_value"
            )
        ).alias(
            "raw_value_examples"
        )
    )

    .withColumn(
        "crosswalk_seed_status",

        F.when(
            F.col(
                "candidate_asset_count"
            )
            >=
            2,
            "RECURRING_SOURCE_VALUE"
        )

        .otherwise(
            "SINGLE_CANDIDATE_VALUE"
        )
    )

    .withColumn(
        "automatic_crosswalk_allowed",
        F.lit(False)
    )
)


write_shared_latest(
    signal_value_profile,
    "ATTRIBUTION_VALUE_PROFILE"
)


asset_value_support = (
    candidate_source_signals

    .groupBy(
        "asset_key",
        "normalized_signal_value"
    )

    .agg(
        F.countDistinct(
            "source_system"
        ).alias(
            "supporting_source_count"
        ),

        F.count("*").alias(
            "supporting_signal_count"
        ),

        F.sort_array(
            F.collect_set(
                "source_system"
            )
        ).alias(
            "supporting_sources"
        ),

        F.sort_array(
            F.collect_set(
                "signal_type"
            )
        ).alias(
            "supporting_signal_types"
        ),

        F.sort_array(
            F.collect_set(
                "raw_signal_value"
            )
        ).alias(
            "raw_value_examples"
        )
    )
)


dominant_value_window = (
    Window
    .partitionBy(
        "asset_key"
    )
    .orderBy(
        F.desc(
            "supporting_source_count"
        ),
        F.desc(
            "supporting_signal_count"
        ),
        F.asc(
            "normalized_signal_value"
        )
    )
)


dominant_value = (
    asset_value_support

    .withColumn(
        "value_rank",
        F.row_number().over(
            dominant_value_window
        )
    )

    .filter(
        F.col("value_rank") == 1
    )

    .select(
        "asset_key",

        F.col(
            "normalized_signal_value"
        ).alias(
            "proposed_crosswalk_value"
        ),

        F.col(
            "supporting_source_count"
        ).alias(
            "dominant_value_source_count"
        ),

        F.col(
            "supporting_signal_count"
        ).alias(
            "dominant_value_signal_count"
        ),

        F.col(
            "supporting_sources"
        ).alias(
            "dominant_value_sources"
        ),

        F.col(
            "supporting_signal_types"
        ).alias(
            "dominant_value_signal_types"
        ),

        F.col(
            "raw_value_examples"
        ).alias(
            "dominant_value_examples"
        )
    )
)


direct_assignment_signal_types = [
    "MACHINE_GROUP",
    "SITE",
    "SCOPE_TAGS",
    "SCOPE",
    "ASSET_DOMAIN"
]


asset_signal_summary = (
    candidate_source_signals

    .groupBy(
        "asset_key"
    )

    .agg(
        F.count("*").alias(
            "attribution_signal_count"
        ),

        F.countDistinct(
            "source_system"
        ).alias(
            "attribution_source_count"
        ),

        F.countDistinct(
            "normalized_signal_value"
        ).alias(
            "distinct_attribution_value_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "signal_type"
                ).isin(
                    direct_assignment_signal_types
                ),
                1
            ).otherwise(0)
        ).alias(
            "direct_assignment_signal_count"
        ),

        F.sort_array(
            F.collect_set(
                "source_system"
            )
        ).alias(
            "attribution_sources"
        ),

        F.sort_array(
            F.collect_set(
                "signal_type"
            )
        ).alias(
            "attribution_signal_types"
        ),

        F.sort_array(
            F.collect_set(
                "normalized_signal_value"
            )
        ).alias(
            "attribution_values"
        )
    )
)


# ---------------------------------------------------------------------------
# 4. One evidence assessment per candidate
# ---------------------------------------------------------------------------

candidate_attribution_evidence = (
    candidate_foundation

    .join(
        asset_signal_summary,
        on="asset_key",
        how="left"
    )

    .join(
        dominant_value,
        on="asset_key",
        how="left"
    )

    .withColumn(
        "attribution_signal_count",
        F.coalesce(
            F.col(
                "attribution_signal_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "attribution_source_count",
        F.coalesce(
            F.col(
                "attribution_source_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "distinct_attribution_value_count",
        F.coalesce(
            F.col(
                "distinct_attribution_value_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "direct_assignment_signal_count",
        F.coalesce(
            F.col(
                "direct_assignment_signal_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "dominant_value_source_count",
        F.coalesce(
            F.col(
                "dominant_value_source_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "dominant_value_signal_count",
        F.coalesce(
            F.col(
                "dominant_value_signal_count"
            ),
            F.lit(0)
        ).cast("integer")
    )

    .withColumn(
        "attribution_evidence_status",

        F.when(
            F.col(
                "attribution_signal_count"
            )
            ==
            0,
            "NO_USABLE_ATTRIBUTION_SIGNAL"
        )

        .when(
            (
                F.col(
                    "attribution_source_count"
                )
                >=
                2
            )
            &
            (
                F.col(
                    "dominant_value_source_count"
                )
                >=
                2
            )
            &
            (
                F.col(
                    "distinct_attribution_value_count"
                )
                ==
                1
            ),
            "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED"
        )

        .when(
            (
                F.col(
                    "attribution_source_count"
                )
                >=
                2
            )
            &
            (
                F.col(
                    "dominant_value_source_count"
                )
                >=
                2
            ),
            "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE"
        )

        .when(
            F.col(
                "attribution_source_count"
            )
            >=
            2,
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT"
        )

        .when(
            F.col(
                "direct_assignment_signal_count"
            )
            >
            0,
            "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED"
        )

        .otherwise(
            "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE"
        )
    )

    .withColumn(
        "attribution_confidence",

        F.when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED",
            "HIGH"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED",
            "STRONG"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            ).isin(
                "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE",
                "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE"
            ),
            "MODERATE"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT",
            "LOW"
        )

        .otherwise(
            "UNKNOWN"
        )
    )

    .withColumn(
        "route_priority",

        F.when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT",
            "P1"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "NO_USABLE_ATTRIBUTION_SIGNAL",
            "P2"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            ).isin(
                "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED",
                "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE"
            ),
            "P2"
        )

        .otherwise(
            "P3"
        )
    )

    .withColumn(
        "attribution_route",

        F.when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED",
            "ENTERPRISE_CROSSWALK_VALIDATION"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE",
            "PRIMARY_VALUE_AND_VARIANCE_REVIEW"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT",
            "IDENTITY_MERGE_RECENCY_AND_SOURCE_OWNER_REVIEW"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED",
            "SOURCE_VALUE_CROSSWALK_THEN_CORROBORATE"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE",
            "SECOND_SOURCE_CORROBORATION"
        )

        .otherwise(
            "ENTERPRISE_INVENTORY_AND_ATTRIBUTION_LOOKUP"
        )
    )

    .withColumn(
        "recommended_next_action",

        F.when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED",
            F.lit(
                "Validate the recurring source value against an approved "
                "enterprise Post/location crosswalk. Do not assign the asset "
                "automatically."
            )
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE",
            F.lit(
                "Validate the dominant attribution value and explain the "
                "secondary source variance before changing ownership or "
                "location."
            )
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT",
            F.lit(
                "Resolve identity merge, source recency, and conflicting "
                "location or ownership evidence with enterprise source "
                "owners."
            )
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED",
            F.lit(
                "Validate the source value through an approved crosswalk and "
                "obtain a second corroborating source when practical."
            )
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE",
            F.lit(
                "Obtain corroborating CMDB, Go Manager, MDE, Intune, network, "
                "or Post evidence before attribution."
            )
        )

        .otherwise(
            F.lit(
                "Use enterprise inventory, CMDB, Go Manager, source-owner, "
                "or joint Post validation to establish ownership and "
                "location."
            )
        )
    )

    .withColumn(
        "validation_responsibility",

        F.when(
            F.col(
                "route_priority"
            )
            ==
            "P1",
            "Enterprise Identity, Source Owners, and RCAIP Data Governance"
        )

        .when(
            F.col(
                "route_priority"
            )
            ==
            "P2",
            "Enterprise Asset Management with Post Corroboration"
        )

        .otherwise(
            "Enterprise Attribution Crosswalk Owner"
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

    .withColumn(
        "automatic_ticket_allowed",
        F.lit(False)
    )

    .withColumn(
        "automatic_remediation_allowed",
        F.lit(False)
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


evidence_metrics = (
    candidate_attribution_evidence

    .agg(
        F.count("*").alias(
            "row_count"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        )
    )

    .first()
)


evidence_count = int(
    evidence_metrics["row_count"] or 0
)

evidence_distinct_count = int(
    evidence_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)


if (
    evidence_count != foundation_count
    or
    evidence_count != evidence_distinct_count
):
    raise RuntimeError(
        "Attribution evidence did not preserve one row per candidate. "
        f"Foundation={foundation_count}; "
        f"evidence={evidence_count}; "
        f"distinct={evidence_distinct_count}."
    )


write_shared_latest(
    candidate_attribution_evidence,
    "CANDIDATE_ATTRIBUTION_EVIDENCE"
)


# ---------------------------------------------------------------------------
# 5. Crosswalk seed
# ---------------------------------------------------------------------------

attribution_crosswalk_seed = (
    signal_value_profile

    .filter(
        F.col(
            "normalized_signal_value"
        ).isNotNull()
        &
        (
            F.length(
                F.trim(
                    F.col(
                        "normalized_signal_value"
                    )
                )
            )
            >
            0
        )
    )

    .withColumn(
        "crosswalk_priority",

        F.when(
            F.col(
                "candidate_asset_count"
            )
            >=
            5,
            "High"
        )

        .when(
            F.col(
                "candidate_asset_count"
            )
            >=
            2,
            "Medium"
        )

        .otherwise(
            "Review"
        )
    )

    .withColumn(
        "recommended_crosswalk_action",
        F.lit(
            "Map the source value to an approved enterprise Post, bureau, "
            "site, or ownership value; retain provenance and effective dates."
        )
    )

    .withColumn(
        "automatic_crosswalk_allowed",
        F.lit(False)
    )
)


write_shared_latest(
    attribution_crosswalk_seed,
    "ATTRIBUTION_CROSSWALK_SEED"
)


# ---------------------------------------------------------------------------
# 6. One review action per candidate
# ---------------------------------------------------------------------------

attribution_action_queue = (
    candidate_attribution_evidence

    .select(
        F.concat(
            F.lit(
                "DISC002-ATTRIBUTION-"
            ),
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("asset_key"),
                    F.col(
                        "attribution_evidence_status"
                    )
                ),
                256
            )
        ).alias(
            "action_id"
        ),

        F.lit(
            "Enterprise Attribution"
        ).alias(
            "action_domain"
        ),

        F.lit(
            "ATTRIBUTION_VALIDATION"
        ).alias(
            "action_type"
        ),

        F.col(
            "asset_key"
        ).alias(
            "subject_key"
        ),

        F.col(
            "candidate_asset_name"
        ).alias(
            "subject_name"
        ),

        "asset_key",

        F.when(
            F.col("route_priority") == "P1",
            "High"
        )
        .when(
            F.col("route_priority") == "P2",
            "Medium"
        )
        .otherwise(
            "Review"
        ).alias(
            "priority"
        ),

        F.col(
            "attribution_evidence_status"
        ).alias(
            "condition"
        ),

        F.concat(
            F.lit(
                "Attribution confidence: "
            ),
            F.col(
                "attribution_confidence"
            ),
            F.lit(
                "; candidate source count: "
            ),
            F.col(
                "attribution_source_count"
            ).cast("string"),
            F.lit(
                "; distinct source values: "
            ),
            F.col(
                "distinct_attribution_value_count"
            ).cast("string")
        ).alias(
            "interpretation"
        ),

        "attribution_route",
        "recommended_next_action",
        "validation_responsibility",
        "post_name",
        "post_slug",
        "post_key",
        "country_name",
        "rcaip_run_id",

        F.lit(
            "Requires Validation"
        ).alias(
            "action_status"
        ),

        F.lit(
            "DISCOVERY-002"
        ).alias(
            "source_module"
        ),

        F.lit(False).alias(
            "automatic_assignment_allowed"
        ),

        F.lit(False).alias(
            "automatic_finding_allowed"
        ),

        F.lit(False).alias(
            "automatic_ticket_allowed"
        ),

        F.lit(False).alias(
            "automatic_remediation_allowed"
        )
    )

    .dropDuplicates(
        ["action_id"]
    )
)


attribution_action_count = (
    attribution_action_queue.count()
)


if attribution_action_count != foundation_count:
    raise RuntimeError(
        "Discovery 002 must produce one validation action per candidate. "
        f"Candidates={foundation_count}; "
        f"actions={attribution_action_count}."
    )


write_shared_latest(
    attribution_action_queue,
    "ATTRIBUTION_ACTION_QUEUE"
)


# ---------------------------------------------------------------------------
# 7. Cell result
# ---------------------------------------------------------------------------

evidence_summary = (
    candidate_attribution_evidence

    .groupBy(
        "attribution_evidence_status"
    )

    .agg(
        F.count("*").alias(
            "asset_count"
        )
    )

    .orderBy(
        F.desc(
            "asset_count"
        )
    )
)


print("=" * 100)
print("RCAIP DISCOVERY 002 - ATTRIBUTION EVIDENCE")
print("=" * 100)
print("Status: PASS")
print("Candidate assets assessed:", evidence_count)
print("Validation actions created:", attribution_action_count)
print(
    "Crosswalk seed values:",
    attribution_crosswalk_seed.count()
)
print("Raw enterprise source scans performed: 0")
print("Automatic Post assignments performed: 0")
print("Automatic findings, tickets, or remediation created: 0")
print("=" * 100)

display(evidence_summary)

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 002 - ENTERPRISE ATTRIBUTION SOURCE DISCOVERY
# Cell 4 - Final disposition, source-gap register, and closure certification
#
# Purpose:
#   - Publish one final disposition row per enterprise-attribution candidate.
#   - Record enterprise source and governance gaps.
#   - Certify complete candidate accounting.
#
# Safety:
#   - No automatic Post attribution.
#   - No source-system changes.
#   - No automatic findings, tickets, assignments, or remediation.
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)


# ---------------------------------------------------------------------------
# 1. Validate prior-cell products
# ---------------------------------------------------------------------------

required_prior_objects = [
    "DISCOVERY002_OUTPUTS",
    "write_shared_latest",
    "candidate_count"
]

missing_prior_objects = [
    name
    for name in required_prior_objects
    if name not in globals()
]

if missing_prior_objects:
    raise RuntimeError(
        "Run Discovery 002 Cells 2 and 3 before Cell 4. Missing: "
        + " | ".join(missing_prior_objects)
    )


required_input_keys = [
    "CANDIDATE_FOUNDATION",
    "SOURCE_AVAILABILITY",
    "CANDIDATE_ATTRIBUTION_EVIDENCE",
    "ATTRIBUTION_CROSSWALK_SEED",
    "ATTRIBUTION_ACTION_QUEUE"
]

for input_key in required_input_keys:
    input_table = DISCOVERY002_OUTPUTS[
        input_key
    ]

    if not spark.catalog.tableExists(
        input_table
    ):
        raise RuntimeError(
            "Required Discovery 002 product is missing: "
            +
            input_table
        )


def current_post_rows(
    table_name
):
    dataframe = spark.table(
        table_name
    )

    if "post_slug" not in dataframe.columns:
        raise RuntimeError(
            "Shared Discovery 002 product lacks post_slug: "
            +
            table_name
        )

    return dataframe.filter(
        F.lower(
            F.trim(
                F.col("post_slug").cast("string")
            )
        )
        ==
        F.lit(
            str(
                RCAIP_POST_SLUG
            ).lower()
        )
    )



def optional_column(
    dataframe,
    candidates,
    data_type="string"
):
    lookup = {
        name.lower(): name
        for name in dataframe.columns
    }

    for candidate in candidates:
        resolved = lookup.get(
            candidate.lower()
        )

        if resolved is not None:
            return F.col(
                f"`{resolved}`"
            ).cast(data_type)

    return F.lit(None).cast(data_type)


candidate_foundation = current_post_rows(
    DISCOVERY002_OUTPUTS[
        "CANDIDATE_FOUNDATION"
    ]
)

source_availability = current_post_rows(
    DISCOVERY002_OUTPUTS[
        "SOURCE_AVAILABILITY"
    ]
)

candidate_evidence = current_post_rows(
    DISCOVERY002_OUTPUTS[
        "CANDIDATE_ATTRIBUTION_EVIDENCE"
    ]
)

crosswalk_seed = current_post_rows(
    DISCOVERY002_OUTPUTS[
        "ATTRIBUTION_CROSSWALK_SEED"
    ]
)

attribution_actions = current_post_rows(
    DISCOVERY002_OUTPUTS[
        "ATTRIBUTION_ACTION_QUEUE"
    ]
)


# ---------------------------------------------------------------------------
# 2. Final one-row-per-candidate disposition
# ---------------------------------------------------------------------------

final_disposition = (
    candidate_evidence

    .select(
        "asset_key",
        "candidate_asset_name",
        "candidate_scope",
        "active_source_count",
        "identity_evidence_present",

        "attribution_evidence_status",
        "attribution_confidence",
        "attribution_signal_count",
        "attribution_source_count",
        "distinct_attribution_value_count",
        "direct_assignment_signal_count",

        "proposed_crosswalk_value",
        "dominant_value_source_count",
        "dominant_value_signal_count",
        "dominant_value_sources",
        "dominant_value_signal_types",
        "dominant_value_examples",

        "attribution_sources",
        "attribution_signal_types",
        "attribution_values",

        "route_priority",
        F.col(
            "route_priority"
        ).alias(
            "priority_tier"
        ),

        "attribution_route",
        "recommended_next_action",
        F.col(
            "recommended_next_action"
        ).alias(
            "recommended_next_step"
        ),

        "validation_responsibility",

        optional_column(
            candidate_evidence,
            [
                "mde_machine_group"
            ]
        ).alias(
            "mde_machine_group"
        ),

        optional_column(
            candidate_evidence,
            [
                "mde_site",
                "mde_site_name"
            ]
        ).alias(
            "mde_site"
        ),

        F.lit(
            "NOT_RECONCILED"
        ).alias(
            "cmdb_match_status"
        ),

        F.lit(
            "DEFERRED_UNTIL_BOUNDED_ADAPTER_APPROVED"
        ).alias(
            "network_probe_route"
        ),

        "post_name",
        "post_slug",
        "post_key",
        "country_name",
        "rcaip_run_id",

        F.when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED",
            "CROSSWALK_READY_FOR_VALIDATION"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE",
            "PRIMARY_VALUE_REQUIRES_VARIANCE_REVIEW"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "CROSS_SOURCE_ATTRIBUTION_CONFLICT",
            "ENTERPRISE_IDENTITY_AND_SOURCE_CONFLICT_REVIEW"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED",
            "SINGLE_SOURCE_CROSSWALK_AND_CORROBORATION"
        )

        .when(
            F.col(
                "attribution_evidence_status"
            )
            ==
            "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE",
            "SECOND_SOURCE_CORROBORATION_REQUIRED"
        )

        .otherwise(
            "ENTERPRISE_ATTRIBUTION_LOOKUP_REQUIRED"
        ).alias(
            "final_disposition"
        ),

        F.lit(
            "Requires Validation"
        ).alias(
            "disposition_status"
        ),

        F.lit(False).alias(
            "automatic_assignment_allowed"
        ),

        F.lit(False).alias(
            "automatic_finding_allowed"
        ),

        F.lit(False).alias(
            "automatic_ticket_allowed"
        ),

        F.lit(False).alias(
            "automatic_remediation_allowed"
        )
    )

    .withColumn(
        "runtime_disposition",
        F.col(
            "final_disposition"
        )
    )

    .withColumn(
        "disposition",
        F.col(
            "final_disposition"
        )
    )
)


disposition_metrics = (
    final_disposition

    .agg(
        F.count("*").alias(
            "row_count"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        ),

        F.sum(
            F.when(
                F.col(
                    "automatic_assignment_allowed"
                ),
                1
            ).otherwise(0)
        ).alias(
            "automatic_assignment_rows"
        ),

        F.sum(
            F.when(
                F.col("rcaip_run_id")
                !=
                F.lit(RCAIP_RUN_ID),
                1
            ).otherwise(0)
        ).alias(
            "cross_run_rows"
        )
    )

    .first()
)


disposition_count = int(
    disposition_metrics["row_count"] or 0
)

disposition_distinct_count = int(
    disposition_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)

automatic_assignment_count = int(
    disposition_metrics[
        "automatic_assignment_rows"
    ]
    or
    0
)

disposition_cross_run_count = int(
    disposition_metrics[
        "cross_run_rows"
    ]
    or
    0
)


if (
    disposition_count != candidate_count
    or
    disposition_count != disposition_distinct_count
):
    raise RuntimeError(
        "Final attribution disposition did not preserve the candidate "
        "population. "
        f"Candidates={candidate_count}; "
        f"disposition={disposition_count}; "
        f"distinct={disposition_distinct_count}."
    )


if automatic_assignment_count != 0:
    raise RuntimeError(
        "Discovery 002 attempted an automatic attribution assignment."
    )


if disposition_cross_run_count != 0:
    raise RuntimeError(
        "Final disposition contains cross-run rows: "
        +
        str(
            disposition_cross_run_count
        )
    )


write_shared_latest(
    final_disposition,
    "FINAL_DISPOSITION"
)


# ---------------------------------------------------------------------------
# 3. Summary counts for source and governance gaps
# ---------------------------------------------------------------------------

status_counts = {
    row[
        "attribution_evidence_status"
    ]: int(
        row["asset_count"] or 0
    )
    for row in (
        final_disposition

        .groupBy(
            "attribution_evidence_status"
        )

        .agg(
            F.count("*").alias(
                "asset_count"
            )
        )

        .collect()
    )
}


def status_count(
    status
):
    return int(
        status_counts.get(
            status,
            0
        )
    )


no_signal_count = status_count(
    "NO_USABLE_ATTRIBUTION_SIGNAL"
)

consistent_multi_source_count = status_count(
    "CONSISTENT_MULTI_SOURCE_ATTRIBUTION_SEED"
)

multi_source_variance_count = status_count(
    "MULTI_SOURCE_PRIMARY_WITH_SECONDARY_VARIANCE"
)

cross_source_conflict_count = status_count(
    "CROSS_SOURCE_ATTRIBUTION_CONFLICT"
)

strong_single_source_count = status_count(
    "STRONG_SINGLE_SOURCE_ATTRIBUTION_SEED"
)

moderate_single_source_count = status_count(
    "MODERATE_SINGLE_SOURCE_ATTRIBUTION_EVIDENCE"
)

single_source_dependency_count = (
    strong_single_source_count
    +
    moderate_single_source_count
)


source_status_lookup = {
    row["source_system"]:
        row["availability_status"]
    for row in (
        source_availability

        .select(
            "source_system",
            "availability_status"
        )

        .collect()
    )
}


go_manager_status = source_status_lookup.get(
    "GO_MANAGER",
    "OPTIONAL_NOT_AVAILABLE"
)

networkinfo_status = source_status_lookup.get(
    "MDE_NETWORKINFO",
    "DEFERRED_NOT_QUERIED"
)

crosswalk_seed_count = (
    crosswalk_seed.count()
)

action_count = (
    attribution_actions.count()
)


# ---------------------------------------------------------------------------
# 4. Source and governance gap register
# ---------------------------------------------------------------------------

gap_rows = [
    (
        "DISC002-GAP-CMDB-RECONCILIATION",
        "CMDB_RECONCILIATION",
        "CMDB",
        "OPEN",
        candidate_count,
        "Every Discovery 002 candidate has current evidence but is not "
        "presently reconciled to CMDB.",
        "Enterprise Asset Management and CMDB Governance",
        "Validate canonical identity, ownership, location, and the reason "
        "the asset is not represented or matched in CMDB.",
        False
    ),

    (
        "DISC002-GAP-NO-ATTRIBUTION-SIGNAL",
        "ATTRIBUTION_SIGNAL",
        "MULTI_SOURCE",
        (
            "OPEN"
            if no_signal_count > 0
            else
            "NOT_OBSERVED"
        ),
        no_signal_count,
        "Current asset evidence exists, but the materialized sources do not "
        "provide a usable ownership or location signal.",
        "Enterprise Source Owners and Asset Management",
        "Use CMDB, Go Manager, approved source crosswalks, and joint Post "
        "validation to establish ownership and location.",
        False
    ),

    (
        "DISC002-GAP-CROSS-SOURCE-CONFLICT",
        "ATTRIBUTION_CONFLICT",
        "MULTI_SOURCE",
        (
            "OPEN"
            if cross_source_conflict_count > 0
            else
            "NOT_OBSERVED"
        ),
        cross_source_conflict_count,
        "Multiple source systems provide differing attribution values for "
        "the same candidate identity.",
        "Enterprise Identity, Source Owners, and RCAIP Data Governance",
        "Resolve identity merges, source recency, and conflicting source "
        "crosswalk values before attribution.",
        False
    ),

    (
        "DISC002-GAP-SINGLE-SOURCE-DEPENDENCY",
        "SOURCE_CORROBORATION",
        "MULTI_SOURCE",
        (
            "OPEN"
            if single_source_dependency_count > 0
            else
            "NOT_OBSERVED"
        ),
        single_source_dependency_count,
        "Attribution currently depends on only one source system.",
        "Enterprise Asset Management with Post Corroboration",
        "Obtain a second corroborating source when practical before changing "
        "the canonical owner or Post.",
        False
    ),

    (
        "DISC002-GAP-GO-MANAGER",
        "OPTIONAL_SOURCE",
        "GO_MANAGER",
        go_manager_status,
        (
            candidate_count
            if go_manager_status
            !=
            "AVAILABLE"
            else
            0
        ),
        "Go Manager may provide managed-device ownership and location "
        "evidence, but it is optional for this run.",
        "Go Manager Data Owner",
        "Provide a governed table, export, or approved dashboard adapter when "
        "available. Its absence does not invalidate this assessment.",
        True
    ),

    (
        "DISC002-GAP-NETWORK-CORROBORATION",
        "OPTIONAL_SOURCE",
        "MDE_NETWORKINFO",
        networkinfo_status,
        candidate_count,
        "Network corroboration was intentionally not executed because a "
        "bounded, cost-controlled adapter has not been approved.",
        "MDE and Network Data Owners",
        "Implement a DeviceId- or candidate-key-bounded adapter before "
        "querying enterprise NetworkInfo data.",
        True
    ),

    (
        "DISC002-GAP-CROSSWALK-GOVERNANCE",
        "CROSSWALK_GOVERNANCE",
        "ATTRIBUTION_VALUE_CROSSWALK",
        (
            "OPEN"
            if crosswalk_seed_count > 0
            else
            "NO_SEEDS"
        ),
        crosswalk_seed_count,
        "Source values can support attribution only after they are mapped to "
        "approved Post, bureau, site, or ownership values.",
        "Enterprise Asset and Data Governance",
        "Create a versioned crosswalk with source, value, approved mapping, "
        "effective dates, owner, and validation provenance.",
        False
    )
]


gap_schema = StructType([
    StructField("gap_id", StringType(), False),
    StructField("gap_category", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("gap_status", StringType(), False),
    StructField("affected_asset_count", LongType(), False),
    StructField("operational_impact", StringType(), False),
    StructField("recommended_owner", StringType(), False),
    StructField("recommended_action", StringType(), False),
    StructField("optional_source", BooleanType(), False)
])


source_gaps = (
    spark.createDataFrame(
        gap_rows,
        gap_schema
    )

    .withColumn(
        "evidence_summary",
        F.col(
            "operational_impact"
        )
    )

    .withColumn(
        "required_capability",
        F.col(
            "recommended_owner"
        )
    )
)


write_shared_latest(
    source_gaps,
    "SOURCE_GAPS"
)


# ---------------------------------------------------------------------------
# 5. Closure certificate
# ---------------------------------------------------------------------------

closure_schema = StructType([
    StructField("status", StringType(), False),
    StructField("ready", BooleanType(), False),
    StructField("candidate_asset_count", LongType(), False),
    StructField("disposition_asset_count", LongType(), False),
    StructField("action_count", LongType(), False),
    StructField("crosswalk_seed_count", LongType(), False),
    StructField("consistent_multi_source_count", LongType(), False),
    StructField("multi_source_variance_count", LongType(), False),
    StructField("cross_source_conflict_count", LongType(), False),
    StructField("strong_single_source_count", LongType(), False),
    StructField("moderate_single_source_count", LongType(), False),
    StructField("no_usable_signal_count", LongType(), False),
    StructField("go_manager_status", StringType(), False),
    StructField("networkinfo_status", StringType(), False),
    StructField("raw_enterprise_source_scans", LongType(), False),
    StructField("automatic_assignments", LongType(), False),
    StructField("automatic_findings", LongType(), False),
    StructField("automatic_tickets", LongType(), False),
    StructField("automatic_remediation", LongType(), False)
])


closure_certificate = spark.createDataFrame(
    [(
        "PASS",
        True,
        candidate_count,
        disposition_count,
        action_count,
        crosswalk_seed_count,
        consistent_multi_source_count,
        multi_source_variance_count,
        cross_source_conflict_count,
        strong_single_source_count,
        moderate_single_source_count,
        no_signal_count,
        go_manager_status,
        networkinfo_status,
        0,
        0,
        0,
        0,
        0
    )],
    closure_schema
)


write_shared_latest(
    closure_certificate,
    "CLOSURE_CERTIFICATE"
)


# ---------------------------------------------------------------------------
# 6. Final durable-output certification
# ---------------------------------------------------------------------------

expected_output_tables = list(
    DISCOVERY002_OUTPUTS.values()
)

missing_outputs = [
    table_name
    for table_name in expected_output_tables
    if not spark.catalog.tableExists(
        table_name
    )
]


cross_post_output_rows = 0
cross_run_output_rows = 0


for table_name in expected_output_tables:
    if not spark.catalog.tableExists(
        table_name
    ):
        continue

    output_df = spark.table(
        table_name
    )

    if "post_slug" in output_df.columns:
        current_post_df = output_df.filter(
            F.lower(
                F.trim(
                    F.col(
                        "post_slug"
                    ).cast("string")
                )
            )
            ==
            F.lit(
                str(
                    RCAIP_POST_SLUG
                ).lower()
            )
        )

        if "rcaip_run_id" in current_post_df.columns:
            cross_run_output_rows += (
                current_post_df

                .filter(
                    F.col(
                        "rcaip_run_id"
                    ).cast("string")
                    !=
                    F.lit(
                        RCAIP_RUN_ID
                    )
                )

                .count()
            )


if missing_outputs:
    raise RuntimeError(
        "Discovery 002 is missing durable outputs: "
        +
        " | ".join(
            missing_outputs
        )
    )


if cross_post_output_rows != 0:
    raise RuntimeError(
        "Discovery 002 detected cross-Post output rows: "
        +
        str(
            cross_post_output_rows
        )
    )


if cross_run_output_rows != 0:
    raise RuntimeError(
        "Discovery 002 detected cross-run rows for the current Post: "
        +
        str(
            cross_run_output_rows
        )
    )


# ---------------------------------------------------------------------------
# 7. Final runtime contract
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP DISCOVERY 002 - FINAL RUNTIME CONTRACT")
print("=" * 100)
print("Status: PASS")
print("Ready: True")
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Country:", RCAIP_COUNTRY_NAME)
print("Run ID:", RCAIP_RUN_ID)
print("Run mode:", RCAIP_RUN_MODE)
print(
    "Assessment dates:",
    RCAIP_ASSESSMENT_START_DATE_TEXT,
    "to",
    RCAIP_ASSESSMENT_END_DATE_TEXT
)
print("Enterprise-attribution candidates:", candidate_count)
print("Final disposition rows:", disposition_count)
print(
    "  Consistent multi-source seeds:",
    consistent_multi_source_count
)
print(
    "  Multi-source primary with variance:",
    multi_source_variance_count
)
print(
    "  Cross-source conflicts:",
    cross_source_conflict_count
)
print(
    "  Strong single-source seeds:",
    strong_single_source_count
)
print(
    "  Moderate single-source evidence:",
    moderate_single_source_count
)
print(
    "  No usable attribution signal:",
    no_signal_count
)
print("Attribution validation actions:", action_count)
print("Crosswalk seed values:", crosswalk_seed_count)
print("Go Manager source:", go_manager_status)
print("Network corroboration:", networkinfo_status)
print("Raw enterprise source scans performed: 0")
print("Durable outputs:", len(expected_output_tables))
print("Missing durable outputs:", len(missing_outputs))
print("Cross-Post output rows:", cross_post_output_rows)
print("Cross-run output rows:", cross_run_output_rows)
print("Automatic Post assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)