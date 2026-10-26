# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry
# MAGIC

# COMMAND ----------

# ============================================================================
# RCAIP IP-005 - OPERATIONAL VIEW
# Cell 2 - Certified operational asset foundation
#
# Anchor:
#   The certified scored Asset Plane is the operational asset population.
#
# Optional enrichment:
#   Open-port evidence and Go Manager are allowed to be unavailable.
#   Missing optional sources are recorded, not treated as control failures.
#
# Safety:
#   This cell performs no automated finding, ticket, assignment, retirement,
#   deletion, or remediation.
# ============================================================================

from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType
)


# ---------------------------------------------------------------------------
# 1. Runtime contract
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
        "IP-005 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

expected_post_table_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()


# ---------------------------------------------------------------------------
# 2. Table resolution
# ---------------------------------------------------------------------------

def post_table(stem):
    return (
        f"{RCAIP_RESULTS_NAMESPACE}."
        f"{RCAIP_POST_SLUG}_{stem}"
    )


def registered_existing_table(logical_names):
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

        if (
            table_name
            and spark.catalog.tableExists(table_name)
        ):
            return table_name, logical_name

    return None, None


def resolve_required_input(
    label,
    logical_names,
    fallback_stems
):
    table_name, resolution_basis = registered_existing_table(
        logical_names
    )

    if table_name is None:
        for stem in fallback_stems:
            candidate = post_table(stem)

            if spark.catalog.tableExists(candidate):
                table_name = candidate
                resolution_basis = f"POST_FALLBACK:{stem}"
                break

    if table_name is None:
        raise RuntimeError(
            f"Required IP-005 input is unavailable: {label}"
        )

    return {
        "label": label,
        "table_name": table_name,
        "required": True,
        "available": True,
        "resolution_basis": resolution_basis
    }


def resolve_optional_input(
    label,
    logical_names,
    fallback_stems
):
    table_name, resolution_basis = registered_existing_table(
        logical_names
    )

    if table_name is None:
        for stem in fallback_stems:
            candidate = post_table(stem)

            if spark.catalog.tableExists(candidate):
                table_name = candidate
                resolution_basis = f"POST_FALLBACK:{stem}"
                break

    return {
        "label": label,
        "table_name": table_name,
        "required": False,
        "available": table_name is not None,
        "resolution_basis": (
            resolution_basis
            if table_name is not None
            else "OPTIONAL_NOT_AVAILABLE"
        )
    }


def resolve_output_table(
    logical_names,
    fallback_stem
):
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

        if (
            table_name
            and table_name.lower().startswith(
                expected_post_table_prefix
            )
        ):
            return table_name

    return post_table(fallback_stem)


IP005_INPUTS = {
    "ASSET_PLANE": resolve_required_input(
        "Certified scored Asset Plane",
        [
            "ASSET_RECONCILIATION_SCORED"
        ],
        [
            "asset_reconciliation_scored"
        ]
    ),

    "LIFECYCLE": resolve_required_input(
        "IP-003 asset lifecycle rollup",
        [
            "IP003_ASSET_LIFECYCLE_ROLLUP"
        ],
        [
            "ip003_asset_lifecycle_rollup"
        ]
    ),

    "TOOL_COVERAGE": resolve_required_input(
        "IP-002 asset-tool coverage evidence",
        [
            "IP002_TOOL_COVERAGE_REVIEW",
            "TOOL_COVERAGE_REVIEW",
            "SECURITY_TOOL_COVERAGE"
        ],
        [
            "ip002_tool_coverage_review",
            "ip002_security_tool_coverage_review"
        ]
    ),

    "IP004_COMPUTER_REVIEW": resolve_optional_input(
        "IP-004 AD computer hygiene review",
        [
            "IP004_COMPUTER_HYGIENE_REVIEW"
        ],
        [
            "ip004_computer_hygiene_review"
        ]
    ),

    "IP004_COMPUTER_MAP": resolve_optional_input(
        "IP-004 AD computer-to-asset mapping",
        [
            "IP004_COMPUTER_ASSET_MAP"
        ],
        [
            "ip004_computer_asset_map"
        ]
    ),

    "IP004_ACTION_QUEUE": resolve_optional_input(
        "IP-004 identity and AD hygiene action queue",
        [
            "IP004_HYGIENE_ACTION_QUEUE"
        ],
        [
            "ip004_hygiene_action_queue"
        ]
    ),

    "OPEN_PORTS": resolve_optional_input(
        "IP-001 open-port review evidence",
        [
            "IP001_OPEN_PORT_REVIEW",
            "IP001_PRIORITIZED_OPEN_PORTS",
            "OPEN_PORT_REVIEW"
        ],
        [
            "ip001_open_port_review",
            "ip001_prioritized_open_ports",
            "ip001_open_port_observations",
            "open_port_review"
        ]
    ),

    "GO_MANAGER": resolve_optional_input(
        "Go Manager device evidence",
        [
            "GO_MANAGER_DEVICES",
            "GOMANAGER_DEVICES",
            "GO_MANAGER"
        ],
        [
            "go_manager_devices",
            "gomanager_devices",
            "go_manager"
        ]
    )
}


IP005_OUTPUT_TABLES = {
    "SOURCE_AVAILABILITY": resolve_output_table(
        [
            "IP005_SOURCE_AVAILABILITY"
        ],
        "ip005_source_availability"
    ),

    "OPERATIONAL_ASSET_VIEW": resolve_output_table(
        [
            "OPERATIONAL_ASSET_VIEW",
            "IP005_OPERATIONAL_ASSET_VIEW"
        ],
        "operational_asset_view"
    )
}


# ---------------------------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------------------------

def first_existing_column(
    dataframe,
    candidates
):
    lookup = {
        column_name.lower(): column_name
        for column_name in dataframe.columns
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
        column_name.lower(): column_name
        for column_name in dataframe.columns
    }

    return [
        lookup[candidate.lower()]
        for candidate in candidates
        if candidate.lower() in lookup
    ]


def normalized_key(dataframe):
    if "asset_key" not in dataframe.columns:
        raise RuntimeError(
            "Required asset_key column is missing."
        )

    return (
        dataframe
        .withColumn(
            "asset_key",
            F.trim(
                F.col("asset_key").cast("string")
            )
        )
        .filter(
            F.col("asset_key").isNotNull()
            &
            (F.col("asset_key") != "")
        )
    )


def certify_one_row_per_asset(
    dataframe,
    label
):
    metrics = (
        dataframe
        .agg(
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
        )
        .first()
    )

    row_count = int(metrics["row_count"] or 0)
    distinct_count = int(
        metrics["distinct_asset_keys"] or 0
    )
    blank_count = int(
        metrics["blank_asset_keys"] or 0
    )

    if row_count == 0:
        raise RuntimeError(
            f"{label} contains no records."
        )

    if (
        row_count != distinct_count
        or blank_count != 0
    ):
        raise RuntimeError(
            f"{label} is not one row per nonblank asset_key. "
            f"Rows={row_count}; distinct={distinct_count}; "
            f"blank={blank_count}."
        )

    return row_count


def string_array_or_empty(
    dataframe,
    column_name
):
    if column_name in dataframe.columns:
        return F.col(column_name)

    return F.array().cast(
        ArrayType(StringType())
    )


def text_from_candidates(
    dataframe,
    candidates
):
    columns = existing_columns(
        dataframe,
        candidates
    )

    if not columns:
        return F.lit("")

    return F.lower(
        F.concat_ws(
            " ",
            *[
                F.coalesce(
                    F.col(
                        f"`{column_name}`"
                    ).cast("string"),
                    F.lit("")
                )
                for column_name in columns
            ]
        )
    )


def priority_rank_expression(
    column_expression
):
    normalized = F.upper(
        F.trim(
            column_expression.cast("string")
        )
    )

    return (
        F.when(
            normalized.isin(
                "CRITICAL",
                "P0"
            ),
            5
        )
        .when(
            normalized.isin(
                "HIGH",
                "P1"
            ),
            4
        )
        .when(
            normalized.isin(
                "MEDIUM",
                "P2"
            ),
            3
        )
        .when(
            normalized.isin(
                "REVIEW",
                "P3"
            ),
            2
        )
        .when(
            normalized.isin(
                "LOW",
                "P4"
            ),
            1
        )
        .otherwise(0)
    )


def rank_to_priority(
    rank_column
):
    return (
        F.when(rank_column >= 5, "Critical")
        .when(rank_column == 4, "High")
        .when(rank_column == 3, "Medium")
        .when(rank_column == 2, "Review")
        .when(rank_column == 1, "Low")
        .otherwise("Pass")
    )


def add_runtime_columns(
    dataframe
):
    output = dataframe

    runtime_values = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "rcaip_run_id": RCAIP_RUN_ID
    }

    for column_name, value in runtime_values.items():
        if column_name not in output.columns:
            output = output.withColumn(
                column_name,
                F.lit(value)
            )

    return (
        output
        .withColumn(
            "ip005_generated_at_utc",
            F.current_timestamp()
        )
    )


def save_ip005(
    dataframe,
    output_key
):
    table_name = IP005_OUTPUT_TABLES[
        output_key
    ]

    if not table_name.lower().startswith(
        expected_post_table_prefix
    ):
        raise RuntimeError(
            "Cross-Post output binding blocked: "
            + table_name
        )

    (
        add_runtime_columns(dataframe)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print("Saved:", table_name)


# ---------------------------------------------------------------------------
# 4. Source availability record
# ---------------------------------------------------------------------------

source_availability_rows = []

for source_key, source_contract in IP005_INPUTS.items():
    table_name = source_contract["table_name"]

    row_count = None
    column_count = None
    read_error = None

    if source_contract["available"]:
        try:
            source_df = spark.table(table_name)
            row_count = int(source_df.count())
            column_count = len(source_df.columns)
        except Exception as exc:
            read_error = str(exc)[:2000]

            if source_contract["required"]:
                raise

    source_availability_rows.append({
        "source_key": source_key,
        "source_label": source_contract["label"],
        "required": source_contract["required"],
        "available": (
            source_contract["available"]
            and read_error is None
        ),
        "table_name": table_name,
        "resolution_basis": source_contract[
            "resolution_basis"
        ],
        "row_count": row_count,
        "column_count": column_count,
        "read_error": read_error,
        "missing_source_interpretation": (
            "REQUIRED_SOURCE_FAILURE"
            if source_contract["required"]
            and not source_contract["available"]
            else
            "OPTIONAL_SOURCE_NOT_AVAILABLE_NO_FAILURE"
            if not source_contract["required"]
            and not source_contract["available"]
            else
            "SOURCE_AVAILABLE"
        )
    })

source_availability_schema = StructType([
    StructField(
        "source_key",
        StringType(),
        False
    ),
    StructField(
        "source_label",
        StringType(),
        False
    ),
    StructField(
        "required",
        BooleanType(),
        False
    ),
    StructField(
        "available",
        BooleanType(),
        False
    ),
    StructField(
        "table_name",
        StringType(),
        True
    ),
    StructField(
        "resolution_basis",
        StringType(),
        True
    ),
    StructField(
        "row_count",
        LongType(),
        True
    ),
    StructField(
        "column_count",
        LongType(),
        True
    ),
    StructField(
        "read_error",
        StringType(),
        True
    ),
    StructField(
        "missing_source_interpretation",
        StringType(),
        False
    )
])

source_availability = spark.createDataFrame(
    source_availability_rows,
    schema=source_availability_schema
)

save_ip005(
    source_availability,
    "SOURCE_AVAILABILITY"
)


# ---------------------------------------------------------------------------
# 5. Certified Asset Plane and lifecycle population
# ---------------------------------------------------------------------------

asset_plane = (
    normalized_key(
        spark.table(
            IP005_INPUTS[
                "ASSET_PLANE"
            ]["table_name"]
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)

asset_plane_count = certify_one_row_per_asset(
    asset_plane,
    "Certified scored Asset Plane"
)

required_asset_plane_fields = {
    "asset_key",
    "cmdb_present",
    "observed_source_count_raw",
    "observed_source_count_active",
    "observed_source_count_stale",
    "current_observation_status",
    "lifecycle_reconciliation_status"
}

missing_asset_plane_fields = sorted(
    required_asset_plane_fields
    -
    set(asset_plane.columns)
)

if missing_asset_plane_fields:
    raise RuntimeError(
        "Asset Plane is missing required IP-005 fields: "
        + " | ".join(missing_asset_plane_fields)
    )


lifecycle_base = normalized_key(
    spark.table(
        IP005_INPUTS[
            "LIFECYCLE"
        ]["table_name"]
    )
)

lifecycle_count = certify_one_row_per_asset(
    lifecycle_base,
    "IP-003 lifecycle rollup"
)

if lifecycle_count != asset_plane_count:
    raise RuntimeError(
        "IP-003 lifecycle population does not match "
        "the certified Asset Plane. "
        f"Asset Plane={asset_plane_count}; "
        f"lifecycle={lifecycle_count}."
    )


lifecycle_fields = existing_columns(
    lifecycle_base,
    [
        "asset_key",
        "asset_name",
        "device_name",
        "asset_type",
        "asset_domain",
        "evidence_state",
        "lifecycle_state",
        "lifecycle_status",
        "freshness_status",
        "days_since_last_seen",
        "days_since_most_recent_active",
        "record_age_bucket",
        "active_age_bucket",
        "stale_evidence_indicator",
        "unknown_freshness_indicator",
        "has_post_token",
        "review_priority",
        "validation_responsibility",
        "recommended_action"
    ]
)

lifecycle = lifecycle_base.select(
    *[
        (
            F.col(column_name)
            if column_name == "asset_key"
            else
            F.col(column_name).alias(
                f"lifecycle__{column_name}"
            )
        )
        for column_name in lifecycle_fields
    ]
)


# ---------------------------------------------------------------------------
# 6. Aggregate IP-002 tool evidence to one row per asset
# ---------------------------------------------------------------------------

tool_evidence = normalized_key(
    spark.table(
        IP005_INPUTS[
            "TOOL_COVERAGE"
        ]["table_name"]
    )
)

tool_name_column = first_existing_column(
    tool_evidence,
    [
        "required_tool",
        "expected_tool",
        "tool_name",
        "security_tool",
        "tool",
        "source_name"
    ]
)

tool_priority_column = first_existing_column(
    tool_evidence,
    [
        "priority",
        "review_priority",
        "validation_priority"
    ]
)

tool_explicit_validation_column = first_existing_column(
    tool_evidence,
    [
        "requires_validation",
        "validation_required",
        "requires_review",
        "review_required"
    ]
)

tool_status_text = text_from_candidates(
    tool_evidence,
    [
        "coverage_status",
        "validation_status",
        "tool_status",
        "reporting_status",
        "current_status",
        "interpretation",
        "recommended_action",
        "priority"
    ]
)

if tool_explicit_validation_column is not None:
    explicit_validation_text = F.lower(
        F.trim(
            F.col(
                f"`{tool_explicit_validation_column}`"
            ).cast("string")
        )
    )

    explicit_validation = (
        explicit_validation_text.isin(
            "true",
            "1",
            "y",
            "yes",
            "required",
            "review",
            "requires validation"
        )
    )

else:
    explicit_validation = F.lit(False)


tool_requires_validation = (
    explicit_validation
    |
    tool_status_text.rlike(
        "validation|review|missing|gap|"
        "not reporting|not installed|unknown|"
        "unresolved|absent|stale"
    )
)

tool_pass_or_reporting = (
    ~tool_requires_validation
    &
    tool_status_text.rlike(
        "pass|reporting|covered|current|installed|present"
    )
)

tool_aggregation_expressions = [
    F.count("*").alias(
        "tool_expectation_count"
    ),

    F.sum(
        F.when(
            tool_requires_validation,
            1
        ).otherwise(0)
    ).alias(
        "tool_validation_required_count"
    ),

    F.sum(
        F.when(
            tool_pass_or_reporting,
            1
        ).otherwise(0)
    ).alias(
        "tool_pass_or_reporting_count"
    )
]

if tool_name_column is not None:
    tool_aggregation_expressions.extend([
        F.sort_array(
            F.collect_set(
                F.col(
                    f"`{tool_name_column}`"
                ).cast("string")
            )
        ).alias(
            "expected_tools"
        ),

        F.sort_array(
            F.collect_set(
                F.when(
                    tool_requires_validation,
                    F.col(
                        f"`{tool_name_column}`"
                    ).cast("string")
                )
            )
        ).alias(
            "tools_requiring_validation"
        )
    ])

else:
    tool_aggregation_expressions.extend([
        F.array().cast(
            ArrayType(StringType())
        ).alias(
            "expected_tools"
        ),

        F.array().cast(
            ArrayType(StringType())
        ).alias(
            "tools_requiring_validation"
        )
    ])


if tool_priority_column is not None:
    tool_aggregation_expressions.append(
        F.max(
            priority_rank_expression(
                F.col(
                    f"`{tool_priority_column}`"
                )
            )
        ).alias(
            "tool_max_priority_rank"
        )
    )

else:
    tool_aggregation_expressions.append(
        F.lit(0).cast("integer").alias(
            "tool_max_priority_rank"
        )
    )


tool_summary = (
    tool_evidence
    .groupBy("asset_key")
    .agg(
        *tool_aggregation_expressions
    )
    .withColumn(
        "tool_max_priority",
        rank_to_priority(
            F.col("tool_max_priority_rank")
        )
    )
)


# ---------------------------------------------------------------------------
# 7. Aggregate IP-004 computer hygiene to asset level when available
# ---------------------------------------------------------------------------

empty_ad_summary = (
    asset_plane
    .select("asset_key")
    .limit(0)
    .withColumn(
        "ad_computer_record_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "ad_computer_max_priority_rank",
        F.lit(None).cast("integer")
    )
    .withColumn(
        "ad_computer_max_priority",
        F.lit(None).cast("string")
    )
    .withColumn(
        "ad_computer_conditions",
        F.array().cast(
            ArrayType(StringType())
        )
    )
)

ad_computer_summary = empty_ad_summary

if IP005_INPUTS[
    "IP004_COMPUTER_REVIEW"
]["available"]:

    ad_computer_review = spark.table(
        IP005_INPUTS[
            "IP004_COMPUTER_REVIEW"
        ]["table_name"]
    )

    if "asset_key" not in ad_computer_review.columns:
        if (
            IP005_INPUTS[
                "IP004_COMPUTER_MAP"
            ]["available"]
            and
            "ad_object_key"
            in ad_computer_review.columns
        ):
            computer_map = spark.table(
                IP005_INPUTS[
                    "IP004_COMPUTER_MAP"
                ]["table_name"]
            )

            if (
                "ad_object_key"
                in computer_map.columns
                and
                "asset_key"
                in computer_map.columns
            ):
                ad_computer_review = (
                    ad_computer_review
                    .join(
                        computer_map.select(
                            "ad_object_key",
                            "asset_key"
                        ),
                        on="ad_object_key",
                        how="left"
                    )
                )

    if "asset_key" in ad_computer_review.columns:
        ad_computer_review = normalized_key(
            ad_computer_review
        )

        ad_priority_column = first_existing_column(
            ad_computer_review,
            [
                "computer_security_priority",
                "priority",
                "review_priority"
            ]
        )

        ad_condition_column = first_existing_column(
            ad_computer_review,
            [
                "computer_review_condition",
                "condition",
                "interpretation"
            ]
        )

        ad_aggregation_expressions = [
            F.count("*").alias(
                "ad_computer_record_count"
            )
        ]

        if ad_priority_column is not None:
            ad_aggregation_expressions.append(
                F.max(
                    priority_rank_expression(
                        F.col(
                            f"`{ad_priority_column}`"
                        )
                    )
                ).alias(
                    "ad_computer_max_priority_rank"
                )
            )
        else:
            ad_aggregation_expressions.append(
                F.lit(0).cast("integer").alias(
                    "ad_computer_max_priority_rank"
                )
            )

        if ad_condition_column is not None:
            ad_aggregation_expressions.append(
                F.sort_array(
                    F.collect_set(
                        F.col(
                            f"`{ad_condition_column}`"
                        ).cast("string")
                    )
                ).alias(
                    "ad_computer_conditions"
                )
            )
        else:
            ad_aggregation_expressions.append(
                F.array().cast(
                    ArrayType(StringType())
                ).alias(
                    "ad_computer_conditions"
                )
            )

        ad_computer_summary = (
            ad_computer_review
            .groupBy("asset_key")
            .agg(
                *ad_aggregation_expressions
            )
            .withColumn(
                "ad_computer_max_priority",
                rank_to_priority(
                    F.col(
                        "ad_computer_max_priority_rank"
                    )
                )
            )
        )


# ---------------------------------------------------------------------------
# 8. Optional open-port enrichment
# ---------------------------------------------------------------------------

empty_open_port_summary = (
    asset_plane
    .select("asset_key")
    .limit(0)
    .withColumn(
        "open_port_record_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "distinct_open_port_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "high_interest_open_port_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "observed_open_ports",
        F.array().cast(
            ArrayType(StringType())
        )
    )
)

open_port_summary = empty_open_port_summary

if IP005_INPUTS["OPEN_PORTS"]["available"]:
    open_ports = spark.table(
        IP005_INPUTS[
            "OPEN_PORTS"
        ]["table_name"]
    )

    if "asset_key" not in open_ports.columns:
        if (
            "host_key" in open_ports.columns
            and
            "host_key" in asset_plane.columns
        ):
            host_to_asset = (
                asset_plane
                .select(
                    "host_key",
                    "asset_key"
                )
                .filter(
                    F.col("host_key").isNotNull()
                )
                .dropDuplicates(
                    ["host_key"]
                )
            )

            open_ports = open_ports.join(
                host_to_asset,
                on="host_key",
                how="left"
            )

    if "asset_key" in open_ports.columns:
        open_ports = normalized_key(
            open_ports
        )

        port_column = first_existing_column(
            open_ports,
            [
                "port",
                "port_number",
                "destination_port",
                "open_port"
            ]
        )

        open_port_text = text_from_candidates(
            open_ports,
            [
                "priority",
                "risk",
                "severity",
                "service",
                "service_name",
                "review_reason",
                "interpretation"
            ]
        )

        high_interest_condition = (
            open_port_text.rlike(
                "critical|high|review|required|"
                "rdp|telnet|ftp|smb|vnc|database|"
                "management|remote"
            )
        )

        open_port_aggregations = [
            F.count("*").alias(
                "open_port_record_count"
            ),

            F.sum(
                F.when(
                    high_interest_condition,
                    1
                ).otherwise(0)
            ).alias(
                "high_interest_open_port_count"
            )
        ]

        if port_column is not None:
            open_port_aggregations.extend([
                F.countDistinct(
                    F.col(
                        f"`{port_column}`"
                    )
                ).alias(
                    "distinct_open_port_count"
                ),

                F.sort_array(
                    F.collect_set(
                        F.col(
                            f"`{port_column}`"
                        ).cast("string")
                    )
                ).alias(
                    "observed_open_ports"
                )
            ])
        else:
            open_port_aggregations.extend([
                F.lit(None).cast("long").alias(
                    "distinct_open_port_count"
                ),

                F.array().cast(
                    ArrayType(StringType())
                ).alias(
                    "observed_open_ports"
                )
            ])

        open_port_summary = (
            open_ports
            .groupBy("asset_key")
            .agg(
                *open_port_aggregations
            )
        )


# ---------------------------------------------------------------------------
# 9. Optional Go Manager enrichment
#
# Go Manager is context only in this run. Its absence does not create a gap.
# ---------------------------------------------------------------------------

empty_go_manager_summary = (
    asset_plane
    .select("asset_key")
    .limit(0)
    .withColumn(
        "go_manager_record_count",
        F.lit(None).cast("long")
    )
)

go_manager_summary = empty_go_manager_summary

if IP005_INPUTS["GO_MANAGER"]["available"]:
    go_manager = spark.table(
        IP005_INPUTS[
            "GO_MANAGER"
        ]["table_name"]
    )

    if "asset_key" in go_manager.columns:
        go_manager_summary = (
            normalized_key(go_manager)
            .groupBy("asset_key")
            .agg(
                F.count("*").alias(
                    "go_manager_record_count"
                )
            )
        )


# ---------------------------------------------------------------------------
# 10. Build the one-row operational asset view
# ---------------------------------------------------------------------------

operational_asset_view = (
    asset_plane

    .join(
        lifecycle,
        on="asset_key",
        how="left"
    )

    .join(
        tool_summary,
        on="asset_key",
        how="left"
    )

    .join(
        ad_computer_summary,
        on="asset_key",
        how="left"
    )

    .join(
        open_port_summary,
        on="asset_key",
        how="left"
    )

    .join(
        go_manager_summary,
        on="asset_key",
        how="left"
    )

    .withColumn(
        "post_key",
        F.lit(RCAIP_POST_SLUG)
    )

    .withColumn(
        "post_display",
        F.lit(RCAIP_POST_NAME)
    )

    .withColumn(
        "present_in_asset_plane",
        F.lit(True)
    )

    .withColumn(
        "present_in_lifecycle",
        F.col(
            "lifecycle__lifecycle_state"
        ).isNotNull()
    )

    .withColumn(
        "present_in_tool_coverage",
        F.coalesce(
            F.col(
                "tool_expectation_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "present_in_ad_computer_review",
        F.coalesce(
            F.col(
                "ad_computer_record_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "present_in_open_port_evidence",
        F.coalesce(
            F.col(
                "open_port_record_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "present_in_go_manager",
        F.coalesce(
            F.col(
                "go_manager_record_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "open_port_source_status",
        F.lit(
            "AVAILABLE"
            if IP005_INPUTS[
                "OPEN_PORTS"
            ]["available"]
            else
            "OPTIONAL_NOT_AVAILABLE"
        )
    )

    .withColumn(
        "go_manager_source_status",
        F.lit(
            "AVAILABLE"
            if IP005_INPUTS[
                "GO_MANAGER"
            ]["available"]
            else
            "OPTIONAL_NOT_AVAILABLE"
        )
    )

    .withColumn(
        "current_evidence_flag",
        F.coalesce(
            F.col(
                "observed_source_count_active"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "stale_evidence_flag",
        F.coalesce(
            F.col(
                "observed_source_count_stale"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "unknown_freshness_flag",
        (
            F.coalesce(
                F.col(
                    "observed_source_count_raw"
                ),
                F.lit(0)
            )
            >
            0
        )
        &
        (
            F.coalesce(
                F.col(
                    "observed_source_count_active"
                ),
                F.lit(0)
            )
            ==
            0
        )
        &
        (
            F.coalesce(
                F.col(
                    "observed_source_count_stale"
                ),
                F.lit(0)
            )
            ==
            0
        )
    )

    .withColumn(
        "cmdb_present_flag",
        F.upper(
            F.trim(
                F.col(
                    "cmdb_present"
                ).cast("string")
            )
        ).isin(
            "Y",
            "YES",
            "TRUE",
            "1"
        )
    )

    .withColumn(
        "tool_validation_required_flag",
        F.coalesce(
            F.col(
                "tool_validation_required_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "tool_coverage_assessed_flag",
        F.coalesce(
            F.col(
                "tool_expectation_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "ad_computer_validation_required_flag",
        F.coalesce(
            F.col(
                "ad_computer_max_priority_rank"
            ),
            F.lit(0)
        )
        >=
        2
    )

    .withColumn(
        "open_port_validation_required_flag",
        F.coalesce(
            F.col(
                "high_interest_open_port_count"
            ),
            F.lit(0)
        )
        >
        0
    )

    .withColumn(
        "operational_population_class",

        F.when(
            F.col("current_evidence_flag")
            &
            F.col("cmdb_present_flag"),
            "CURRENT_CMDB_RECONCILED"
        )

        .when(
            F.col("current_evidence_flag")
            &
            ~F.col("cmdb_present_flag"),
            "CURRENT_OBSERVED_NOT_CMDB"
        )

        .when(
            ~F.col("current_evidence_flag")
            &
            F.col("stale_evidence_flag")
            &
            F.col("cmdb_present_flag"),
            "CMDB_WITH_STALE_ONLY_EVIDENCE"
        )

        .when(
            ~F.col("current_evidence_flag")
            &
            F.col("stale_evidence_flag")
            &
            ~F.col("cmdb_present_flag"),
            "OBSERVED_NOT_CMDB_STALE_ONLY"
        )

        .when(
            F.col("unknown_freshness_flag")
            &
            F.col("cmdb_present_flag"),
            "CMDB_WITH_UNKNOWN_FRESHNESS"
        )

        .when(
            F.col("unknown_freshness_flag")
            &
            ~F.col("cmdb_present_flag"),
            "OBSERVED_NOT_CMDB_UNKNOWN_FRESHNESS"
        )

        .when(
            F.col("cmdb_present_flag"),
            "CMDB_WITHOUT_ACTIVE_EVIDENCE"
        )

        .otherwise(
            "UNRESOLVED_OPERATIONAL_IDENTITY"
        )
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


# ---------------------------------------------------------------------------
# 11. Persist and certify
# ---------------------------------------------------------------------------

save_ip005(
    operational_asset_view,
    "OPERATIONAL_ASSET_VIEW"
)

persisted_operational_view = spark.table(
    IP005_OUTPUT_TABLES[
        "OPERATIONAL_ASSET_VIEW"
    ]
)

operational_metrics = (
    persisted_operational_view
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
            ).otherwise(0)
        ).alias(
            "cross_post_rows"
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
        ),

        F.sum(
            F.coalesce(
                F.col(
                    "tool_expectation_count"
                ),
                F.lit(0)
            )
        ).alias(
            "tool_expectation_records"
        ),

        F.sum(
            F.coalesce(
                F.col(
                    "tool_validation_required_count"
                ),
                F.lit(0)
            )
        ).alias(
            "tool_validation_records"
        )
    )
    .first()
)

operational_row_count = int(
    operational_metrics[
        "row_count"
    ]
    or
    0
)

operational_distinct_count = int(
    operational_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)

cross_post_rows = int(
    operational_metrics[
        "cross_post_rows"
    ]
    or
    0
)

cross_run_rows = int(
    operational_metrics[
        "cross_run_rows"
    ]
    or
    0
)

if operational_row_count != asset_plane_count:
    raise RuntimeError(
        "Operational view did not preserve the complete "
        "Asset Plane population. "
        f"Asset Plane={asset_plane_count}; "
        f"operational view={operational_row_count}."
    )

if operational_distinct_count != operational_row_count:
    raise RuntimeError(
        "Operational view is not one row per asset_key."
    )

if cross_post_rows > 0 or cross_run_rows > 0:
    raise RuntimeError(
        "Operational view contains cross-Post or cross-run rows."
    )


IP005_OPERATIONAL_FOUNDATION_READY = True


print("=" * 100)
print("RCAIP IP-005 - OPERATIONAL ASSET FOUNDATION")
print("=" * 100)
print("Status: PASS")
print("Ready for operational taxonomy:", IP005_OPERATIONAL_FOUNDATION_READY)
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
print("Certified Asset Plane rows:", asset_plane_count)
print("Operational asset rows:", operational_row_count)
print(
    "Asset-tool expectation records:",
    int(
        operational_metrics[
            "tool_expectation_records"
        ]
        or
        0
    )
)
print(
    "Asset-tool records requiring validation:",
    int(
        operational_metrics[
            "tool_validation_records"
        ]
        or
        0
    )
)
print(
    "IP-004 computer hygiene available:",
    IP005_INPUTS[
        "IP004_COMPUTER_REVIEW"
    ]["available"]
)
print(
    "IP-001 open-port enrichment:",
    (
        "AVAILABLE"
        if IP005_INPUTS[
            "OPEN_PORTS"
        ]["available"]
        else
        "OPTIONAL_NOT_AVAILABLE"
    )
)
print(
    "Go Manager enrichment:",
    (
        "AVAILABLE"
        if IP005_INPUTS[
            "GO_MANAGER"
        ]["available"]
        else
        "OPTIONAL_NOT_AVAILABLE"
    )
)
print("Cross-Post rows:", cross_post_rows)
print("Cross-run rows:", cross_run_rows)
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-005 - OPERATIONAL VIEW
# Cell 3 - Operational taxonomy and decision products
#
# Design:
#   - Identity disposition remains mutually exclusive.
#   - Tool, AD-computer, open-port, and data-governance actions remain
#     independent dimensions.
#   - Optional missing sources do not create findings or failed coverage.
#   - IP-004 non-asset identity actions are retained in the integrated
#     management queue without forcing them into the asset population.
# ============================================================================

from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# 1. Foundation contract
# ---------------------------------------------------------------------------

required_foundation_objects = [
    "IP005_OPERATIONAL_FOUNDATION_READY",
    "IP005_INPUTS",
    "IP005_OUTPUT_TABLES",
    "resolve_output_table",
    "save_ip005"
]

missing_foundation_objects = [
    object_name
    for object_name in required_foundation_objects
    if object_name not in globals()
]

if missing_foundation_objects:
    raise RuntimeError(
        "Run IP-005 Cell 2 before Cell 3. Missing: "
        + " | ".join(missing_foundation_objects)
    )

if not IP005_OPERATIONAL_FOUNDATION_READY:
    raise RuntimeError(
        "IP-005 operational foundation is not certified."
    )


IP005_OUTPUT_TABLES.update({
    "INVESTIGATION_UNIVERSE": resolve_output_table(
        [
            "OPERATIONAL_INVESTIGATION_UNIVERSE",
            "IP005_OPERATIONAL_INVESTIGATION_UNIVERSE"
        ],
        "operational_investigation_universe"
    ),

    "IDENTITY_QUEUE": resolve_output_table(
        [
            "OPERATIONAL_IDENTITY_INVESTIGATION_QUEUE",
            "IP005_IDENTITY_INVESTIGATION_QUEUE"
        ],
        "operational_identity_investigation_queue"
    ),

    "TOOL_QUEUE": resolve_output_table(
        [
            "SECURITY_TOOL_REMEDIATION_QUEUE",
            "IP005_SECURITY_TOOL_VALIDATION_QUEUE"
        ],
        "security_tool_validation_queue"
    ),

    "DATA_GOVERNANCE_QUEUE": resolve_output_table(
        [
            "CANONICAL_IDENTITY_RECONCILIATION_QUEUE",
            "IP005_DATA_GOVERNANCE_QUEUE"
        ],
        "canonical_identity_reconciliation_queue"
    ),

    "ENTERPRISE_ATTRIBUTION_QUEUE": resolve_output_table(
        [
            "ENTERPRISE_ATTRIBUTION_CANDIDATE_QUEUE",
            "IP005_ENTERPRISE_ATTRIBUTION_QUEUE"
        ],
        "enterprise_attribution_candidate_queue"
    ),

    "CONFIRMED_BASELINE": resolve_output_table(
        [
            "CONFIRMED_OPERATIONAL_BASELINE",
            "IP005_CONFIRMED_OPERATIONAL_BASELINE"
        ],
        "confirmed_operational_baseline"
    ),

    "ASSET_ACTION_QUEUE": resolve_output_table(
        [
            "IP005_OPERATIONAL_ASSET_ACTION_QUEUE"
        ],
        "ip005_operational_asset_action_queue"
    ),

    "INTEGRATED_ACTION_QUEUE": resolve_output_table(
        [
            "IP005_INTEGRATED_OPERATIONAL_ACTION_QUEUE"
        ],
        "ip005_integrated_operational_action_queue"
    )
})


operational_view = (
    spark.table(
        IP005_OUTPUT_TABLES[
            "OPERATIONAL_ASSET_VIEW"
        ]
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


required_operational_fields = {
    "asset_key",
    "post_slug",
    "rcaip_run_id",
    "operational_population_class",
    "current_evidence_flag",
    "stale_evidence_flag",
    "unknown_freshness_flag",
    "cmdb_present_flag",
    "tool_validation_required_flag",
    "ad_computer_validation_required_flag",
    "open_port_validation_required_flag"
}

missing_operational_fields = sorted(
    required_operational_fields
    -
    set(operational_view.columns)
)

if missing_operational_fields:
    raise RuntimeError(
        "Operational asset view is missing required fields: "
        + " | ".join(missing_operational_fields)
    )


# ---------------------------------------------------------------------------
# 2. Reusable expressions
# ---------------------------------------------------------------------------

post_token_column = (
    F.coalesce(
        F.col(
            "lifecycle__has_post_token"
        ).cast("boolean"),
        F.lit(False)
    )
    if "lifecycle__has_post_token"
    in operational_view.columns
    else
    F.lit(False)
)

asset_name_expression = F.coalesce(
    *[
        F.col(column_name).cast("string")
        for column_name in [
            "lifecycle__asset_name",
            "lifecycle__device_name",
            "cmdb_name",
            "cmdb_fqdn",
            "host_key",
            "asset_key"
        ]
        if column_name in operational_view.columns
    ]
)

lifecycle_state_column = (
    F.col(
        "lifecycle__lifecycle_state"
    ).cast("string")
    if "lifecycle__lifecycle_state"
    in operational_view.columns
    else
    F.lit(None).cast("string")
)


# ---------------------------------------------------------------------------
# 3. Mutually exclusive identity and disposition taxonomy
# ---------------------------------------------------------------------------

primary_case_type = (
    F.when(
        F.col("unknown_freshness_flag"),
        "RECENCY_EVIDENCE_EXCEPTION"
    )

    .when(
        F.col("current_evidence_flag")
        &
        ~F.col("cmdb_present_flag")
        &
        post_token_column,
        "POST_CMDB_REGISTRATION_VALIDATION"
    )

    .when(
        F.col("current_evidence_flag")
        &
        ~F.col("cmdb_present_flag")
        &
        ~post_token_column,
        "ENTERPRISE_ATTRIBUTION_CANDIDATE"
    )

    .when(
        ~F.col("current_evidence_flag")
        &
        F.col("stale_evidence_flag")
        &
        ~F.col("cmdb_present_flag"),
        "ENTERPRISE_STALE_IDENTITY_CLEANUP"
    )

    .when(
        ~F.col("current_evidence_flag")
        &
        F.col("cmdb_present_flag"),
        "CMDB_LIFECYCLE_VALIDATION"
    )

    .when(
        F.col("current_evidence_flag")
        &
        F.col("cmdb_present_flag"),
        "CONFIRMED_OPERATIONAL_BASELINE"
    )

    .otherwise(
        "MANUAL_OPERATIONAL_REVIEW"
    )
)


identity_priority = (
    F.when(
        primary_case_type
        ==
        "POST_CMDB_REGISTRATION_VALIDATION",
        "High"
    )

    .when(
        primary_case_type.isin(
            "ENTERPRISE_ATTRIBUTION_CANDIDATE",
            "RECENCY_EVIDENCE_EXCEPTION",
            "CMDB_LIFECYCLE_VALIDATION"
        ),
        "Medium"
    )

    .when(
        primary_case_type.isin(
            "ENTERPRISE_STALE_IDENTITY_CLEANUP",
            "MANUAL_OPERATIONAL_REVIEW"
        ),
        "Review"
    )

    .otherwise(
        "Pass"
    )
)


recommended_owner_scope = (
    F.when(
        primary_case_type
        ==
        "POST_CMDB_REGISTRATION_VALIDATION",
        "Post corroboration with CMDB and Enterprise Attribution"
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_ATTRIBUTION_CANDIDATE",
        "Enterprise Attribution with Post corroboration when evidence exists"
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_STALE_IDENTITY_CLEANUP",
        "Enterprise Source Owner and Data Hygiene"
    )

    .when(
        primary_case_type
        ==
        "CMDB_LIFECYCLE_VALIDATION",
        "CMDB and Enterprise Asset Management with Post corroboration"
    )

    .when(
        primary_case_type
        ==
        "RECENCY_EVIDENCE_EXCEPTION",
        "Enterprise Source Owner and Ingestion Governance"
    )

    .when(
        primary_case_type
        ==
        "MANUAL_OPERATIONAL_REVIEW",
        "RCAIP Data Governance"
    )

    .otherwise(
        "Assurance Baseline"
    )
)


identity_reason = (
    F.when(
        primary_case_type
        ==
        "RECENCY_EVIDENCE_EXCEPTION",
        "The identity has source evidence but insufficient timestamp "
        "semantics to determine whether it is current or stale."
    )

    .when(
        primary_case_type
        ==
        "POST_CMDB_REGISTRATION_VALIDATION",
        "Current evidence and Post attribution indicators exist, but "
        "the identity does not presently reconcile to CMDB."
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_ATTRIBUTION_CANDIDATE",
        "Current enterprise evidence exists without a CMDB match or "
        "sufficient evidence to assign the identity to this Post."
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_STALE_IDENTITY_CLEANUP",
        "The identity is absent from CMDB and supported only by stale "
        "enterprise evidence."
    )

    .when(
        primary_case_type
        ==
        "CMDB_LIFECYCLE_VALIDATION",
        "The CMDB identity lacks current corroborating operational evidence."
    )

    .when(
        primary_case_type
        ==
        "CONFIRMED_OPERATIONAL_BASELINE",
        "The identity is current, represented in CMDB, and retained in "
        "the certified operational asset population."
    )

    .otherwise(
        "The current evidence combination requires analyst validation "
        "before an operational disposition can be certified."
    )
)


identity_recommended_action = (
    F.when(
        primary_case_type
        ==
        "RECENCY_EVIDENCE_EXCEPTION",
        "Validate source timestamps, ingestion freshness, and timestamp "
        "semantics before assigning current or stale status."
    )

    .when(
        primary_case_type
        ==
        "POST_CMDB_REGISTRATION_VALIDATION",
        "Validate canonical identity, ownership, operational status, and "
        "CMDB representation. Do not create a finding solely from the gap."
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_ATTRIBUTION_CANDIDATE",
        "Use enterprise identity, endpoint, naming, network, CMDB, and "
        "management evidence to determine the appropriate owner."
    )

    .when(
        primary_case_type
        ==
        "ENTERPRISE_STALE_IDENTITY_CLEANUP",
        "Validate retirement, transfer, rename, duplication, and source "
        "retention before cleanup."
    )

    .when(
        primary_case_type
        ==
        "CMDB_LIFECYCLE_VALIDATION",
        "Validate existence, ownership, lifecycle status, identity change, "
        "and source-discovery coverage."
    )

    .when(
        primary_case_type
        ==
        "CONFIRMED_OPERATIONAL_BASELINE",
        "Retain in the operational baseline and process independent control "
        "or management actions through their separate queues."
    )

    .otherwise(
        "Perform analyst review and assign the appropriate validation owner."
    )
)


# ---------------------------------------------------------------------------
# 4. Independent operational action dimensions
# ---------------------------------------------------------------------------

identity_investigation_required = (
    primary_case_type
    !=
    "CONFIRMED_OPERATIONAL_BASELINE"
)

security_tool_validation_required = (
    F.col(
        "tool_validation_required_flag"
    )
)

ad_computer_validation_required = (
    F.col(
        "ad_computer_validation_required_flag"
    )
)

open_port_validation_required = (
    F.col(
        "open_port_validation_required_flag"
    )
)

data_governance_action_required = (
    F.col("unknown_freshness_flag")
    |
    primary_case_type.isin(
        "MANUAL_OPERATIONAL_REVIEW"
    )
)

operational_action_required = (
    identity_investigation_required
    |
    security_tool_validation_required
    |
    ad_computer_validation_required
    |
    open_port_validation_required
    |
    data_governance_action_required
)


investigation_universe = (
    operational_view

    .withColumn(
        "primary_case_type",
        primary_case_type
    )

    .withColumn(
        "identity_priority",
        identity_priority
    )

    .withColumn(
        "recommended_owner_scope",
        recommended_owner_scope
    )

    .withColumn(
        "identity_reason",
        identity_reason
    )

    .withColumn(
        "identity_recommended_action",
        identity_recommended_action
    )

    .withColumn(
        "identity_investigation_required",
        identity_investigation_required
    )

    .withColumn(
        "security_tool_validation_required",
        security_tool_validation_required
    )

    .withColumn(
        "ad_computer_validation_required",
        ad_computer_validation_required
    )

    .withColumn(
        "open_port_validation_required",
        open_port_validation_required
    )

    .withColumn(
        "data_governance_action_required",
        data_governance_action_required
    )

    .withColumn(
        "operational_action_required",
        operational_action_required
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


save_ip005(
    investigation_universe,
    "INVESTIGATION_UNIVERSE"
)


# ---------------------------------------------------------------------------
# 5. Decision products
# ---------------------------------------------------------------------------

identity_queue = (
    investigation_universe
    .filter(
        F.col(
            "identity_investigation_required"
        )
    )
)

tool_queue = (
    investigation_universe
    .filter(
        F.col(
            "security_tool_validation_required"
        )
    )
)

data_governance_queue = (
    investigation_universe
    .filter(
        F.col(
            "data_governance_action_required"
        )
    )
)

enterprise_attribution_queue = (
    investigation_universe
    .filter(
        F.col(
            "primary_case_type"
        )
        ==
        "ENTERPRISE_ATTRIBUTION_CANDIDATE"
    )
)

confirmed_baseline = (
    investigation_universe
    .filter(
        F.col(
            "primary_case_type"
        )
        ==
        "CONFIRMED_OPERATIONAL_BASELINE"
    )
)


save_ip005(
    identity_queue,
    "IDENTITY_QUEUE"
)

save_ip005(
    tool_queue,
    "TOOL_QUEUE"
)

save_ip005(
    data_governance_queue,
    "DATA_GOVERNANCE_QUEUE"
)

save_ip005(
    enterprise_attribution_queue,
    "ENTERPRISE_ATTRIBUTION_QUEUE"
)

save_ip005(
    confirmed_baseline,
    "CONFIRMED_BASELINE"
)


# ---------------------------------------------------------------------------
# 6. Asset-level action queue
# ---------------------------------------------------------------------------

def asset_action_frame(
    source_df,
    condition,
    action_domain,
    action_type,
    priority_expression,
    condition_expression,
    interpretation_expression,
    responsibility_expression
):
    return (
        source_df
        .filter(condition)
        .select(
            F.concat(
                F.lit(
                    f"IP005-{action_type}-"
                ),
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.col("asset_key"),
                        F.lit(action_type)
                    ),
                    256
                )
            ).alias("action_id"),

            F.lit(
                action_domain
            ).alias(
                "action_domain"
            ),

            F.lit(
                action_type
            ).alias(
                "action_type"
            ),

            F.col(
                "asset_key"
            ).alias(
                "subject_key"
            ),

            asset_name_expression.alias(
                "subject_name"
            ),

            F.col(
                "asset_key"
            ).alias(
                "asset_key"
            ),

            priority_expression.alias(
                "priority"
            ),

            condition_expression.alias(
                "condition"
            ),

            interpretation_expression.alias(
                "interpretation"
            ),

            responsibility_expression.alias(
                "validation_responsibility"
            ),

            F.lit(
                "Requires Validation"
            ).alias(
                "action_status"
            ),

            F.lit(
                "IP-005"
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
    )


asset_action_frames = [
    asset_action_frame(
        investigation_universe,
        F.col(
            "identity_investigation_required"
        ),
        "Operational Identity",
        "IDENTITY_DISPOSITION_VALIDATION",
        F.col("identity_priority"),
        F.col("primary_case_type"),
        F.col("identity_reason"),
        F.col("recommended_owner_scope")
    ),

    asset_action_frame(
        investigation_universe,
        F.col(
            "security_tool_validation_required"
        ),
        "Security Tool Coverage",
        "SECURITY_TOOL_COVERAGE_VALIDATION",
        F.coalesce(
            F.col("tool_max_priority"),
            F.lit("Medium")
        ),
        F.concat(
            F.lit(
                "Asset-tool relationships requiring validation: "
            ),
            F.coalesce(
                F.col(
                    "tool_validation_required_count"
                ).cast("string"),
                F.lit("0")
            )
        ),
        F.lit(
            "Validate tool applicability, identity matching, installation, "
            "reporting status, and source freshness. This is not an "
            "automatic tool failure or compliance finding."
        ),
        F.lit(
            "Post corroboration with Enterprise Tool and Data Owners"
        )
    ),

    asset_action_frame(
        investigation_universe,
        F.col(
            "ad_computer_validation_required"
        ),
        "AD Computer",
        "AD_COMPUTER_HYGIENE_VALIDATION",
        F.coalesce(
            F.col(
                "ad_computer_max_priority"
            ),
            F.lit("Review")
        ),
        F.concat_ws(
            "; ",
            F.col(
                "ad_computer_conditions"
            )
        ),
        F.lit(
            "Validate computer-account lifecycle, identity mapping, and "
            "ownership before changing the AD object or Asset Plane."
        ),
        F.lit(
            "Post AD Administration with Enterprise Identity and "
            "Asset-Management support"
        )
    ),

    asset_action_frame(
        investigation_universe,
        F.col(
            "open_port_validation_required"
        ),
        "Open Port",
        "OPEN_PORT_EVIDENCE_VALIDATION",
        F.lit("Review"),
        F.concat(
            F.lit(
                "High-interest open-port records: "
            ),
            F.coalesce(
                F.col(
                    "high_interest_open_port_count"
                ).cast("string"),
                F.lit("0")
            )
        ),
        F.lit(
            "Open-port evidence requires asset, service, exposure, and "
            "business-need validation. An observed port is not an "
            "automatic vulnerability or finding."
        ),
        F.lit(
            "Post technical validation with Enterprise Network and "
            "Vulnerability-Management support"
        )
    ),

    asset_action_frame(
        investigation_universe,
        F.col(
            "data_governance_action_required"
        ),
        "Data Governance",
        "SOURCE_FRESHNESS_AND_IDENTITY_GOVERNANCE",
        F.lit("Medium"),
        F.col("primary_case_type"),
        F.lit(
            "Validate source freshness, timestamp semantics, identity "
            "correlation, and ingestion behavior before operational use."
        ),
        F.lit(
            "Enterprise Source Owner and RCAIP Data Governance"
        )
    )
]


asset_action_queue = (
    reduce(
        lambda left, right:
            left.unionByName(
                right,
                allowMissingColumns=True
            ),
        asset_action_frames
    )
    .dropDuplicates(
        ["action_id"]
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


save_ip005(
    asset_action_queue,
    "ASSET_ACTION_QUEUE"
)


# ---------------------------------------------------------------------------
# 7. Integrated operational-management queue
#
# IP-004 users, groups, and memberships remain non-asset identity actions.
# They are integrated for management visibility but do not change the
# runtime row operational asset population.
# ---------------------------------------------------------------------------

integrated_action_queue = asset_action_queue

ip004_action_count = 0

if IP005_INPUTS[
    "IP004_ACTION_QUEUE"
]["available"]:

    ip004_actions_source = spark.table(
        IP005_INPUTS[
            "IP004_ACTION_QUEUE"
        ]["table_name"]
    )

    required_ip004_action_fields = {
        "action_id",
        "action_domain",
        "subject_key",
        "priority",
        "condition",
        "interpretation",
        "validation_responsibility"
    }

    if required_ip004_action_fields.issubset(
        set(
            ip004_actions_source.columns
        )
    ):
        ip004_actions = (
            ip004_actions_source

            .select(
                F.concat(
                    F.lit("IP004-"),
                    F.col(
                        "action_id"
                    ).cast("string")
                ).alias(
                    "action_id"
                ),

                F.col(
                    "action_domain"
                ).cast("string").alias(
                    "action_domain"
                ),

                F.lit(
                    "IDENTITY_AND_AD_HYGIENE_VALIDATION"
                ).alias(
                    "action_type"
                ),

                F.col(
                    "subject_key"
                ).cast("string").alias(
                    "subject_key"
                ),

                (
                    F.col(
                        "subject_name"
                    ).cast("string")
                    if "subject_name"
                    in ip004_actions_source.columns
                    else
                    F.col(
                        "subject_key"
                    ).cast("string")
                ).alias(
                    "subject_name"
                ),

                F.lit(None).cast(
                    "string"
                ).alias(
                    "asset_key"
                ),

                F.col(
                    "priority"
                ).cast("string").alias(
                    "priority"
                ),

                F.col(
                    "condition"
                ).cast("string").alias(
                    "condition"
                ),

                F.col(
                    "interpretation"
                ).cast("string").alias(
                    "interpretation"
                ),

                F.col(
                    "validation_responsibility"
                ).cast("string").alias(
                    "validation_responsibility"
                ),

                (
                    F.col(
                        "action_status"
                    ).cast("string")
                    if "action_status"
                    in ip004_actions_source.columns
                    else
                    F.lit(
                        "Requires Validation"
                    )
                ).alias(
                    "action_status"
                ),

                F.lit(
                    "IP-004"
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
        )

        ip004_action_count = (
            ip004_actions.count()
        )

        integrated_action_queue = (
            asset_action_queue
            .unionByName(
                ip004_actions,
                allowMissingColumns=True
            )
            .dropDuplicates(
                ["action_id"]
            )
        )


save_ip005(
    integrated_action_queue,
    "INTEGRATED_ACTION_QUEUE"
)


# ---------------------------------------------------------------------------
# 8. Taxonomy and queue certification
# ---------------------------------------------------------------------------

investigation_metrics = (
    investigation_universe
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
                    "primary_case_type"
                )
                ==
                "CONFIRMED_OPERATIONAL_BASELINE",
                1
            ).otherwise(0)
        ).alias(
            "confirmed_baseline_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "identity_investigation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "identity_investigation_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "security_tool_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "tool_validation_asset_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "ad_computer_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "ad_computer_validation_asset_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "open_port_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "open_port_validation_asset_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "data_governance_action_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "data_governance_asset_count"
        )
    )
    .first()
)


investigation_row_count = int(
    investigation_metrics[
        "row_count"
    ]
    or
    0
)

investigation_distinct_count = int(
    investigation_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)

confirmed_baseline_count = int(
    investigation_metrics[
        "confirmed_baseline_count"
    ]
    or
    0
)

identity_investigation_count = int(
    investigation_metrics[
        "identity_investigation_count"
    ]
    or
    0
)

identity_partition_count = (
    confirmed_baseline_count
    +
    identity_investigation_count
)

asset_action_count = (
    asset_action_queue.count()
)

integrated_action_count = (
    integrated_action_queue.count()
)

integrated_distinct_action_count = (
    integrated_action_queue
    .select("action_id")
    .distinct()
    .count()
)

if (
    investigation_row_count
    !=
    investigation_distinct_count
):
    raise RuntimeError(
        "Investigation universe is not one row per asset_key."
    )

if (
    identity_partition_count
    !=
    investigation_row_count
):
    raise RuntimeError(
        "Identity disposition does not partition the "
        "operational asset population."
    )

if (
    integrated_action_count
    !=
    integrated_distinct_action_count
):
    raise RuntimeError(
        "Integrated action queue contains duplicate action IDs."
    )


IP005_DECISION_PRODUCTS_READY = True


print("=" * 100)
print("RCAIP IP-005 - OPERATIONAL TAXONOMY AND DECISION PRODUCTS")
print("=" * 100)
print("Status: PASS")
print("Ready for final certification:", IP005_DECISION_PRODUCTS_READY)
print("Operational asset rows:", investigation_row_count)
print("Confirmed operational baseline:", confirmed_baseline_count)
print("Identity investigation assets:", identity_investigation_count)
print(
    "Security-tool validation assets:",
    int(
        investigation_metrics[
            "tool_validation_asset_count"
        ]
        or
        0
    )
)
print(
    "AD-computer validation assets:",
    int(
        investigation_metrics[
            "ad_computer_validation_asset_count"
        ]
        or
        0
    )
)
print(
    "Open-port validation assets:",
    int(
        investigation_metrics[
            "open_port_validation_asset_count"
        ]
        or
        0
    )
)
print(
    "Data-governance validation assets:",
    int(
        investigation_metrics[
            "data_governance_asset_count"
        ]
        or
        0
    )
)
print("Asset-level action rows:", asset_action_count)
print("IP-004 identity/AD action rows integrated:", ip004_action_count)
print("Integrated operational-management actions:", integrated_action_count)
print("Duplicate integrated action IDs: 0")
print("Automatic assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-005 - OPERATIONAL VIEW
# Cell 4 - Final runtime certification
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)


# ---------------------------------------------------------------------------
# 1. Decision-product contract
# ---------------------------------------------------------------------------

required_decision_objects = [
    "IP005_DECISION_PRODUCTS_READY",
    "IP005_INPUTS",
    "IP005_OUTPUT_TABLES",
    "resolve_output_table",
    "save_ip005"
]

missing_decision_objects = [
    object_name
    for object_name in required_decision_objects
    if object_name not in globals()
]

if missing_decision_objects:
    raise RuntimeError(
        "Run IP-005 Cells 2 and 3 before Cell 4. Missing: "
        + " | ".join(missing_decision_objects)
    )

if not IP005_DECISION_PRODUCTS_READY:
    raise RuntimeError(
        "IP-005 decision products are not certified."
    )


IP005_OUTPUT_TABLES[
    "FINAL_SUMMARY"
] = resolve_output_table(
    [
        "IP005_FINAL_SUMMARY"
    ],
    "ip005_final_summary"
)


required_output_keys = [
    "SOURCE_AVAILABILITY",
    "OPERATIONAL_ASSET_VIEW",
    "INVESTIGATION_UNIVERSE",
    "IDENTITY_QUEUE",
    "TOOL_QUEUE",
    "DATA_GOVERNANCE_QUEUE",
    "ENTERPRISE_ATTRIBUTION_QUEUE",
    "CONFIRMED_BASELINE",
    "ASSET_ACTION_QUEUE",
    "INTEGRATED_ACTION_QUEUE"
]


missing_output_keys = [
    output_key
    for output_key in required_output_keys
    if output_key not in IP005_OUTPUT_TABLES
]

if missing_output_keys:
    raise RuntimeError(
        "IP-005 output bindings are incomplete: "
        + " | ".join(missing_output_keys)
    )


missing_durable_outputs = [
    IP005_OUTPUT_TABLES[output_key]
    for output_key in required_output_keys
    if not spark.catalog.tableExists(
        IP005_OUTPUT_TABLES[output_key]
    )
]

if missing_durable_outputs:
    raise RuntimeError(
        "IP-005 durable outputs are missing: "
        + " | ".join(missing_durable_outputs)
    )


# ---------------------------------------------------------------------------
# 2. Load certified products
# ---------------------------------------------------------------------------

source_availability = spark.table(
    IP005_OUTPUT_TABLES[
        "SOURCE_AVAILABILITY"
    ]
)

operational_view = spark.table(
    IP005_OUTPUT_TABLES[
        "OPERATIONAL_ASSET_VIEW"
    ]
)

investigation_universe = spark.table(
    IP005_OUTPUT_TABLES[
        "INVESTIGATION_UNIVERSE"
    ]
)

identity_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "IDENTITY_QUEUE"
    ]
)

tool_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "TOOL_QUEUE"
    ]
)

data_governance_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "DATA_GOVERNANCE_QUEUE"
    ]
)

enterprise_attribution_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "ENTERPRISE_ATTRIBUTION_QUEUE"
    ]
)

confirmed_baseline = spark.table(
    IP005_OUTPUT_TABLES[
        "CONFIRMED_BASELINE"
    ]
)

asset_action_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "ASSET_ACTION_QUEUE"
    ]
)

integrated_action_queue = spark.table(
    IP005_OUTPUT_TABLES[
        "INTEGRATED_ACTION_QUEUE"
    ]
)


# ---------------------------------------------------------------------------
# 3. Population accounting in bounded aggregations
# ---------------------------------------------------------------------------

population_metrics = (
    investigation_universe
    .agg(
        F.count("*").alias(
            "operational_asset_rows"
        ),

        F.countDistinct(
            "asset_key"
        ).alias(
            "distinct_asset_keys"
        ),

        F.sum(
            F.when(
                F.col(
                    "current_evidence_flag"
                )
                &
                ~F.col(
                    "cmdb_present_flag"
                ),
                1
            ).otherwise(0)
        ).alias(
            "current_observed_not_cmdb"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "current_evidence_flag"
                )
                &
                F.col(
                    "stale_evidence_flag"
                )
                &
                ~F.col(
                    "cmdb_present_flag"
                ),
                1
            ).otherwise(0)
        ).alias(
            "stale_only_observed_not_cmdb"
        ),

        F.sum(
            F.when(
                F.col(
                    "unknown_freshness_flag"
                )
                &
                ~F.col(
                    "cmdb_present_flag"
                ),
                1
            ).otherwise(0)
        ).alias(
            "unknown_freshness_observed_not_cmdb"
        ),

        F.sum(
            F.when(
                F.col(
                    "cmdb_present_flag"
                )
                &
                ~F.col(
                    "current_evidence_flag"
                ),
                1
            ).otherwise(0)
        ).alias(
            "cmdb_no_active_evidence"
        ),

        F.sum(
            F.when(
                F.col(
                    "primary_case_type"
                )
                ==
                "CONFIRMED_OPERATIONAL_BASELINE",
                1
            ).otherwise(0)
        ).alias(
            "confirmed_baseline_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "identity_investigation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "identity_investigation_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "security_tool_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "tool_validation_asset_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "ad_computer_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "ad_computer_validation_asset_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "open_port_validation_required"
                ),
                1
            ).otherwise(0)
        ).alias(
            "open_port_validation_asset_count"
        ),

        F.sum(
            F.coalesce(
                F.col(
                    "tool_expectation_count"
                ),
                F.lit(0)
            )
        ).alias(
            "tool_expectation_records"
        ),

        F.sum(
            F.coalesce(
                F.col(
                    "tool_validation_required_count"
                ),
                F.lit(0)
            )
        ).alias(
            "tool_validation_records"
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
            ).otherwise(0)
        ).alias(
            "cross_post_rows"
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


operational_asset_rows = int(
    population_metrics[
        "operational_asset_rows"
    ]
    or
    0
)

distinct_asset_keys = int(
    population_metrics[
        "distinct_asset_keys"
    ]
    or
    0
)

confirmed_baseline_count = int(
    population_metrics[
        "confirmed_baseline_count"
    ]
    or
    0
)

identity_investigation_count = int(
    population_metrics[
        "identity_investigation_count"
    ]
    or
    0
)


queue_counts = {
    "identity_queue": identity_queue.count(),
    "tool_queue": tool_queue.count(),
    "data_governance_queue":
        data_governance_queue.count(),
    "enterprise_attribution_queue":
        enterprise_attribution_queue.count(),
    "confirmed_baseline":
        confirmed_baseline.count(),
    "asset_action_queue":
        asset_action_queue.count(),
    "integrated_action_queue":
        integrated_action_queue.count()
}


integrated_distinct_action_ids = (
    integrated_action_queue
    .select("action_id")
    .distinct()
    .count()
)


tool_input_count = (
    spark.table(
        IP005_INPUTS[
            "TOOL_COVERAGE"
        ]["table_name"]
    )
    .count()
)


ip004_action_count = 0

if IP005_INPUTS[
    "IP004_ACTION_QUEUE"
]["available"]:
    ip004_action_count = (
        spark.table(
            IP005_INPUTS[
                "IP004_ACTION_QUEUE"
            ]["table_name"]
        )
        .count()
    )


# ---------------------------------------------------------------------------
# 4. Certification gates
# ---------------------------------------------------------------------------

certification_checks = {
    "one_row_per_asset":
        operational_asset_rows
        ==
        distinct_asset_keys,

    "operational_population_preserved":
        operational_asset_rows
        ==
        operational_view.count(),

    "identity_partition_complete":
        operational_asset_rows
        ==
        (
            confirmed_baseline_count
            +
            identity_investigation_count
        ),

    "identity_queue_matches_flag":
        queue_counts[
            "identity_queue"
        ]
        ==
        identity_investigation_count,

    "confirmed_baseline_matches":
        queue_counts[
            "confirmed_baseline"
        ]
        ==
        confirmed_baseline_count,

    "tool_queue_matches_flag":
        queue_counts[
            "tool_queue"
        ]
        ==
        int(
            population_metrics[
                "tool_validation_asset_count"
            ]
            or
            0
        ),

    "enterprise_attribution_matches_case":
        queue_counts[
            "enterprise_attribution_queue"
        ]
        ==
        (
            investigation_universe
            .filter(
                F.col(
                    "primary_case_type"
                )
                ==
                "ENTERPRISE_ATTRIBUTION_CANDIDATE"
            )
            .count()
        ),

    "tool_record_accounting_preserved":
        int(
            population_metrics[
                "tool_expectation_records"
            ]
            or
            0
        )
        ==
        tool_input_count,

    "integrated_action_ids_unique":
        queue_counts[
            "integrated_action_queue"
        ]
        ==
        integrated_distinct_action_ids,

    "integrated_action_accounting":
        queue_counts[
            "integrated_action_queue"
        ]
        ==
        (
            queue_counts[
                "asset_action_queue"
            ]
            +
            ip004_action_count
        ),

    "cross_post_rows_zero":
        int(
            population_metrics[
                "cross_post_rows"
            ]
            or
            0
        )
        ==
        0,

    "cross_run_rows_zero":
        int(
            population_metrics[
                "cross_run_rows"
            ]
            or
            0
        )
        ==
        0
}


failed_certification_checks = [
    check_name
    for check_name, passed
    in certification_checks.items()
    if not passed
]

if failed_certification_checks:
    raise RuntimeError(
        "IP-005 final certification failed: "
        + " | ".join(
            failed_certification_checks
        )
    )


# ---------------------------------------------------------------------------
# 5. Durable final summary
# ---------------------------------------------------------------------------

summary_values = {
    "operational_asset_rows":
        operational_asset_rows,

    "distinct_asset_keys":
        distinct_asset_keys,

    "current_observed_not_cmdb":
        int(
            population_metrics[
                "current_observed_not_cmdb"
            ]
            or
            0
        ),

    "stale_only_observed_not_cmdb":
        int(
            population_metrics[
                "stale_only_observed_not_cmdb"
            ]
            or
            0
        ),

    "unknown_freshness_observed_not_cmdb":
        int(
            population_metrics[
                "unknown_freshness_observed_not_cmdb"
            ]
            or
            0
        ),

    "cmdb_no_active_evidence":
        int(
            population_metrics[
                "cmdb_no_active_evidence"
            ]
            or
            0
        ),

    "confirmed_baseline_count":
        confirmed_baseline_count,

    "identity_investigation_count":
        identity_investigation_count,

    "tool_expectation_records":
        int(
            population_metrics[
                "tool_expectation_records"
            ]
            or
            0
        ),

    "tool_validation_records":
        int(
            population_metrics[
                "tool_validation_records"
            ]
            or
            0
        ),

    "tool_validation_asset_count":
        int(
            population_metrics[
                "tool_validation_asset_count"
            ]
            or
            0
        ),

    "ad_computer_validation_asset_count":
        int(
            population_metrics[
                "ad_computer_validation_asset_count"
            ]
            or
            0
        ),

    "open_port_validation_asset_count":
        int(
            population_metrics[
                "open_port_validation_asset_count"
            ]
            or
            0
        ),

    "asset_action_queue_count":
        queue_counts[
            "asset_action_queue"
        ],

    "ip004_action_count":
        ip004_action_count,

    "integrated_action_queue_count":
        queue_counts[
            "integrated_action_queue"
        ],

    "durable_output_count":
        len(
            required_output_keys
        )
        +
        1,

    "missing_durable_output_count":
        0,

    "cross_post_row_count":
        0,

    "cross_run_row_count":
        0
}


summary_schema = StructType([
    StructField(
        "metric_name",
        StringType(),
        False
    ),
    StructField(
        "metric_value",
        LongType(),
        False
    )
])


final_summary = spark.createDataFrame(
    [
        (
            metric_name,
            int(metric_value)
        )
        for metric_name, metric_value
        in summary_values.items()
    ],
    schema=summary_schema
)


save_ip005(
    final_summary,
    "FINAL_SUMMARY"
)


# ---------------------------------------------------------------------------
# 6. Final runtime contract
# ---------------------------------------------------------------------------

optional_source_status = {
    row["source_key"]: (
        "AVAILABLE"
        if row["available"]
        else
        "OPTIONAL_NOT_AVAILABLE"
    )
    for row in (
        source_availability
        .filter(
            ~F.col("required")
        )
        .select(
            "source_key",
            "available"
        )
        .collect()
    )
}


IP005_READY = True


print("=" * 100)
print("RCAIP IP-005 - FINAL RUNTIME CONTRACT")
print("=" * 100)
print("Status: PASS")
print("Ready:", IP005_READY)
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
print("Operational asset rows:", operational_asset_rows)
print("Distinct asset keys:", distinct_asset_keys)
print(
    "Current observed-not-CMDB:",
    int(
        population_metrics[
            "current_observed_not_cmdb"
        ]
        or
        0
    )
)
print(
    "Stale-only observed-not-CMDB:",
    int(
        population_metrics[
            "stale_only_observed_not_cmdb"
        ]
        or
        0
    )
)
print(
    "Unknown-freshness observed-not-CMDB:",
    int(
        population_metrics[
            "unknown_freshness_observed_not_cmdb"
        ]
        or
        0
    )
)
print(
    "CMDB identities with no active evidence:",
    int(
        population_metrics[
            "cmdb_no_active_evidence"
        ]
        or
        0
    )
)
print("Confirmed operational baseline:", confirmed_baseline_count)
print("Identity investigation assets:", identity_investigation_count)
print(
    "Asset-tool expectation records:",
    int(
        population_metrics[
            "tool_expectation_records"
        ]
        or
        0
    )
)
print(
    "Asset-tool records requiring validation:",
    int(
        population_metrics[
            "tool_validation_records"
        ]
        or
        0
    )
)
print(
    "Assets with tool relationships requiring validation:",
    int(
        population_metrics[
            "tool_validation_asset_count"
        ]
        or
        0
    )
)
print(
    "Assets with AD-computer validation:",
    int(
        population_metrics[
            "ad_computer_validation_asset_count"
        ]
        or
        0
    )
)
print(
    "Assets with open-port validation:",
    int(
        population_metrics[
            "open_port_validation_asset_count"
        ]
        or
        0
    )
)
print("Asset-level action rows:", queue_counts["asset_action_queue"])
print("IP-004 identity/AD action rows:", ip004_action_count)
print(
    "Integrated operational-management actions:",
    queue_counts[
        "integrated_action_queue"
    ]
)
print(
    "Open-port source:",
    optional_source_status.get(
        "OPEN_PORTS",
        "OPTIONAL_NOT_AVAILABLE"
    )
)
print(
    "Go Manager source:",
    optional_source_status.get(
        "GO_MANAGER",
        "OPTIONAL_NOT_AVAILABLE"
    )
)
print(
    "Durable outputs:",
    len(
        required_output_keys
    )
    +
    1
)
print("Missing durable outputs: 0")
print("Cross-Post rows: 0")
print("Cross-run rows: 0")
print("Automatic assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)


display(
    investigation_universe
    .groupBy(
        "primary_case_type",
        "identity_priority",
        "recommended_owner_scope"
    )
    .agg(
        F.count("*").alias(
            "asset_count"
        )
    )
    .orderBy(
        F.when(
            F.col("identity_priority")
            ==
            "High",
            1
        )
        .when(
            F.col("identity_priority")
            ==
            "Medium",
            2
        )
        .when(
            F.col("identity_priority")
            ==
            "Review",
            3
        )
        .otherwise(4),
        F.col("asset_count").desc()
    )
)


display(
    integrated_action_queue
    .groupBy(
        "source_module",
        "action_domain",
        "priority"
    )
    .agg(
        F.count("*").alias(
            "action_count"
        )
    )
    .orderBy(
        "source_module",
        "action_domain",
        "priority"
    )
)