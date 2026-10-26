# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry
# MAGIC

# COMMAND ----------

# ============================================================================
# RCAIP ASSET ENGINE V3
# Registry-Driven Enterprise Asset Plane
#
# Purpose:
#   Normalize the certified Post's CMDB and Qualys files, scope the shared
#   Tenable, Intune, and MDE sources, consume the Nmap evidence already
#   certified by IP-001, and construct the durable RCAIP Asset Plane.
#
# Source ownership:
#   IP-001 owns:
#       NMAP_SCAN_PROVENANCE
#       NMAP_NORM
#       NMAP_NORM_WITH_PROVENANCE
#       NMAP_PORTS_ALL
#       NMAP_PORTS_ALL_WITH_PROVENANCE
#
#   Asset Engine consumes those Nmap products. It does not parse XML and does
#   not replace any Nmap table.
#
# Control-evidence contract amendment: v2.3.0-preflight (Intune + MDE)
#
# Asset Engine writes:
#       CMDB_NORM
#       TENABLE_NORM
#       INTUNE_NORM
#       MDE_NORM
#       QUALYS_NORM
#       OBSERVED_ASSETS
#       OBSERVED_ASSETS_ROLLUP
#       SOURCE_LIFECYCLE_COUNTS
#       STALE_OBSERVED_ASSETS
#       ASSET_RECONCILIATION
#       ASSET_RECONCILIATION_SCORED
#       OBSERVED_NOT_CMDB
#
# Operating rules:
#   - No hard-coded Post identity.
#   - No widgets.
#   - No cell-skipping instructions.
#   - No example_post_d parity or Delta-history recovery logic.
#   - All durable names resolve through Config 001.
#   - Run All safe.
#   - Every durable write becomes a materialized Delta execution boundary.
#   - Enterprise source lineages are never intentionally recomputed downstream.
#   - Join inputs must remain one row per non-null asset_key.
#   - No automatic findings, assignments, tickets, or remediation.
# ============================================================================

import ipaddress
import re
import time

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate the inherited Config 000 / Config 001 runtime
# ---------------------------------------------------------------------------

required_runtime_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_POST_DEVICE_PREFIX",
    "RCAIP_POST_TOKENS",
    "RCAIP_DNS_TOKENS",
    "RCAIP_USER_LOCATION_TOKENS",
    "RCAIP_MDE_TOKENS",
    "RCAIP_ENTERPRISE_CIDRS",
    "RCAIP_INPUT_BASE_DIR",
    "RCAIP_CMDB_FILE",
    "RCAIP_QUALYS_EASM_FILE",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_RUN_MODE",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "RCAIP_RESOLVED_SOURCE_TABLES",
    "RCAIP_TENABLE_TABLE",
    "RCAIP_INTUNE_TABLE",
    "RCAIP_MDE_TABLE",
    "get_rcaip_table"
]

missing_runtime_objects = [
    object_name
    for object_name in required_runtime_objects
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "Asset Engine did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready runtime."
    )

allowed_asset_engine_modes = {
    "DISCOVERY",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}

if str(RCAIP_RUN_MODE).upper() not in allowed_asset_engine_modes:
    raise RuntimeError(
        "Asset Engine cannot run in the current certified mode. "
        f"Observed mode: {RCAIP_RUN_MODE}"
    )

ASSET_ENGINE_GENERATED_AT_UTC = F.current_timestamp()


# ---------------------------------------------------------------------------
# 2. Resolve registered table contracts
# ---------------------------------------------------------------------------

ASSET_ENGINE_INPUT_LOGICAL_TABLES = [
    "NMAP_SCAN_PROVENANCE",
    "NMAP_NORM"
]

ASSET_ENGINE_SOURCE_OUTPUTS = [
    "CMDB_NORM",
    "TENABLE_NORM",
    "INTUNE_NORM",
    "MDE_NORM",
    "QUALYS_NORM"
]

ASSET_ENGINE_ASSET_PLANE_OUTPUTS = [
    "OBSERVED_ASSETS",
    "OBSERVED_ASSETS_ROLLUP",
    "SOURCE_LIFECYCLE_COUNTS",
    "STALE_OBSERVED_ASSETS",
    "ASSET_RECONCILIATION",
    "ASSET_RECONCILIATION_SCORED",
    "OBSERVED_NOT_CMDB"
]

ASSET_ENGINE_ALL_OUTPUTS = (
    ASSET_ENGINE_SOURCE_OUTPUTS
    +
    ASSET_ENGINE_ASSET_PLANE_OUTPUTS
)

ASSET_ENGINE_INPUT_TABLES = {
    logical_name:
        get_rcaip_table(logical_name)

    for logical_name
    in ASSET_ENGINE_INPUT_LOGICAL_TABLES
}

ASSET_ENGINE_OUTPUT_TABLES = {
    logical_name:
        get_rcaip_table(logical_name)

    for logical_name
    in ASSET_ENGINE_ALL_OUTPUTS
}

expected_post_table_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    f"{RCAIP_POST_SLUG}_"
)

invalid_output_bindings = {
    logical_name:
        physical_table_name

    for (
        logical_name,
        physical_table_name
    )
    in ASSET_ENGINE_OUTPUT_TABLES.items()

    if not str(physical_table_name).lower().startswith(
        expected_post_table_prefix.lower()
    )
}

if invalid_output_bindings:
    raise RuntimeError(
        "Asset Engine output registry contains a cross-Post binding: "
        +
        " | ".join(
            (
                f"{logical_name}="
                f"{physical_table_name}"
            )

            for (
                logical_name,
                physical_table_name
            )
            in invalid_output_bindings.items()
        )
    )

missing_ip001_inputs = [
    physical_table_name

    for physical_table_name
    in ASSET_ENGINE_INPUT_TABLES.values()

    if not spark.catalog.tableExists(
        physical_table_name
    )
]

if missing_ip001_inputs:
    raise RuntimeError(
        "Asset Engine requires the certified IP-001 Nmap products. "
        "Run RCAIP_IP_001_Open_Port_Intelligence v2.0 first. Missing: "
        +
        " | ".join(
            missing_ip001_inputs
        )
    )


# ---------------------------------------------------------------------------
# 3. Resolve source locations
# ---------------------------------------------------------------------------

def dbfs_path_join(
    base_path,
    file_name
):
    return (
        str(base_path).rstrip("/")
        +
        "/"
        +
        str(file_name).lstrip("/")
    )


RCAIP_CMDB_PATH = (
    globals().get("RCAIP_CMDB_PATH")
    or
    dbfs_path_join(
        RCAIP_INPUT_BASE_DIR,
        RCAIP_CMDB_FILE
    )
)

RCAIP_QUALYS_EASM_PATH = (
    globals().get("RCAIP_QUALYS_EASM_PATH")
    or
    (
        dbfs_path_join(
            RCAIP_INPUT_BASE_DIR,
            RCAIP_QUALYS_EASM_FILE
        )
        if RCAIP_QUALYS_EASM_FILE
        else None
    )
)

tenable_source_table = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "tenable_assets"
    )
    or
    RCAIP_TENABLE_TABLE
)

intune_source_table = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "intune_devices"
    )
    or
    RCAIP_INTUNE_TABLE
)

mde_source_table = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "mde_device_info"
    )
    or
    RCAIP_MDE_TABLE
)


# ---------------------------------------------------------------------------
# 4. Generic helpers
# ---------------------------------------------------------------------------

def clean_column_name(
    column_name
):
    cleaned_name = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(column_name).strip().lower()
    ).strip("_")

    return cleaned_name or "unnamed_column"


def clean_dataframe_columns(
    dataframe
):
    used_column_names = set()
    cleaned_column_names = []

    for original_column_name in dataframe.columns:
        base_name = clean_column_name(
            original_column_name
        )

        candidate_name = base_name
        suffix_number = 2

        while candidate_name in used_column_names:
            candidate_name = (
                f"{base_name}_"
                f"{suffix_number}"
            )

            suffix_number += 1

        used_column_names.add(
            candidate_name
        )

        cleaned_column_names.append(
            candidate_name
        )

    return dataframe.toDF(
        *cleaned_column_names
    )


def first_existing_column(
    dataframe,
    candidate_names
):
    column_lookup = {
        str(column_name).lower():
            column_name

        for column_name
        in dataframe.columns
    }

    for candidate_name in candidate_names:
        matched_column = column_lookup.get(
            str(candidate_name).lower()
        )

        if matched_column:
            return matched_column

    return None


def blank_to_null(
    column_expression
):
    return (
        F.when(
            F.trim(
                column_expression.cast("string")
            )
            ==
            F.lit(""),
            F.lit(None).cast("string")
        )
        .otherwise(
            F.trim(
                column_expression.cast("string")
            )
        )
    )


def text_expression(
    dataframe,
    candidate_names
):
    column_name = first_existing_column(
        dataframe,
        candidate_names
    )

    if column_name is None:
        return F.lit(
            None
        ).cast(
            "string"
        )

    return blank_to_null(
        F.col(
            f"`{column_name}`"
        )
    )


def timestamp_expression(
    dataframe,
    candidate_names
):
    """
    Return the first safely parsed timestamp from the candidate source field.

    RCAIP guardrail:
      Malformed or non-date source values become NULL rather than failing
      the Asset Engine. Unknown timestamp evidence remains Unknown; it is
      not silently treated as current.
    """

    column_name = first_existing_column(
        dataframe,
        candidate_names
    )

    if column_name is None:
        return F.lit(
            None
        ).cast(
            "timestamp"
        )

    escaped_column_name = str(
        column_name
    ).replace(
        "`",
        "``"
    )

    source_sql = (
        f"`{escaped_column_name}`"
    )

    return F.coalesce(

        # Normal timestamp/date/string values.
        # TRY_CAST returns NULL instead of raising SparkDateTimeException.
        F.expr(
            f"try_cast({source_sql} AS timestamp)"
        ),

        # Preserve the historical generic string-parser fallback.
        F.expr(
            f"try_to_timestamp("
            f"cast({source_sql} AS string)"
            f")"
        ),

        # Preserve the historical Unix-time fallback, but make the numeric
        # conversion tolerant as well.
        F.expr(
            f"try_to_timestamp("
            f"from_unixtime("
            f"try_cast({source_sql} AS double)"
            f")"
            f")"
        )
    )


def normalized_host_key(
    source_expression
):
    first_dns_label = F.regexp_extract(
        F.trim(
            source_expression.cast("string")
        ),
        r"^([^.]+)",
        1
    )

    normalized_value = F.upper(
        F.regexp_replace(
            first_dns_label,
            r"[^A-Za-z0-9_-]",
            ""
        )
    )

    return (
        F.when(
            F.length(
                normalized_value
            )
            >
            F.lit(0),
            normalized_value
        )
        .otherwise(
            F.lit(None).cast("string")
        )
    )


def normalized_serial(
    source_expression
):
    normalized_value = F.upper(
        F.regexp_replace(
            F.trim(
                source_expression.cast("string")
            ),
            r"\s+",
            ""
        )
    )

    return (
        F.when(
            F.length(
                normalized_value
            )
            >
            F.lit(0),
            normalized_value
        )
        .otherwise(
            F.lit(None).cast("string")
        )
    )


enterprise_networks = [
    ipaddress.ip_network(
        enterprise_cidr,
        strict=False
    )

    for enterprise_cidr
    in RCAIP_ENTERPRISE_CIDRS
]

enterprise_ipv4_ranges = [
    (
        int(enterprise_network.network_address),
        int(enterprise_network.broadcast_address)
    )

    for enterprise_network
    in enterprise_networks

    if enterprise_network.version == 4
]

enterprise_ipv6_networks = [
    enterprise_network

    for enterprise_network
    in enterprise_networks

    if enterprise_network.version == 6
]


def ipv4_to_long_expression(
    raw_ip_expression
):
    """
    Convert a valid dotted-quad IPv4 value to a native Spark long.

    RCAIP guardrail:
      Invalid, blank, IPv6, or malformed values return NULL.
      Array access is tolerant under Spark ANSI mode.
    """

    ipv4_text = F.regexp_extract(
        F.trim(
            raw_ip_expression.cast("string")
        ),
        r"^([0-9]{1,3}(?:\.[0-9]{1,3}){3})$",
        1
    )

    valid_ipv4_shape = (
        F.length(
            ipv4_text
        )
        >
        F.lit(0)
    )

    ipv4_parts = F.split(
        ipv4_text,
        r"\."
    )

    # Spark get() is zero-based and returns NULL for an
    # out-of-bounds array index instead of raising an exception.
    octets = [
        F.when(
            valid_ipv4_shape,
            F.get(
                ipv4_parts,
                octet_index
            ).cast(
                "long"
            )
        ).otherwise(
            F.lit(
                None
            ).cast(
                "long"
            )
        )

        for octet_index
        in range(4)
    ]

    valid_ipv4 = valid_ipv4_shape

    for octet_expression in octets:
        valid_ipv4 = (
            valid_ipv4
            &
            octet_expression.between(
                0,
                255
            )
        )

    ipv4_long = (
        octets[0] * F.lit(16777216)
        +
        octets[1] * F.lit(65536)
        +
        octets[2] * F.lit(256)
        +
        octets[3]
    )

    return F.when(
        valid_ipv4,
        ipv4_long
    ).otherwise(
        F.lit(
            None
        ).cast(
            "long"
        )
    )


if enterprise_ipv6_networks:
    @F.udf(
        returnType=T.BooleanType()
    )
    def ipv6_in_enterprise_scope_udf(
        raw_ip_address
    ):
        if raw_ip_address is None:
            return False

        try:
            parsed_ip_address = ipaddress.ip_address(
                str(raw_ip_address).strip()
            )

            return any(
                parsed_ip_address in enterprise_network
                for enterprise_network in enterprise_ipv6_networks
            )

        except Exception:
            return False


def ip_in_enterprise_scope(
    raw_ip_expression
):
    """Return a native Spark predicate for current enterprise CIDRs.

    IPv4 evaluation remains entirely in the Spark JVM and therefore permits
    Catalyst optimization. A Python UDF is used only when Config 000 actually
    contains an IPv6 network.
    """

    enterprise_scope_expression = F.lit(
        False
    )

    if enterprise_ipv4_ranges:
        ipv4_long = ipv4_to_long_expression(
            raw_ip_expression
        )

        for (
            network_start,
            network_end
        ) in enterprise_ipv4_ranges:
            enterprise_scope_expression = (
                enterprise_scope_expression
                |
                ipv4_long.between(
                    F.lit(network_start),
                    F.lit(network_end)
                )
            )

    if enterprise_ipv6_networks:
        enterprise_scope_expression = (
            enterprise_scope_expression
            |
            ipv6_in_enterprise_scope_udf(
                raw_ip_expression
            )
        )

    return F.coalesce(
        enterprise_scope_expression,
        F.lit(False)
    )


def build_token_regex(
    token_values
):
    normalized_tokens = sorted({
        str(token_value).strip().lower()

        for token_value
        in token_values

        if str(token_value or "").strip()
    })

    if not normalized_tokens:
        return r"(?!)"

    return "|".join(
        re.escape(token_value)
        for token_value in normalized_tokens
    )


post_token_regex = build_token_regex(
    [
        *RCAIP_POST_TOKENS,
        *RCAIP_DNS_TOKENS,
        *RCAIP_USER_LOCATION_TOKENS,
        *RCAIP_MDE_TOKENS
    ]
)

post_device_prefix_upper = str(
    RCAIP_POST_DEVICE_PREFIX or ""
).strip().upper()


def add_lifecycle_columns(
    dataframe,
    timestamp_column_name
):
    assessment_end_date = F.to_date(
        F.lit(
            RCAIP_ASSESSMENT_END_DATE_TEXT
        )
    )

    output_dataframe = (
        dataframe

        .withColumn(
            "last_observed_timestamp",
            F.col(
                timestamp_column_name
            ).cast(
                "timestamp"
            )
        )

        .withColumn(
            "days_since_observed",
            F.when(
                F.col(
                    "last_observed_timestamp"
                ).isNotNull(),

                F.datediff(
                    assessment_end_date,
                    F.to_date(
                        F.col(
                            "last_observed_timestamp"
                        )
                    )
                )
            )
            .otherwise(
                F.lit(None).cast("integer")
            )
        )

        .withColumn(
            "observation_age_bucket",

            F.when(
                F.col(
                    "days_since_observed"
                ).isNull(),
                F.lit(
                    "Unknown"
                )
            )

            .when(
                F.col(
                    "days_since_observed"
                )
                <
                F.lit(0),
                F.lit(
                    "Future Date Review"
                )
            )

            .when(
                F.col(
                    "days_since_observed"
                )
                <=
                F.lit(30),
                F.lit(
                    "Current"
                )
            )

            .when(
                F.col(
                    "days_since_observed"
                )
                <=
                F.lit(90),
                F.lit(
                    "Recent"
                )
            )

            .otherwise(
                F.lit(
                    "Stale"
                )
            )
        )

        .withColumn(
            "is_stale_90",

            F.when(
                F.col(
                    "days_since_observed"
                )
                >
                F.lit(90),
                F.lit(
                    "Y"
                )
            )

            .when(
                F.col(
                    "days_since_observed"
                ).isNotNull(),
                F.lit(
                    "N"
                )
            )

            .otherwise(
                F.lit(
                    "Unknown"
                )
            )
        )
    )

    return output_dataframe


def deduplicate_by_asset(
    dataframe,
    timestamp_column_name="last_observed_timestamp"
):
    ordering_column = (
        F.col(
            timestamp_column_name
        ).desc_nulls_last()

        if timestamp_column_name in dataframe.columns

        else F.lit(1)
    )

    asset_window = (
        Window
        .partitionBy(
            "asset_key"
        )
        .orderBy(
            ordering_column
        )
    )

    return (
        dataframe

        .filter(
            F.col(
                "asset_key"
            ).isNotNull()
        )

        .withColumn(
            "_asset_row_number",
            F.row_number().over(
                asset_window
            )
        )

        .filter(
            F.col(
                "_asset_row_number"
            )
            ==
            F.lit(1)
        )

        .drop(
            "_asset_row_number"
        )
    )


def add_runtime_columns(
    dataframe
):
    output_dataframe = dataframe

    runtime_values = {
        "post_name":
            RCAIP_POST_NAME,

        "post_slug":
            RCAIP_POST_SLUG,

        "rcaip_run_id":
            RCAIP_RUN_ID,

        "runtime_country_name":
            RCAIP_COUNTRY_NAME,

        "runtime_run_mode":
            RCAIP_RUN_MODE
    }

    for (
        column_name,
        column_value
    ) in runtime_values.items():

        if column_name not in output_dataframe.columns:
            output_dataframe = (
                output_dataframe
                .withColumn(
                    column_name,
                    F.lit(
                        column_value
                    )
                )
            )

    if "generated_at_utc" not in output_dataframe.columns:
        output_dataframe = (
            output_dataframe
            .withColumn(
                "generated_at_utc",
                ASSET_ENGINE_GENERATED_AT_UTC
            )
        )

    return output_dataframe


ASSET_ENGINE_WRITTEN_TABLES = []
ASSET_ENGINE_OUTPUT_DATAFRAMES = {}
ASSET_ENGINE_WRITE_TIMINGS = []


def save_asset_engine_table(
    dataframe,
    logical_name
):
    """Write one durable product and return a fresh table-backed DataFrame.

    The previous implementation returned no DataFrame, so every downstream
    action retained the complete lineage to the enterprise source tables. That
    caused Tenable, Intune, and MDE to be rescanned repeatedly by later writes,
    counts, displays, and certification checks. Re-reading the just-written
    Delta product creates an explicit execution boundary.
    """

    if logical_name not in ASSET_ENGINE_OUTPUT_TABLES:
        raise KeyError(
            "Asset Engine attempted to write an unregistered table: "
            +
            logical_name
        )

    physical_table_name = ASSET_ENGINE_OUTPUT_TABLES[
        logical_name
    ]

    if not physical_table_name.lower().startswith(
        expected_post_table_prefix.lower()
    ):
        raise RuntimeError(
            "Cross-Post output protection blocked: "
            +
            physical_table_name
        )

    output_dataframe = add_runtime_columns(
        dataframe
    )

    write_started_at = time.perf_counter()

    (
        output_dataframe.write
        .mode(
            "overwrite"
        )
        .format(
            "delta"
        )
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            physical_table_name
        )
    )

    write_elapsed_seconds = (
        time.perf_counter()
        -
        write_started_at
    )

    durable_dataframe = spark.table(
        physical_table_name
    )

    ASSET_ENGINE_WRITTEN_TABLES.append(
        physical_table_name
    )

    ASSET_ENGINE_OUTPUT_DATAFRAMES[
        logical_name
    ] = durable_dataframe

    ASSET_ENGINE_WRITE_TIMINGS.append({
        "logical_name": logical_name,
        "physical_table_name": physical_table_name,
        "write_elapsed_seconds": float(
            write_elapsed_seconds
        )
    })

    print(
        "Saved:",
        physical_table_name,
        "| write seconds:",
        round(
            write_elapsed_seconds,
            2
        )
    )

    return durable_dataframe


def empty_dataframe(
    schema_definition
):
    return spark.createDataFrame(
        [],
        schema=schema_definition
    )


# ---------------------------------------------------------------------------
# 5. Normalize CMDB
# ---------------------------------------------------------------------------

cmdb_raw = clean_dataframe_columns(
    (
        spark.read
        .option(
            "header",
            True
        )
        .option(
            "inferSchema",
            True
        )
        .csv(
            RCAIP_CMDB_PATH
        )
    )
)

cmdb_stage = (
    cmdb_raw

    .withColumn(
        "cmdb_name",
        text_expression(
            cmdb_raw,
            [
                "name",
                "device_name",
                "computer_name",
                "hostname",
                "host_name",
                "ci_name",
                "asset_name"
            ]
        )
    )

    .withColumn(
        "cmdb_fqdn",
        text_expression(
            cmdb_raw,
            [
                "fully_qualified_domain_name",
                "fqdn",
                "dns_name",
                "dns_host_name"
            ]
        )
    )

    .withColumn(
        "cmdb_ip",
        text_expression(
            cmdb_raw,
            [
                "ip_address",
                "ipv4_address",
                "ipv4",
                "ip"
            ]
        )
    )

    .withColumn(
        "cmdb_serial_number",
        text_expression(
            cmdb_raw,
            [
                "serial_number",
                "serialnumber",
                "serial",
                "bios_serial_number"
            ]
        )
    )

    .withColumn(
        "cmdb_class",
        text_expression(
            cmdb_raw,
            [
                "sys_class_name",
                "class",
                "ci_class",
                "device_class"
            ]
        )
    )

    .withColumn(
        "cmdb_location",
        text_expression(
            cmdb_raw,
            [
                "location",
                "location_name",
                "site",
                "site_name"
            ]
        )
    )

    .withColumn(
        "cmdb_category",
        text_expression(
            cmdb_raw,
            [
                "category",
                "subcategory",
                "asset_category"
            ]
        )
    )

    .withColumn(
        "cmdb_mission",
        text_expression(
            cmdb_raw,
            [
                "mission",
                "post",
                "post_name",
                "organization"
            ]
        )
    )

    .withColumn(
        "cmdb_managed_by_group",
        text_expression(
            cmdb_raw,
            [
                "managed_by_group",
                "managed_by",
                "management_group"
            ]
        )
    )

    .withColumn(
        "cmdb_support_group",
        text_expression(
            cmdb_raw,
            [
                "support_group",
                "assignment_group",
                "support_team"
            ]
        )
    )

    .withColumn(
        "cmdb_install_status",
        text_expression(
            cmdb_raw,
            [
                "install_status",
                "operational_status",
                "status"
            ]
        )
    )

    .withColumn(
        "cmdb_discovery_source",
        text_expression(
            cmdb_raw,
            [
                "discovery_source",
                "source",
                "data_source"
            ]
        )
    )

    .withColumn(
        "cmdb_most_recent_discovery",
        timestamp_expression(
            cmdb_raw,
            [
                "most_recent_discovery",
                "last_discovered",
                "last_discovery",
                "last_seen",
                "updated",
                "updated_at",
                "sys_updated_on"
            ]
        )
    )

    .withColumn(
        "cmdb_host_key",
        normalized_host_key(
            F.col(
                "cmdb_name"
            )
        )
    )

    .withColumn(
        "cmdb_fqdn_host_key",
        normalized_host_key(
            F.col(
                "cmdb_fqdn"
            )
        )
    )

    .withColumn(
        "host_key",
        F.coalesce(
            F.col(
                "cmdb_host_key"
            ),
            F.col(
                "cmdb_fqdn_host_key"
            )
        )
    )

    .withColumn(
        "asset_key",
        F.coalesce(
            F.col(
                "host_key"
            ),

            normalized_serial(
                F.col(
                    "cmdb_serial_number"
                )
            ),

            F.col(
                "cmdb_ip"
            )
        )
    )

    .withColumn(
        "post_name",
        F.lit(
            RCAIP_POST_NAME
        )
    )

    .withColumn(
        "post_slug",
        F.lit(
            RCAIP_POST_SLUG
        )
    )

    .withColumn(
        "rcaip_run_id",
        F.lit(
            RCAIP_RUN_ID
        )
    )

    .withColumn(
        "asset_domain",
        F.lit(
            "Enterprise"
        )
    )

    .withColumn(
        "source_system",
        F.lit(
            "CMDB"
        )
    )

    .withColumn(
        "cmdb_present",
        F.lit(
            "Y"
        )
    )

    .withColumn(
        "raw_source_table",
        F.lit(
            RCAIP_CMDB_PATH
        )
    )
)

cmdb_norm = add_lifecycle_columns(
    cmdb_stage,
    "cmdb_most_recent_discovery"
)

cmdb_norm = deduplicate_by_asset(
    cmdb_norm
).select(
    "post_name",
    "post_slug",
    "rcaip_run_id",
    "asset_domain",
    "asset_key",
    "host_key",
    "source_system",
    "cmdb_present",

    "cmdb_name",
    "cmdb_fqdn",
    "cmdb_ip",
    "cmdb_serial_number",
    "cmdb_host_key",
    "cmdb_fqdn_host_key",

    "cmdb_class",
    "cmdb_location",
    "cmdb_category",
    "cmdb_mission",
    "cmdb_managed_by_group",
    "cmdb_support_group",
    "cmdb_install_status",
    "cmdb_most_recent_discovery",
    "cmdb_discovery_source",

    "last_observed_timestamp",
    "days_since_observed",
    "observation_age_bucket",
    "is_stale_90",
    "raw_source_table"
)

cmdb_norm = save_asset_engine_table(
    cmdb_norm,
    "CMDB_NORM"
)


# ---------------------------------------------------------------------------
# 6. Consume IP-001 Nmap evidence
# ---------------------------------------------------------------------------

nmap_norm_input = spark.table(
    ASSET_ENGINE_INPUT_TABLES[
        "NMAP_NORM"
    ]
)

nmap_provenance_input = spark.table(
    ASSET_ENGINE_INPUT_TABLES[
        "NMAP_SCAN_PROVENANCE"
    ]
)

required_nmap_columns = {
    "scan_id",
    "host_key",
    "ip_address",
    "nmap_hostname",
    "asset_domain",
    "scan_scope",
    "scan_location",
    "scan_source_file",
    "nmap_services",
    "nmap_ports",
    "open_port_count",
    "nmap_present"
}

missing_nmap_columns = sorted(
    required_nmap_columns
    -
    set(
        nmap_norm_input.columns
    )
)

if missing_nmap_columns:
    raise RuntimeError(
        "IP-001 NMAP_NORM is missing required Asset Engine columns: "
        +
        " | ".join(
            missing_nmap_columns
        )
    )

nmap_runtime_mismatch_count = (
    nmap_norm_input

    .filter(
        F.col(
            "rcaip_run_id"
        )
        !=
        F.lit(
            RCAIP_RUN_ID
        )
    )

    .count()
)

if nmap_runtime_mismatch_count:
    raise RuntimeError(
        "IP-001 Nmap evidence belongs to a different certified run. "
        f"Mismatched rows: {nmap_runtime_mismatch_count}"
    )

nmap_scan_dates = (
    nmap_provenance_input

    .select(
        "scan_id",

        F.coalesce(
            F.col(
                "scan_end_timestamp"
            ),
            F.col(
                "scan_start_timestamp"
            )
        ).alias(
            "nmap_last_observed_timestamp"
        )
    )

    .dropDuplicates(
        [
            "scan_id"
        ]
    )
)

nmap_asset_source = (
    nmap_norm_input

    .join(
        nmap_scan_dates,
        on="scan_id",
        how="left"
    )

    .withColumn(
        "asset_key",
        F.coalesce(
            F.col(
                "host_key"
            ),

            normalized_serial(
                F.col(
                    "mac_address"
                )
            ),

            F.col(
                "ip_address"
            )
        )
    )

    .withColumn(
        "source_system",
        F.lit(
            "NMAP"
        )
    )

    .withColumn(
        "raw_source_table",
        F.lit(
            ASSET_ENGINE_INPUT_TABLES[
                "NMAP_NORM"
            ]
        )
    )
)

nmap_asset_source = add_lifecycle_columns(
    nmap_asset_source,
    "nmap_last_observed_timestamp"
)

nmap_asset_source = deduplicate_by_asset(
    nmap_asset_source
)


# ---------------------------------------------------------------------------
# 7. Normalize Tenable
# ---------------------------------------------------------------------------

tenable_schema = T.StructType([
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("rcaip_run_id", T.StringType(), False),
    T.StructField("asset_domain", T.StringType(), False),
    T.StructField("asset_key", T.StringType(), True),
    T.StructField("host_key", T.StringType(), True),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("tenable_present", T.StringType(), False),
    T.StructField("ip_address", T.StringType(), True),
    T.StructField("dns_name", T.StringType(), True),
    T.StructField("tenable_dvc_hostname", T.StringType(), True),
    T.StructField("tenable_event_time", T.TimestampType(), True),
    T.StructField("tenable_host_uuid", T.StringType(), True),
    T.StructField("tenable_acr_score", T.StringType(), True),
    T.StructField("tenable_key_drivers", T.StringType(), True),
    T.StructField("tenable_organization_name", T.StringType(), True),
    T.StructField("last_observed_timestamp", T.TimestampType(), True),
    T.StructField("days_since_observed", T.IntegerType(), True),
    T.StructField("observation_age_bucket", T.StringType(), False),
    T.StructField("is_stale_90", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), True)
])

tenable_source_available = bool(
    tenable_source_table
    and
    spark.catalog.tableExists(
        tenable_source_table
    )
)

if tenable_source_available:
    tenable_raw = clean_dataframe_columns(
        spark.table(
            tenable_source_table
        )
    )

    tenable_stage = (
        tenable_raw

        .withColumn(
            "ip_address",
            text_expression(
                tenable_raw,
                [
                    "raw_ip",
                    "tenable_ip",
                    "ip_address",
                    "ipv4_address",
                    "ip"
                ]
            )
        )

        .withColumn(
            "dns_name",
            text_expression(
                tenable_raw,
                [
                    "raw_dnsname",
                    "tenable_dns_name",
                    "dns_name",
                    "fqdn",
                    "hostname"
                ]
            )
        )

        .withColumn(
            "tenable_dvc_hostname",
            text_expression(
                tenable_raw,
                [
                    "dvc_hostname",
                    "tenable_dvc_hostname",
                    "device_hostname"
                ]
            )
        )

        .withColumn(
            "tenable_event_time",
            timestamp_expression(
                tenable_raw,
                [
                    "time",
                    "event_time",
                    "timestamp",
                    "last_seen",
                    "tenable_event_time",
                    "ingest_time"
                ]
            )
        )

        .withColumn(
            "tenable_host_uuid",
            text_expression(
                tenable_raw,
                [
                    "raw_hostuuid",
                    "tenable_host_uuid",
                    "host_uuid",
                    "uuid"
                ]
            )
        )

        .withColumn(
            "tenable_acr_score",
            text_expression(
                tenable_raw,
                [
                    "raw_acrscore",
                    "tenable_acr_score",
                    "acr_score"
                ]
            )
        )

        .withColumn(
            "tenable_key_drivers",
            text_expression(
                tenable_raw,
                [
                    "raw_keydrivers",
                    "tenable_key_drivers",
                    "key_drivers"
                ]
            )
        )

        .withColumn(
            "tenable_organization_name",
            text_expression(
                tenable_raw,
                [
                    "raw_organization_name",
                    "tenable_organization_name",
                    "organization_name"
                ]
            )
        )

        .withColumn(
            "host_key",
            normalized_host_key(
                F.col(
                    "dns_name"
                )
            )
        )

        .withColumn(
            "asset_key",
            F.coalesce(
                F.col(
                    "host_key"
                ),
                F.col(
                    "ip_address"
                )
            )
        )

        .withColumn(
            "_tenable_scope_blob",
            F.lower(
                F.concat_ws(
                    " ",
                    "dns_name",
                    "tenable_dvc_hostname",
                    "tenable_organization_name"
                )
            )
        )

        .filter(
            ip_in_enterprise_scope(
                F.col(
                    "ip_address"
                )
            )
            |
            F.col(
                "_tenable_scope_blob"
            ).rlike(
                post_token_regex
            )
        )

        .withColumn(
            "post_name",
            F.lit(
                RCAIP_POST_NAME
            )
        )

        .withColumn(
            "post_slug",
            F.lit(
                RCAIP_POST_SLUG
            )
        )

        .withColumn(
            "rcaip_run_id",
            F.lit(
                RCAIP_RUN_ID
            )
        )

        .withColumn(
            "asset_domain",
            F.lit(
                "Enterprise"
            )
        )

        .withColumn(
            "source_system",
            F.lit(
                "Tenable"
            )
        )

        .withColumn(
            "tenable_present",
            F.lit(
                "Y"
            )
        )

        .withColumn(
            "raw_source_table",
            F.lit(
                tenable_source_table
            )
        )
    )

    tenable_norm = add_lifecycle_columns(
        tenable_stage,
        "tenable_event_time"
    )

    tenable_norm = deduplicate_by_asset(
        tenable_norm
    ).select(
        *[
            schema_field.name
            for schema_field in tenable_schema.fields
        ]
    )

else:
    tenable_norm = empty_dataframe(
        tenable_schema
    )

tenable_norm = save_asset_engine_table(
    tenable_norm,
    "TENABLE_NORM"
)


# ---------------------------------------------------------------------------
# 8. Normalize Intune
# ---------------------------------------------------------------------------

intune_schema = T.StructType([
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("rcaip_run_id", T.StringType(), False),
    T.StructField("asset_domain", T.StringType(), False),
    T.StructField("asset_key", T.StringType(), True),
    T.StructField("host_key", T.StringType(), True),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("intune_present", T.StringType(), False),
    T.StructField("intune_device_name", T.StringType(), True),
    T.StructField("intune_user_name", T.StringType(), True),
    T.StructField("intune_last_contact", T.TimestampType(), True),
    T.StructField("intune_os_version", T.StringType(), True),
    T.StructField("intune_serial_number", T.StringType(), True),
    # Backward-compatible raw coded value from properties_ComplianceState.
    T.StructField("intune_compliance_state", T.StringType(), True),
    # Complementary raw text state from properties_CompliantState.
    T.StructField("intune_compliant_state_alternate", T.StringType(), True),
    # Canonical state coalesced across the coded, alternate, and localized variants.
    T.StructField("intune_compliance_state_normalized", T.StringType(), False),
    T.StructField("intune_compliance_evidence_source", T.StringType(), False),
    T.StructField("intune_device_health_threat_level", T.StringType(), True),
    T.StructField("intune_device_health_state", T.StringType(), False),
    T.StructField("intune_encryption_state", T.StringType(), False),
    T.StructField("intune_operating_system", T.StringType(), True),
    T.StructField("last_observed_timestamp", T.TimestampType(), True),
    T.StructField("days_since_observed", T.IntegerType(), True),
    T.StructField("observation_age_bucket", T.StringType(), False),
    T.StructField("is_stale_90", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), True)
])

intune_source_available = bool(
    intune_source_table
    and
    spark.catalog.tableExists(
        intune_source_table
    )
)

if intune_source_available:
    intune_raw = clean_dataframe_columns(
        spark.table(
            intune_source_table
        )
    )

    intune_stage = (
        intune_raw

        .withColumn(
            "intune_device_name",
            text_expression(
                intune_raw,
                [
                    "properties_devicename",
                    "intune_device_name",
                    "device_name",
                    "devicename",
                    "managed_device_name"
                ]
            )
        )

        .withColumn(
            "intune_user_name",
            text_expression(
                intune_raw,
                [
                    "properties_username",
                    "intune_user_name",
                    "user_name",
                    "username",
                    "user_principal_name",
                    "userprincipalname"
                ]
            )
        )

        .withColumn(
            "intune_last_contact",
            timestamp_expression(
                intune_raw,
                [
                    "properties_lastcontact",
                    "intune_last_contact",
                    "last_contact",
                    "last_sync_date_time",
                    "last_check_in",
                    "last_seen"
                ]
            )
        )

        .withColumn(
            "intune_os_version",
            text_expression(
                intune_raw,
                [
                    "properties_osversion",
                    "intune_os_version",
                    "os_version"
                ]
            )
        )

        .withColumn(
            "intune_serial_number",
            text_expression(
                intune_raw,
                [
                    "properties_serialnumber",
                    "intune_serial_number",
                    "serial_number",
                    "serialnumber"
                ]
            )
        )

        .withColumn(
            "intune_compliance_state",
            text_expression(
                intune_raw,
                [
                    "properties_compliancestate",
                    "intune_compliance_state",
                    "compliance_state"
                ]
            )
        )

        .withColumn(
            "_intune_compliance_state_localized",
            text_expression(
                intune_raw,
                [
                    "properties_compliancestate_loc",
                    "intune_compliance_state_localized",
                    "compliance_state_localized"
                ]
            )
        )

        .withColumn(
            "intune_compliant_state_alternate",
            text_expression(
                intune_raw,
                [
                    "properties_compliantstate",
                    "intune_compliant_state_alternate",
                    "compliant_state"
                ]
            )
        )

        .withColumn(
            "_intune_compliance_alternate_key",
            F.lower(
                F.regexp_replace(
                    F.trim(
                        F.coalesce(
                            F.col("intune_compliant_state_alternate"),
                            F.lit("")
                        )
                    ),
                    r"[^a-zA-Z0-9]+",
                    ""
                )
            )
        )

        .withColumn(
            "_intune_compliance_localized_key",
            F.lower(
                F.regexp_replace(
                    F.trim(
                        F.coalesce(
                            F.col("_intune_compliance_state_localized"),
                            F.lit("")
                        )
                    ),
                    r"[^a-zA-Z0-9]+",
                    ""
                )
            )
        )

        .withColumn(
            "intune_compliance_state_normalized",
            F.when(
                F.col("intune_compliance_state") == F.lit("0"),
                F.lit("REPORTED_NOT_EVALUATED")
            )
            .when(
                F.col("intune_compliance_state") == F.lit("1"),
                F.lit("REPORTED_COMPLIANT")
            )
            .when(
                F.col("intune_compliance_state") == F.lit("2"),
                F.lit("REPORTED_NONCOMPLIANT")
            )
            .when(
                F.col("intune_compliance_state") == F.lit("254"),
                F.lit("REPORTED_IN_GRACE_PERIOD")
            )
            .when(
                F.col("intune_compliance_state") == F.lit("255"),
                F.lit("REPORTED_CONFIG_MANAGER")
            )
            .when(
                F.col("intune_compliance_state").isNotNull(),
                F.lit("UNMAPPED_COMPLIANCE_CODE_REQUIRES_REVIEW")
            )
            .when(
                F.col("_intune_compliance_alternate_key") == F.lit("compliant"),
                F.lit("REPORTED_COMPLIANT")
            )
            .when(
                F.col("_intune_compliance_alternate_key") == F.lit("noncompliant"),
                F.lit("REPORTED_NONCOMPLIANT")
            )
            .when(
                F.col("_intune_compliance_alternate_key") == F.lit("notevaluated"),
                F.lit("REPORTED_NOT_EVALUATED")
            )
            .when(
                F.col("_intune_compliance_alternate_key") == F.lit("ingraceperiod"),
                F.lit("REPORTED_IN_GRACE_PERIOD")
            )
            .when(
                F.col("_intune_compliance_alternate_key") == F.lit("configmanager"),
                F.lit("REPORTED_CONFIG_MANAGER")
            )
            .when(
                F.col("intune_compliant_state_alternate").isNotNull(),
                F.lit("UNMAPPED_COMPLIANCE_TEXT_REQUIRES_REVIEW")
            )
            .when(
                F.col("_intune_compliance_localized_key") == F.lit("compliant"),
                F.lit("REPORTED_COMPLIANT")
            )
            .when(
                F.col("_intune_compliance_localized_key") == F.lit("notcompliant"),
                F.lit("REPORTED_NONCOMPLIANT")
            )
            .when(
                F.col("_intune_compliance_localized_key") == F.lit("notevaluated"),
                F.lit("REPORTED_NOT_EVALUATED")
            )
            .when(
                F.col("_intune_compliance_localized_key") == F.lit("ingraceperiod"),
                F.lit("REPORTED_IN_GRACE_PERIOD")
            )
            .when(
                F.col("_intune_compliance_localized_key")
                == F.lit("managedbyconfigurationmanager"),
                F.lit("REPORTED_CONFIG_MANAGER")
            )
            .when(
                F.col("_intune_compliance_state_localized").isNotNull(),
                F.lit("UNMAPPED_COMPLIANCE_LABEL_REQUIRES_REVIEW")
            )
            .otherwise(
                F.lit("NO_COMPLIANCE_VALUE")
            )
        )

        .withColumn(
            "intune_compliance_evidence_source",
            F.when(
                F.col("intune_compliance_state").isNotNull(),
                F.lit("COMPLIANCE_STATE_CODE")
            )
            .when(
                F.col("intune_compliant_state_alternate").isNotNull(),
                F.lit("COMPLIANT_STATE_TEXT")
            )
            .when(
                F.col("_intune_compliance_state_localized").isNotNull(),
                F.lit("COMPLIANCE_STATE_LOCALIZED")
            )
            .otherwise(
                F.lit("NO_SOURCE_VALUE")
            )
        )

        .withColumn(
            "intune_device_health_threat_level",
            text_expression(
                intune_raw,
                [
                    "properties_devicehealththreatlevel",
                    "intune_device_health_threat_level",
                    "device_health_threat_level"
                ]
            )
        )

        .withColumn(
            "_intune_device_health_state_raw",
            text_expression(
                intune_raw,
                [
                    "properties_devicehealththreatlevel_loc",
                    "intune_device_health_state",
                    "device_health_state"
                ]
            )
        )

        .withColumn(
            "_intune_device_health_state_key",
            F.lower(
                F.regexp_replace(
                    F.trim(
                        F.coalesce(
                            F.col("_intune_device_health_state_raw"),
                            F.lit("")
                        )
                    ),
                    r"[^a-zA-Z0-9]+",
                    ""
                )
            )
        )

        .withColumn(
            "intune_device_health_state",
            F.when(
                F.col("_intune_device_health_state_key") == F.lit("secured"),
                F.lit("SECURED")
            )
            .when(
                F.col("_intune_device_health_state_key") == F.lit("deactivated"),
                F.lit("DEACTIVATED")
            )
            .when(
                F.col("_intune_device_health_state_key") == F.lit("unknown"),
                F.lit("UNKNOWN")
            )
            .when(
                F.col("_intune_device_health_state_key") == F.lit("low"),
                F.lit("LOW")
            )
            .when(
                F.col("_intune_device_health_state_key") == F.lit("medium"),
                F.lit("MEDIUM")
            )
            .when(
                F.col("_intune_device_health_state_key") == F.lit("high"),
                F.lit("HIGH")
            )
            .when(
                F.col("_intune_device_health_state_raw").isNotNull(),
                F.lit("UNMAPPED_HEALTH_LABEL_REQUIRES_REVIEW")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("0"),
                F.lit("UNKNOWN")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("2"),
                F.lit("DEACTIVATED")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("3"),
                F.lit("SECURED")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("4"),
                F.lit("LOW")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("5"),
                F.lit("MEDIUM")
            )
            .when(
                F.col("intune_device_health_threat_level") == F.lit("6"),
                F.lit("HIGH")
            )
            .when(
                F.col("intune_device_health_threat_level").isNotNull(),
                F.lit("UNMAPPED_HEALTH_CODE_REQUIRES_REVIEW")
            )
            .otherwise(
                F.lit("NO_HEALTH_VALUE")
            )
        )

        .withColumn(
            "_intune_encryption_state_raw",
            text_expression(
                intune_raw,
                [
                    "properties_encryptionstatusstring",
                    "intune_encryption_state",
                    "encryption_status"
                ]
            )
        )

        .withColumn(
            "intune_encryption_state",
            F.when(
                F.lower(F.trim(F.col("_intune_encryption_state_raw")))
                == F.lit("true"),
                F.lit("REPORTED_ENCRYPTED")
            )
            .when(
                F.lower(F.trim(F.col("_intune_encryption_state_raw")))
                == F.lit("false"),
                F.lit("REPORTED_NOT_ENCRYPTED")
            )
            .when(
                F.col("_intune_encryption_state_raw").isNotNull(),
                F.lit("UNMAPPED_ENCRYPTION_VALUE_REQUIRES_REVIEW")
            )
            .otherwise(
                F.lit("NO_ENCRYPTION_VALUE")
            )
        )

        .withColumn(
            "intune_operating_system",
            text_expression(
                intune_raw,
                [
                    "properties_deviceoperatingsystem",
                    "intune_operating_system",
                    "operating_system",
                    "os"
                ]
            )
        )

        .withColumn(
            "host_key",
            normalized_host_key(
                F.col(
                    "intune_device_name"
                )
            )
        )

        .withColumn(
            "asset_key",
            F.coalesce(
                F.col(
                    "host_key"
                ),

                normalized_serial(
                    F.col(
                        "intune_serial_number"
                    )
                )
            )
        )

        .withColumn(
            "_intune_scope_blob",
            F.lower(
                F.concat_ws(
                    " ",
                    "intune_device_name",
                    "intune_user_name"
                )
            )
        )

        .withColumn(
            "_intune_prefix_match",
            (
                F.upper(
                    F.coalesce(
                        F.col(
                            "intune_device_name"
                        ),
                        F.lit("")
                    )
                ).startswith(
                    post_device_prefix_upper
                )

                if post_device_prefix_upper

                else F.lit(False)
            )
        )

        .filter(
            F.col(
                "_intune_prefix_match"
            )
            |
            F.col(
                "_intune_scope_blob"
            ).rlike(
                post_token_regex
            )
        )

        .withColumn(
            "post_name",
            F.lit(
                RCAIP_POST_NAME
            )
        )

        .withColumn(
            "post_slug",
            F.lit(
                RCAIP_POST_SLUG
            )
        )

        .withColumn(
            "rcaip_run_id",
            F.lit(
                RCAIP_RUN_ID
            )
        )

        .withColumn(
            "asset_domain",
            F.lit(
                "Enterprise"
            )
        )

        .withColumn(
            "source_system",
            F.lit(
                "Intune"
            )
        )

        .withColumn(
            "intune_present",
            F.lit(
                "Y"
            )
        )

        .withColumn(
            "raw_source_table",
            F.lit(
                intune_source_table
            )
        )
    )

    intune_norm = add_lifecycle_columns(
        intune_stage,
        "intune_last_contact"
    )

    intune_norm = deduplicate_by_asset(
        intune_norm
    ).select(
        *[
            schema_field.name
            for schema_field in intune_schema.fields
        ]
    )

else:
    intune_norm = empty_dataframe(
        intune_schema
    )

intune_norm = save_asset_engine_table(
    intune_norm,
    "INTUNE_NORM"
)


# ---------------------------------------------------------------------------
# 9. Normalize Microsoft Defender for Endpoint
# ---------------------------------------------------------------------------

mde_schema = T.StructType([
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("rcaip_run_id", T.StringType(), False),
    T.StructField("asset_domain", T.StringType(), False),
    T.StructField("asset_key", T.StringType(), True),
    T.StructField("host_key", T.StringType(), True),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("mde_present", T.StringType(), False),
    T.StructField("mde_device_name", T.StringType(), True),
    T.StructField("mde_device_category", T.StringType(), True),
    T.StructField("mde_device_type", T.StringType(), True),
    T.StructField("mde_dynamic_tags", T.StringType(), True),
    T.StructField("mde_manual_tags", T.StringType(), True),
    T.StructField("mde_machine_group", T.StringType(), True),
    T.StructField("mde_public_ip", T.StringType(), True),
    T.StructField("mde_last_seen", T.TimestampType(), True),
    T.StructField("mde_onboarding_status", T.StringType(), True),
    T.StructField("mde_sensor_health_state", T.StringType(), True),
    # Exposure is prioritization context, not proof of configuration correctness.
    T.StructField("mde_exposure_level", T.StringType(), True),
    T.StructField("last_observed_timestamp", T.TimestampType(), True),
    T.StructField("days_since_observed", T.IntegerType(), True),
    T.StructField("observation_age_bucket", T.StringType(), False),
    T.StructField("is_stale_90", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), True)
])

mde_source_available = bool(
    mde_source_table
    and
    spark.catalog.tableExists(
        mde_source_table
    )
)

if mde_source_available:
    mde_raw = clean_dataframe_columns(
        spark.table(
            mde_source_table
        )
    )

    mde_stage = (
        mde_raw

        .withColumn(
            "mde_device_name",
            text_expression(
                mde_raw,
                [
                    "devicename",
                    "device_name",
                    "mde_device_name",
                    "computer_dns_name",
                    "hostname"
                ]
            )
        )

        .withColumn(
            "mde_device_category",
            text_expression(
                mde_raw,
                [
                    "devicecategory",
                    "device_category",
                    "mde_device_category"
                ]
            )
        )

        .withColumn(
            "mde_device_type",
            text_expression(
                mde_raw,
                [
                    "devicetype",
                    "device_type",
                    "mde_device_type"
                ]
            )
        )

        .withColumn(
            "mde_dynamic_tags",
            text_expression(
                mde_raw,
                [
                    "devicedynamictags",
                    "device_dynamic_tags",
                    "mde_dynamic_tags"
                ]
            )
        )

        .withColumn(
            "mde_manual_tags",
            text_expression(
                mde_raw,
                [
                    "devicemanualtags",
                    "device_manual_tags",
                    "mde_manual_tags"
                ]
            )
        )

        .withColumn(
            "mde_machine_group",
            text_expression(
                mde_raw,
                [
                    "machinegroup",
                    "machine_group",
                    "mde_machine_group"
                ]
            )
        )

        .withColumn(
            "mde_public_ip",
            text_expression(
                mde_raw,
                [
                    "publicip",
                    "public_ip",
                    "mde_public_ip",
                    "ip_address"
                ]
            )
        )

        .withColumn(
            "mde_last_seen",
            timestamp_expression(
                mde_raw,
                [
                    "timestamp",
                    "mde_last_seen",
                    "last_seen",
                    "lastseen"
                ]
            )
        )

        .withColumn(
            "mde_onboarding_status",
            text_expression(
                mde_raw,
                [
                    "onboardingstatus",
                    "mde_onboarding_status",
                    "onboarding_status"
                ]
            )
        )

        .withColumn(
            "mde_sensor_health_state",
            text_expression(
                mde_raw,
                [
                    "sensorhealthstate",
                    "mde_sensor_health_state",
                    "sensor_health_state"
                ]
            )
        )

        .withColumn(
            "mde_exposure_level",
            text_expression(
                mde_raw,
                [
                    "exposurelevel",
                    "mde_exposure_level",
                    "exposure_level"
                ]
            )
        )

        .withColumn(
            "host_key",
            normalized_host_key(
                F.col(
                    "mde_device_name"
                )
            )
        )

        .withColumn(
            "asset_key",
            F.col(
                "host_key"
            )
        )

        .withColumn(
            "_mde_scope_blob",
            F.lower(
                F.concat_ws(
                    " ",
                    "mde_device_name",
                    "mde_dynamic_tags",
                    "mde_manual_tags",
                    "mde_machine_group"
                )
            )
        )

        .withColumn(
            "_mde_prefix_match",
            (
                F.upper(
                    F.coalesce(
                        F.col(
                            "mde_device_name"
                        ),
                        F.lit("")
                    )
                ).startswith(
                    post_device_prefix_upper
                )

                if post_device_prefix_upper

                else F.lit(False)
            )
        )

        .filter(
            F.col(
                "_mde_prefix_match"
            )
            |
            F.col(
                "_mde_scope_blob"
            ).rlike(
                post_token_regex
            )
        )

        .withColumn(
            "post_name",
            F.lit(
                RCAIP_POST_NAME
            )
        )

        .withColumn(
            "post_slug",
            F.lit(
                RCAIP_POST_SLUG
            )
        )

        .withColumn(
            "rcaip_run_id",
            F.lit(
                RCAIP_RUN_ID
            )
        )

        .withColumn(
            "asset_domain",
            F.lit(
                "Enterprise"
            )
        )

        .withColumn(
            "source_system",
            F.lit(
                "MDE"
            )
        )

        .withColumn(
            "mde_present",
            F.lit(
                "Y"
            )
        )

        .withColumn(
            "raw_source_table",
            F.lit(
                mde_source_table
            )
        )
    )

    mde_norm = add_lifecycle_columns(
        mde_stage,
        "mde_last_seen"
    )

    mde_norm = deduplicate_by_asset(
        mde_norm
    ).select(
        *[
            schema_field.name
            for schema_field in mde_schema.fields
        ]
    )

else:
    mde_norm = empty_dataframe(
        mde_schema
    )

mde_norm = save_asset_engine_table(
    mde_norm,
    "MDE_NORM"
)


# ---------------------------------------------------------------------------
# 10. Normalize the Post-specific Qualys export
# ---------------------------------------------------------------------------

qualys_schema = T.StructType([
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("rcaip_run_id", T.StringType(), False),
    T.StructField("asset_domain", T.StringType(), False),
    T.StructField("asset_key", T.StringType(), True),
    T.StructField("host_key", T.StringType(), True),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("qualys_present", T.StringType(), False),
    T.StructField("qualys_scope", T.StringType(), True),
    T.StructField("ip_address", T.StringType(), True),
    T.StructField("qualys_asset_name", T.StringType(), True),
    T.StructField("qualys_bios_serial_number", T.StringType(), True),
    T.StructField("qualys_last_logged_on_user", T.StringType(), True),
    T.StructField("qualys_inventory_source", T.StringType(), True),
    T.StructField("qualys_agent_id", T.StringType(), True),
    T.StructField("qualys_inventory_created_on", T.TimestampType(), True),
    T.StructField("qualys_inventory_last_updated_on", T.TimestampType(), True),
    T.StructField("qualys_criticality_score", T.StringType(), True),
    T.StructField("qualys_open_ports", T.StringType(), True),
    T.StructField("qualys_tags", T.StringType(), True),
    T.StructField("qualys_trurisk_score", T.StringType(), True),
    T.StructField("last_observed_timestamp", T.TimestampType(), True),
    T.StructField("days_since_observed", T.IntegerType(), True),
    T.StructField("observation_age_bucket", T.StringType(), False),
    T.StructField("is_stale_90", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), True)
])

qualys_source_available = bool(
    RCAIP_QUALYS_EASM_PATH
)

if qualys_source_available:
    qualys_raw = clean_dataframe_columns(
        (
            spark.read
            .option(
                "header",
                True
            )
            .option(
                "inferSchema",
                True
            )
            .csv(
                RCAIP_QUALYS_EASM_PATH
            )
        )
    )

    qualys_stage = (
        qualys_raw

        .withColumn(
            "ip_address",
            text_expression(
                qualys_raw,
                [
                    "ipv4_address",
                    "ip_address",
                    "ipv4",
                    "ip"
                ]
            )
        )

        .withColumn(
            "qualys_asset_name",
            text_expression(
                qualys_raw,
                [
                    "asset_name",
                    "qualys_asset_name",
                    "device_name",
                    "hostname"
                ]
            )
        )

        .withColumn(
            "qualys_bios_serial_number",
            text_expression(
                qualys_raw,
                [
                    "bios_serial_number",
                    "qualys_bios_serial_number",
                    "serial_number",
                    "serialnumber"
                ]
            )
        )

        .withColumn(
            "qualys_last_logged_on_user",
            text_expression(
                qualys_raw,
                [
                    "last_logged_on_user",
                    "qualys_last_logged_on_user"
                ]
            )
        )

        .withColumn(
            "qualys_inventory_source",
            text_expression(
                qualys_raw,
                [
                    "inventory_source",
                    "qualys_inventory_source"
                ]
            )
        )

        .withColumn(
            "qualys_agent_id",
            text_expression(
                qualys_raw,
                [
                    "agent_id",
                    "qualys_agent_id"
                ]
            )
        )

        .withColumn(
            "qualys_inventory_created_on",
            timestamp_expression(
                qualys_raw,
                [
                    "inventory_created_on",
                    "qualys_inventory_created_on",
                    "created_on"
                ]
            )
        )

        .withColumn(
            "qualys_inventory_last_updated_on",
            timestamp_expression(
                qualys_raw,
                [
                    "inventory_last_updated_on",
                    "qualys_inventory_last_updated_on",
                    "last_updated_on",
                    "updated_on"
                ]
            )
        )

        .withColumn(
            "qualys_criticality_score",
            text_expression(
                qualys_raw,
                [
                    "criticalityscore",
                    "criticality_score",
                    "qualys_criticality_score"
                ]
            )
        )

        .withColumn(
            "qualys_open_ports",
            text_expression(
                qualys_raw,
                [
                    "open_ports",
                    "qualys_open_ports"
                ]
            )
        )

        .withColumn(
            "qualys_tags",
            text_expression(
                qualys_raw,
                [
                    "tags",
                    "qualys_tags"
                ]
            )
        )

        .withColumn(
            "qualys_trurisk_score",
            text_expression(
                qualys_raw,
                [
                    "trurisk_score",
                    "qualys_trurisk_score"
                ]
            )
        )

        .withColumn(
            "host_key",
            normalized_host_key(
                F.col(
                    "qualys_asset_name"
                )
            )
        )

        .withColumn(
            "asset_key",
            F.coalesce(
                F.col(
                    "host_key"
                ),

                normalized_serial(
                    F.col(
                        "qualys_bios_serial_number"
                    )
                ),

                F.col(
                    "ip_address"
                )
            )
        )

        .withColumn(
            "post_name",
            F.lit(
                RCAIP_POST_NAME
            )
        )

        .withColumn(
            "post_slug",
            F.lit(
                RCAIP_POST_SLUG
            )
        )

        .withColumn(
            "rcaip_run_id",
            F.lit(
                RCAIP_RUN_ID
            )
        )

        .withColumn(
            "asset_domain",
            F.lit(
                "Enterprise"
            )
        )

        .withColumn(
            "source_system",
            F.lit(
                "Qualys"
            )
        )

        .withColumn(
            "qualys_present",
            F.lit(
                "Y"
            )
        )

        .withColumn(
            "qualys_scope",
            F.when(
                ip_in_enterprise_scope(
                    F.col(
                        "ip_address"
                    )
                ),
                F.lit(
                    "OpenNet CIDR"
                )
            )
            .otherwise(
                F.lit(
                    "Post-Provided Qualys Export"
                )
            )
        )

        .withColumn(
            "raw_source_table",
            F.lit(
                RCAIP_QUALYS_EASM_PATH
            )
        )
    )

    qualys_norm = add_lifecycle_columns(
        qualys_stage,
        "qualys_inventory_last_updated_on"
    )

    qualys_norm = deduplicate_by_asset(
        qualys_norm
    ).select(
        *[
            schema_field.name
            for schema_field in qualys_schema.fields
        ]
    )

else:
    qualys_norm = empty_dataframe(
        qualys_schema
    )

qualys_norm = save_asset_engine_table(
    qualys_norm,
    "QUALYS_NORM"
)


# ---------------------------------------------------------------------------
# 11. Build the unified observed-assets record set
# ---------------------------------------------------------------------------

observation_columns = [
    "post_name",
    "post_slug",
    "rcaip_run_id",
    "asset_domain",
    "asset_key",
    "host_key",
    "observed_source",
    "observed_ip",
    "observed_name",
    "observed_serial",
    "last_observed_timestamp",
    "days_since_observed",
    "observation_age_bucket",
    "source_record_present",
    "source_active",
    "source_stale",
    "source_state_reason"
]


def source_activity_expression(
    is_stale_column_name,
    unknown_is_active=False
):
    return (
        F.when(
            F.col(
                is_stale_column_name
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "N"
            )
        )

        .when(
            F.col(
                is_stale_column_name
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Y"
            )
        )

        .otherwise(
            F.lit(
                "Y"
                if unknown_is_active
                else
                "N"
            )
        )
    )


nmap_observed = (
    nmap_asset_source

    .select(
        F.lit(
            RCAIP_POST_NAME
        ).alias(
            "post_name"
        ),

        F.lit(
            RCAIP_POST_SLUG
        ).alias(
            "post_slug"
        ),

        F.lit(
            RCAIP_RUN_ID
        ).alias(
            "rcaip_run_id"
        ),

        "asset_domain",
        "asset_key",
        "host_key",

        F.lit(
            "NMAP"
        ).alias(
            "observed_source"
        ),

        F.col(
            "ip_address"
        ).alias(
            "observed_ip"
        ),

        F.col(
            "nmap_hostname"
        ).alias(
            "observed_name"
        ),

        F.col(
            "mac_address"
        ).alias(
            "observed_serial"
        ),

        "last_observed_timestamp",
        "days_since_observed",
        "observation_age_bucket",

        F.lit(
            "Y"
        ).alias(
            "source_record_present"
        ),

        source_activity_expression(
            "is_stale_90",
            unknown_is_active=True
        ).alias(
            "source_active"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Y"
            )
        )
        .otherwise(
            F.lit(
                "N"
            )
        ).alias(
            "source_stale"
        ),

        F.lit(
            "Certified current-assessment Nmap observation"
        ).alias(
            "source_state_reason"
        )
    )
)


tenable_observed = (
    tenable_norm

    .select(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "asset_domain",
        "asset_key",
        "host_key",

        F.lit(
            "Tenable"
        ).alias(
            "observed_source"
        ),

        F.col(
            "ip_address"
        ).alias(
            "observed_ip"
        ),

        F.col(
            "dns_name"
        ).alias(
            "observed_name"
        ),

        F.lit(
            None
        ).cast(
            "string"
        ).alias(
            "observed_serial"
        ),

        "last_observed_timestamp",
        "days_since_observed",
        "observation_age_bucket",

        F.lit(
            "Y"
        ).alias(
            "source_record_present"
        ),

        source_activity_expression(
            "is_stale_90"
        ).alias(
            "source_active"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Y"
            )
        )
        .otherwise(
            F.lit(
                "N"
            )
        ).alias(
            "source_stale"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Tenable record older than 90 days"
            )
        )
        .when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Tenable record observed within 90 days"
            )
        )
        .otherwise(
            F.lit(
                "Tenable observation age unknown"
            )
        ).alias(
            "source_state_reason"
        )
    )
)


intune_observed = (
    intune_norm

    .select(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "asset_domain",
        "asset_key",
        "host_key",

        F.lit(
            "Intune"
        ).alias(
            "observed_source"
        ),

        F.lit(
            None
        ).cast(
            "string"
        ).alias(
            "observed_ip"
        ),

        F.col(
            "intune_device_name"
        ).alias(
            "observed_name"
        ),

        F.col(
            "intune_serial_number"
        ).alias(
            "observed_serial"
        ),

        "last_observed_timestamp",
        "days_since_observed",
        "observation_age_bucket",

        F.lit(
            "Y"
        ).alias(
            "source_record_present"
        ),

        source_activity_expression(
            "is_stale_90"
        ).alias(
            "source_active"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Y"
            )
        )
        .otherwise(
            F.lit(
                "N"
            )
        ).alias(
            "source_stale"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Intune contact older than 90 days"
            )
        )
        .when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Intune contact within 90 days"
            )
        )
        .otherwise(
            F.lit(
                "Intune contact age unknown"
            )
        ).alias(
            "source_state_reason"
        )
    )
)


mde_observed = (
    mde_norm

    .select(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "asset_domain",
        "asset_key",
        "host_key",

        F.lit(
            "MDE"
        ).alias(
            "observed_source"
        ),

        F.col(
            "mde_public_ip"
        ).alias(
            "observed_ip"
        ),

        F.col(
            "mde_device_name"
        ).alias(
            "observed_name"
        ),

        F.lit(
            None
        ).cast(
            "string"
        ).alias(
            "observed_serial"
        ),

        "last_observed_timestamp",
        "days_since_observed",
        "observation_age_bucket",

        F.lit(
            "Y"
        ).alias(
            "source_record_present"
        ),

        source_activity_expression(
            "is_stale_90"
        ).alias(
            "source_active"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Y"
            )
        )
        .otherwise(
            F.lit(
                "N"
            )
        ).alias(
            "source_stale"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "MDE record older than 90 days"
            )
        )
        .when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "MDE record observed within 90 days"
            )
        )
        .otherwise(
            F.lit(
                "MDE observation age unknown"
            )
        ).alias(
            "source_state_reason"
        )
    )
)


qualys_observed = (
    qualys_norm

    .select(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "asset_domain",
        "asset_key",
        "host_key",

        F.lit(
            "Qualys"
        ).alias(
            "observed_source"
        ),

        F.col(
            "ip_address"
        ).alias(
            "observed_ip"
        ),

        F.col(
            "qualys_asset_name"
        ).alias(
            "observed_name"
        ),

        F.col(
            "qualys_bios_serial_number"
        ).alias(
            "observed_serial"
        ),

        "last_observed_timestamp",
        "days_since_observed",
        "observation_age_bucket",

        F.lit(
            "Y"
        ).alias(
            "source_record_present"
        ),

        source_activity_expression(
            "is_stale_90"
        ).alias(
            "source_active"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Y"
            )
        )
        .otherwise(
            F.lit(
                "N"
            )
        ).alias(
            "source_stale"
        ),

        F.when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "Qualys record older than 90 days"
            )
        )
        .when(
            F.col(
                "is_stale_90"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Qualys record observed within 90 days"
            )
        )
        .otherwise(
            F.lit(
                "Qualys observation age unknown"
            )
        ).alias(
            "source_state_reason"
        )
    )
)


observed_assets = reduce(
    lambda left_dataframe, right_dataframe:
        left_dataframe.unionByName(
            right_dataframe,
            allowMissingColumns=True
        ),

    [
        nmap_observed,
        tenable_observed,
        intune_observed,
        mde_observed,
        qualys_observed
    ]
).filter(
    F.col(
        "asset_key"
    ).isNotNull()
)

observed_assets = save_asset_engine_table(
    observed_assets,
    "OBSERVED_ASSETS"
)


# ---------------------------------------------------------------------------
# 12. Lifecycle-aware observed-asset rollup
# ---------------------------------------------------------------------------

observed_assets_rollup = (
    observed_assets

    .groupBy(
        "asset_key"
    )

    .agg(
        F.first(
            "host_key",
            ignorenulls=True
        ).alias(
            "host_key"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "asset_domain"
                )
            )
        ).alias(
            "observed_asset_domains"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "observed_source"
                )
            )
        ).alias(
            "observed_sources_raw"
        ),

        F.countDistinct(
            "observed_source"
        ).alias(
            "observed_source_count_raw"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    F.when(
                        F.col(
                            "source_active"
                        )
                        ==
                        F.lit(
                            "Y"
                        ),
                        F.col(
                            "observed_source"
                        )
                    )
                )
            )
        ).alias(
            "observed_sources_active"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "source_active"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.col(
                    "observed_source"
                )
            )
        ).alias(
            "observed_source_count_active"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    F.when(
                        F.col(
                            "source_stale"
                        )
                        ==
                        F.lit(
                            "Y"
                        ),
                        F.col(
                            "observed_source"
                        )
                    )
                )
            )
        ).alias(
            "observed_sources_stale"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "source_stale"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.col(
                    "observed_source"
                )
            )
        ).alias(
            "observed_source_count_stale"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "observed_ip"
                )
            )
        ).alias(
            "observed_ips"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "observed_name"
                )
            )
        ).alias(
            "observed_names"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "observed_serial"
                )
            )
        ).alias(
            "observed_serials"
        ),

        F.max(
            "last_observed_timestamp"
        ).alias(
            "most_recent_record_timestamp"
        ),

        F.max(
            F.when(
                F.col(
                    "source_active"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.col(
                    "last_observed_timestamp"
                )
            )
        ).alias(
            "most_recent_active_timestamp"
        ),

        *[
            F.max(
                F.when(
                    F.upper(
                        F.col(
                            "observed_source"
                        )
                    )
                    ==
                    F.lit(
                        source_name.upper()
                    ),
                    F.lit(
                        1
                    )
                )
                .otherwise(
                    F.lit(
                        0
                    )
                )
            ).alias(
                f"{source_name.lower()}_record_present"
            )

            for source_name
            in [
                "NMAP",
                "Tenable",
                "Intune",
                "MDE",
                "Qualys"
            ]
        ],

        *[
            F.max(
                F.when(
                    (
                        F.upper(
                            F.col(
                                "observed_source"
                            )
                        )
                        ==
                        F.lit(
                            source_name.upper()
                        )
                    )
                    &
                    (
                        F.col(
                            "source_active"
                        )
                        ==
                        F.lit(
                            "Y"
                        )
                    ),
                    F.lit(
                        1
                    )
                )
                .otherwise(
                    F.lit(
                        0
                    )
                )
            ).alias(
                f"{source_name.lower()}_active"
            )

            for source_name
            in [
                "NMAP",
                "Tenable",
                "Intune",
                "MDE",
                "Qualys"
            ]
        ],

        *[
            F.max(
                F.when(
                    (
                        F.upper(
                            F.col(
                                "observed_source"
                            )
                        )
                        ==
                        F.lit(
                            source_name.upper()
                        )
                    )
                    &
                    (
                        F.col(
                            "source_stale"
                        )
                        ==
                        F.lit(
                            "Y"
                        )
                    ),
                    F.lit(
                        1
                    )
                )
                .otherwise(
                    F.lit(
                        0
                    )
                )
            ).alias(
                f"{source_name.lower()}_stale"
            )

            for source_name
            in [
                "NMAP",
                "Tenable",
                "Intune",
                "MDE",
                "Qualys"
            ]
        ]
    )

    .withColumn(
        "observed_sources",
        F.col(
            "observed_sources_active"
        )
    )

    .withColumn(
        "observed_source_count",
        F.col(
            "observed_source_count_active"
        )
    )

    .withColumn(
        "current_observation_status",

        F.when(
            F.col(
                "observed_source_count_active"
            )
            >=
            F.lit(
                2
            ),
            F.lit(
                "Active Multi-Source"
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                1
            ),
            F.lit(
                "Active Single-Source"
            )
        )

        .when(
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                F.lit(
                    0
                )
            )
            &
            (
                F.col(
                    "observed_source_count_stale"
                )
                >
                F.lit(
                    0
                )
            ),
            F.lit(
                "Stale Historical Records Only"
            )
        )

        .otherwise(
            F.lit(
                "No Current Evidence"
            )
        )
    )

    .withColumn(
        "post_name",
        F.lit(
            RCAIP_POST_NAME
        )
    )

    .withColumn(
        "post_slug",
        F.lit(
            RCAIP_POST_SLUG
        )
    )

    .withColumn(
        "rcaip_run_id",
        F.lit(
            RCAIP_RUN_ID
        )
    )
)

observed_assets_rollup = save_asset_engine_table(
    observed_assets_rollup,
    "OBSERVED_ASSETS_ROLLUP"
)


# ---------------------------------------------------------------------------
# 13. Lifecycle products
# ---------------------------------------------------------------------------

source_lifecycle_counts = (
    observed_assets

    .groupBy(
        "observed_source"
    )

    .agg(
        F.countDistinct(
            "asset_key"
        ).alias(
            "raw_asset_count"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "source_active"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.col(
                    "asset_key"
                )
            )
        ).alias(
            "active_asset_count"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "source_stale"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.col(
                    "asset_key"
                )
            )
        ).alias(
            "stale_asset_count"
        ),

        F.countDistinct(
            F.when(
                (
                    F.col(
                        "source_active"
                    )
                    ==
                    F.lit(
                        "N"
                    )
                )
                &
                (
                    F.col(
                        "source_stale"
                    )
                    ==
                    F.lit(
                        "N"
                    )
                ),
                F.col(
                    "asset_key"
                )
            )
        ).alias(
            "unknown_age_asset_count"
        ),

        F.max(
            "last_observed_timestamp"
        ).alias(
            "most_recent_source_timestamp"
        )
    )
)

source_lifecycle_counts = save_asset_engine_table(
    source_lifecycle_counts,
    "SOURCE_LIFECYCLE_COUNTS"
)


stale_observed_assets = (
    observed_assets_rollup

    .filter(
        F.col(
            "observed_source_count_active"
        )
        ==
        F.lit(
            0
        )
    )

    .filter(
        F.col(
            "observed_source_count_stale"
        )
        >
        F.lit(
            0
        )
    )

    .withColumn(
        "stale_record_category",

        F.when(
            (
                F.col(
                    "mde_stale"
                )
                ==
                F.lit(
                    1
                )
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                F.lit(
                    1
                )
            ),
            F.lit(
                "Stale MDE Only"
            )
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >=
            F.lit(
                2
            ),
            F.lit(
                "Multiple Stale Sources"
            )
        )

        .otherwise(
            F.lit(
                "Single Stale Source"
            )
        )
    )
)

stale_observed_assets = save_asset_engine_table(
    stale_observed_assets,
    "STALE_OBSERVED_ASSETS"
)


# ---------------------------------------------------------------------------
# 14. Prepare one-row-per-asset source views
# ---------------------------------------------------------------------------

nmap_join = (
    nmap_asset_source

    .select(
        "asset_key",
        "host_key",

        F.col(
            "nmap_present"
        ).alias(
            "nmap_present"
        ),

        F.col(
            "asset_domain"
        ).alias(
            "nmap_asset_domain"
        ),

        F.col(
            "scan_scope"
        ).alias(
            "nmap_scan_scope"
        ),

        F.col(
            "scan_location"
        ).alias(
            "nmap_scan_location"
        ),

        F.col(
            "ip_address"
        ).alias(
            "nmap_ip"
        ),

        F.col(
            "nmap_hostname"
        ).alias(
            "nmap_hostname"
        ),

        "nmap_services",
        "nmap_ports",

        F.col(
            "open_port_count"
        ).alias(
            "nmap_open_port_count"
        ),

        F.col(
            "last_observed_timestamp"
        ).alias(
            "nmap_last_observed_timestamp"
        ),

        F.col(
            "is_stale_90"
        ).alias(
            "nmap_is_stale_90"
        )
    )
)


tenable_join = (
    tenable_norm

    .select(
        "asset_key",
        "host_key",
        "tenable_present",

        F.col(
            "ip_address"
        ).alias(
            "tenable_ip"
        ),

        F.col(
            "dns_name"
        ).alias(
            "tenable_dns_name"
        ),

        "tenable_event_time",
        "tenable_host_uuid",
        "tenable_acr_score",
        "tenable_key_drivers",
        "tenable_organization_name",

        F.col(
            "is_stale_90"
        ).alias(
            "tenable_is_stale_90"
        )
    )
)


intune_join = (
    intune_norm

    .select(
        "asset_key",
        "host_key",
        "intune_present",
        "intune_device_name",
        "intune_user_name",
        "intune_last_contact",
        "intune_os_version",
        "intune_serial_number",
        "intune_compliance_state",
        "intune_compliant_state_alternate",
        "intune_compliance_state_normalized",
        "intune_compliance_evidence_source",
        "intune_device_health_threat_level",
        "intune_device_health_state",
        "intune_encryption_state",
        "intune_operating_system",

        F.col(
            "is_stale_90"
        ).alias(
            "intune_is_stale_90"
        )
    )
)


mde_join = (
    mde_norm

    .select(
        "asset_key",
        "host_key",
        "mde_present",
        "mde_device_name",
        "mde_device_category",
        "mde_device_type",
        "mde_dynamic_tags",
        "mde_manual_tags",
        "mde_machine_group",
        "mde_public_ip",
        "mde_last_seen",
        "mde_onboarding_status",
        "mde_sensor_health_state",
        "mde_exposure_level",

        F.col(
            "is_stale_90"
        ).alias(
            "mde_is_stale_90"
        )
    )
)


qualys_join = (
    qualys_norm

    .select(
        "asset_key",
        "host_key",
        "qualys_present",
        "qualys_scope",

        F.col(
            "ip_address"
        ).alias(
            "qualys_ip"
        ),

        "qualys_asset_name",
        "qualys_bios_serial_number",
        "qualys_last_logged_on_user",
        "qualys_inventory_source",
        "qualys_agent_id",
        "qualys_inventory_created_on",
        "qualys_inventory_last_updated_on",
        "qualys_criticality_score",
        "qualys_open_ports",
        "qualys_tags",
        "qualys_trurisk_score",

        F.col(
            "is_stale_90"
        ).alias(
            "qualys_is_stale_90"
        )
    )
)


# ---------------------------------------------------------------------------
# 15. Validate one-row-per-key join inputs and build the identity universe
# ---------------------------------------------------------------------------

join_input_dataframes = [
    ("CMDB", cmdb_norm),
    ("NMAP", nmap_asset_source),
    ("Tenable", tenable_norm),
    ("Intune", intune_norm),
    ("MDE", mde_norm),
    ("Qualys", qualys_norm),
    ("Observed Rollup", observed_assets_rollup)
]

join_input_profile_frames = []

for (
    join_input_name,
    join_input_dataframe
) in join_input_dataframes:
    join_input_profile_frames.append(
        join_input_dataframe
        .agg(
            F.count(
                F.lit(1)
            ).cast(
                "long"
            ).alias(
                "row_count"
            ),

            F.countDistinct(
                "asset_key"
            ).cast(
                "long"
            ).alias(
                "distinct_asset_key_count"
            ),

            F.sum(
                F.when(
                    F.col(
                        "asset_key"
                    ).isNull(),
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )
            ).cast(
                "long"
            ).alias(
                "null_asset_key_count"
            )
        )
        .withColumn(
            "join_input_name",
            F.lit(
                join_input_name
            )
        )
        .select(
            "join_input_name",
            "row_count",
            "distinct_asset_key_count",
            "null_asset_key_count"
        )
    )

asset_engine_join_input_profile_df = reduce(
    lambda left_dataframe, right_dataframe:
        left_dataframe.unionByName(
            right_dataframe
        ),
    join_input_profile_frames
)

join_input_profile_rows = (
    asset_engine_join_input_profile_df
    .collect()
)

join_input_failures = []

for join_input_profile_row in join_input_profile_rows:
    join_input_name = join_input_profile_row[
        "join_input_name"
    ]

    row_count = int(
        join_input_profile_row[
            "row_count"
        ]
        or
        0
    )

    distinct_asset_key_count = int(
        join_input_profile_row[
            "distinct_asset_key_count"
        ]
        or
        0
    )

    null_asset_key_count = int(
        join_input_profile_row[
            "null_asset_key_count"
        ]
        or
        0
    )

    if null_asset_key_count > 0:
        join_input_failures.append(
            f"{join_input_name}: {null_asset_key_count} null asset keys"
        )

    if row_count != distinct_asset_key_count:
        join_input_failures.append(
            f"{join_input_name}: {row_count} rows but "
            f"{distinct_asset_key_count} distinct asset keys"
        )

if join_input_failures:
    raise RuntimeError(
        "Asset Engine blocked a potential many-to-many reconciliation join: "
        +
        " | ".join(
            join_input_failures
        )
    )

asset_identity_universe = (
    cmdb_norm

    .select(
        "asset_key",
        "host_key"
    )

    .unionByName(
        observed_assets_rollup.select(
            "asset_key",
            "host_key"
        ),
        allowMissingColumns=True
    )

    .filter(
        F.col(
            "asset_key"
        ).isNotNull()
    )

    .groupBy(
        "asset_key"
    )

    .agg(
        F.first(
            "host_key",
            ignorenulls=True
        ).alias(
            "host_key"
        )
    )
)


# ---------------------------------------------------------------------------
# 16. Build cross-source Asset Plane reconciliation
# ---------------------------------------------------------------------------

asset_reconciliation = (
    asset_identity_universe.alias(
        "a"
    )

    .join(
        cmdb_norm.alias(
            "c"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        nmap_join.alias(
            "n"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        tenable_join.alias(
            "t"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        intune_join.alias(
            "i"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        mde_join.alias(
            "m"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        qualys_join.alias(
            "q"
        ),
        on="asset_key",
        how="left"
    )

    .join(
        observed_assets_rollup.alias(
            "o"
        ),
        on="asset_key",
        how="left"
    )

    .select(
        F.lit(
            RCAIP_POST_NAME
        ).alias(
            "post_name"
        ),

        F.lit(
            RCAIP_POST_SLUG
        ).alias(
            "post_slug"
        ),

        F.lit(
            RCAIP_RUN_ID
        ).alias(
            "rcaip_run_id"
        ),

        "asset_key",

        F.coalesce(
            F.col(
                "a.host_key"
            ),
            F.col(
                "c.host_key"
            ),
            F.col(
                "n.host_key"
            ),
            F.col(
                "t.host_key"
            ),
            F.col(
                "i.host_key"
            ),
            F.col(
                "m.host_key"
            ),
            F.col(
                "q.host_key"
            ),
            F.col(
                "o.host_key"
            )
        ).alias(
            "host_key"
        ),

        F.coalesce(
            F.col(
                "c.cmdb_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "cmdb_present"
        ),

        F.col(
            "c.cmdb_name"
        ),

        F.col(
            "c.cmdb_fqdn"
        ),

        F.col(
            "c.cmdb_ip"
        ),

        F.col(
            "c.cmdb_serial_number"
        ),

        F.col(
            "c.cmdb_class"
        ),

        F.col(
            "c.cmdb_location"
        ),

        F.col(
            "c.cmdb_category"
        ),

        F.col(
            "c.cmdb_mission"
        ),

        F.col(
            "c.cmdb_managed_by_group"
        ),

        F.col(
            "c.cmdb_support_group"
        ),

        F.col(
            "c.cmdb_install_status"
        ),

        F.col(
            "c.cmdb_most_recent_discovery"
        ),

        F.col(
            "c.cmdb_discovery_source"
        ),

        F.coalesce(
            F.col(
                "n.nmap_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "nmap_present"
        ),

        F.col(
            "n.nmap_asset_domain"
        ),

        F.col(
            "n.nmap_scan_scope"
        ),

        F.col(
            "n.nmap_scan_location"
        ),

        F.col(
            "n.nmap_ip"
        ),

        F.col(
            "n.nmap_hostname"
        ),

        F.col(
            "n.nmap_services"
        ),

        F.col(
            "n.nmap_ports"
        ),

        F.col(
            "n.nmap_open_port_count"
        ),

        F.col(
            "n.nmap_last_observed_timestamp"
        ),

        F.col(
            "n.nmap_is_stale_90"
        ),

        F.coalesce(
            F.col(
                "t.tenable_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "tenable_present"
        ),

        F.col(
            "t.tenable_ip"
        ),

        F.col(
            "t.tenable_dns_name"
        ),

        F.col(
            "t.tenable_event_time"
        ),

        F.col(
            "t.tenable_host_uuid"
        ),

        F.col(
            "t.tenable_acr_score"
        ),

        F.col(
            "t.tenable_key_drivers"
        ),

        F.col(
            "t.tenable_organization_name"
        ),

        F.col(
            "t.tenable_is_stale_90"
        ),

        F.coalesce(
            F.col(
                "i.intune_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "intune_present"
        ),

        F.col(
            "i.intune_device_name"
        ),

        F.col(
            "i.intune_user_name"
        ),

        F.col(
            "i.intune_last_contact"
        ),

        F.col(
            "i.intune_os_version"
        ),

        F.col(
            "i.intune_serial_number"
        ),

        F.col(
            "i.intune_compliance_state"
        ),

        F.col(
            "i.intune_compliant_state_alternate"
        ),

        F.col(
            "i.intune_compliance_state_normalized"
        ),

        F.col(
            "i.intune_compliance_evidence_source"
        ),

        F.col(
            "i.intune_device_health_threat_level"
        ),

        F.col(
            "i.intune_device_health_state"
        ),

        F.col(
            "i.intune_encryption_state"
        ),

        F.col(
            "i.intune_operating_system"
        ),

        F.col(
            "i.intune_is_stale_90"
        ),

        F.coalesce(
            F.col(
                "m.mde_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "mde_present"
        ),

        F.col(
            "m.mde_device_name"
        ),

        F.col(
            "m.mde_device_category"
        ),

        F.col(
            "m.mde_device_type"
        ),

        F.col(
            "m.mde_dynamic_tags"
        ),

        F.col(
            "m.mde_manual_tags"
        ),

        F.col(
            "m.mde_machine_group"
        ),

        F.col(
            "m.mde_public_ip"
        ),

        F.col(
            "m.mde_last_seen"
        ),

        F.col(
            "m.mde_onboarding_status"
        ),

        F.col(
            "m.mde_sensor_health_state"
        ),

        F.col(
            "m.mde_exposure_level"
        ),

        F.col(
            "m.mde_is_stale_90"
        ),

        F.coalesce(
            F.col(
                "q.qualys_present"
            ),
            F.lit(
                "N"
            )
        ).alias(
            "qualys_present"
        ),

        F.col(
            "q.qualys_scope"
        ),

        F.col(
            "q.qualys_ip"
        ),

        F.col(
            "q.qualys_asset_name"
        ),

        F.col(
            "q.qualys_bios_serial_number"
        ),

        F.col(
            "q.qualys_last_logged_on_user"
        ),

        F.col(
            "q.qualys_inventory_source"
        ),

        F.col(
            "q.qualys_agent_id"
        ),

        F.col(
            "q.qualys_inventory_created_on"
        ),

        F.col(
            "q.qualys_inventory_last_updated_on"
        ),

        F.col(
            "q.qualys_criticality_score"
        ),

        F.col(
            "q.qualys_open_ports"
        ),

        F.col(
            "q.qualys_tags"
        ),

        F.col(
            "q.qualys_trurisk_score"
        ),

        F.col(
            "q.qualys_is_stale_90"
        ),

        F.coalesce(
            F.col(
                "o.observed_sources_raw"
            ),
            F.lit("")
        ).alias(
            "observed_sources_raw"
        ),

        F.coalesce(
            F.col(
                "o.observed_sources_active"
            ),
            F.lit("")
        ).alias(
            "observed_sources_active"
        ),

        F.coalesce(
            F.col(
                "o.observed_sources_stale"
            ),
            F.lit("")
        ).alias(
            "observed_sources_stale"
        ),

        F.coalesce(
            F.col(
                "o.observed_source_count_raw"
            ),
            F.lit(
                0
            )
        ).alias(
            "observed_source_count_raw"
        ),

        F.coalesce(
            F.col(
                "o.observed_source_count_active"
            ),
            F.lit(
                0
            )
        ).alias(
            "observed_source_count_active"
        ),

        F.coalesce(
            F.col(
                "o.observed_source_count_stale"
            ),
            F.lit(
                0
            )
        ).alias(
            "observed_source_count_stale"
        ),

        F.col(
            "o.observed_ips"
        ),

        F.col(
            "o.observed_names"
        ),

        F.col(
            "o.observed_serials"
        ),

        F.col(
            "o.most_recent_record_timestamp"
        ),

        F.col(
            "o.most_recent_active_timestamp"
        ),

        F.coalesce(
            F.col(
                "o.current_observation_status"
            ),
            F.lit(
                "No Current Evidence"
            )
        ).alias(
            "current_observation_status"
        ),

        *[
            F.coalesce(
                F.col(
                    f"o.{source_name}_record_present"
                ),
                F.lit(
                    0
                )
            ).alias(
                f"{source_name}_record_present"
            )

            for source_name
            in [
                "nmap",
                "tenable",
                "intune",
                "mde",
                "qualys"
            ]
        ],

        *[
            F.coalesce(
                F.col(
                    f"o.{source_name}_active"
                ),
                F.lit(
                    0
                )
            ).alias(
                f"{source_name}_active"
            )

            for source_name
            in [
                "nmap",
                "tenable",
                "intune",
                "mde",
                "qualys"
            ]
        ],

        *[
            F.coalesce(
                F.col(
                    f"o.{source_name}_stale"
                ),
                F.lit(
                    0
                )
            ).alias(
                f"{source_name}_stale"
            )

            for source_name
            in [
                "nmap",
                "tenable",
                "intune",
                "mde",
                "qualys"
            ]
        ]
    )
)

asset_reconciliation = save_asset_engine_table(
    asset_reconciliation,
    "ASSET_RECONCILIATION"
)

rcaip_asset_reconciliation = asset_reconciliation


# ---------------------------------------------------------------------------
# 17. Score visibility, lifecycle, and validation priority
# ---------------------------------------------------------------------------

asset_reconciliation_scored = (
    asset_reconciliation

    .withColumn(
        "observed_source_count",
        (
            F.when(
                F.col(
                    "nmap_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
            +
            F.when(
                F.col(
                    "tenable_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
            +
            F.when(
                F.col(
                    "intune_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
            +
            F.when(
                F.col(
                    "mde_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
            +
            F.when(
                F.col(
                    "qualys_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        )
    )

    .withColumn(
        "visibility_assessment",

        F.when(
            F.col(
                "observed_source_count_active"
            )
            >=
            F.lit(
                4
            ),
            F.lit(
                "Strong Enterprise Visibility"
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                3
            ),
            F.lit(
                "Good Enterprise Visibility"
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                2
            ),
            F.lit(
                "Moderate Enterprise Visibility"
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                1
            ),
            F.lit(
                "Limited Enterprise Visibility"
            )
        )

        .when(
            F.col(
                "cmdb_present"
            )
            ==
            F.lit(
                "Y"
            ),
            F.lit(
                "CMDB Only - Validate"
            )
        )

        .otherwise(
            F.lit(
                "Observed Outside CMDB - Validate"
            )
        )
    )

    .withColumn(
        "coverage_gap_summary",
        F.concat_ws(
            ", ",

            F.when(
                F.col(
                    "nmap_present"
                )
                ==
                F.lit(
                    "N"
                ),
                F.lit(
                    "Missing NMAP"
                )
            ),

            F.when(
                F.col(
                    "tenable_present"
                )
                ==
                F.lit(
                    "N"
                ),
                F.lit(
                    "Missing Tenable"
                )
            ),

            F.when(
                F.col(
                    "intune_present"
                )
                ==
                F.lit(
                    "N"
                ),
                F.lit(
                    "Missing Intune"
                )
            ),

            F.when(
                F.col(
                    "mde_present"
                )
                ==
                F.lit(
                    "N"
                ),
                F.lit(
                    "Missing MDE"
                )
            ),

            F.when(
                F.col(
                    "qualys_present"
                )
                ==
                F.lit(
                    "N"
                ),
                F.lit(
                    "Missing Qualys"
                )
            )
        )
    )

    .withColumn(
        "lifecycle_reconciliation_status",

        F.when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "Y"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                >=
                F.lit(
                    2
                )
            ),
            F.lit(
                "CMDB + Active Multi-Source"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "Y"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                F.lit(
                    1
                )
            ),
            F.lit(
                "CMDB + Active Single-Source"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "Y"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                F.lit(
                    0
                )
            )
            &
            (
                F.col(
                    "observed_source_count_stale"
                )
                >
                F.lit(
                    0
                )
            ),
            F.lit(
                "CMDB + Stale Source Records Only"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "Y"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_raw"
                )
                ==
                F.lit(
                    0
                )
            ),
            F.lit(
                "CMDB Only / No Current Observation"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "N"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                >=
                F.lit(
                    2
                )
            ),
            F.lit(
                "Active Multi-Source / Not in CMDB"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "N"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                F.lit(
                    1
                )
            ),
            F.lit(
                "Active Single-Source / Not in CMDB"
            )
        )

        .when(
            (
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "N"
                )
            )
            &
            (
                F.col(
                    "observed_source_count_active"
                )
                ==
                F.lit(
                    0
                )
            )
            &
            (
                F.col(
                    "observed_source_count_stale"
                )
                >
                F.lit(
                    0
                )
            ),
            F.lit(
                "Stale Source Records Only / Cleanup"
            )
        )

        .otherwise(
            F.lit(
                "Lifecycle Reconciliation Review"
            )
        )
    )

    .withColumn(
        "confidence_score",
        (
            F.when(
                F.col(
                    "cmdb_present"
                )
                ==
                F.lit(
                    "Y"
                ),
                F.lit(
                    25
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
            +
            F.col(
                "nmap_active"
            )
            *
            F.lit(
                15
            )
            +
            F.col(
                "tenable_active"
            )
            *
            F.lit(
                15
            )
            +
            F.col(
                "intune_active"
            )
            *
            F.lit(
                15
            )
            +
            F.col(
                "mde_active"
            )
            *
            F.lit(
                15
            )
            +
            F.col(
                "qualys_active"
            )
            *
            F.lit(
                15
            )
        )
    )

    .withColumn(
        "reconciliation_confidence",

        F.when(
            F.col(
                "confidence_score"
            )
            >=
            F.lit(
                85
            ),
            F.lit(
                "High"
            )
        )

        .when(
            F.col(
                "confidence_score"
            )
            >=
            F.lit(
                60
            ),
            F.lit(
                "Moderate"
            )
        )

        .otherwise(
            F.lit(
                "Low"
            )
        )
    )

    .withColumn(
        "requires_post_validation",

        F.when(
            F.col(
                "cmdb_present"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Y"
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            <=
            F.lit(
                1
            ),
            F.lit(
                "Y"
            )
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >
            F.lit(
                0
            ),
            F.lit(
                "Y"
            )
        )

        .otherwise(
            F.lit(
                "N"
            )
        )
    )

    .withColumn(
        "validation_reason",

        F.when(
            F.col(
                "cmdb_present"
            )
            ==
            F.lit(
                "N"
            ),
            F.lit(
                "Observed asset is not represented by a reconciled CMDB "
                "identity."
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                0
            ),
            F.lit(
                "No active source observation supports the CMDB record."
            )
        )

        .when(
            F.col(
                "observed_source_count_active"
            )
            ==
            F.lit(
                1
            ),
            F.lit(
                "Only one active source currently supports the asset."
            )
        )

        .when(
            F.col(
                "observed_source_count_stale"
            )
            >
            F.lit(
                0
            ),
            F.lit(
                "One or more stale source records require lifecycle review."
            )
        )

        .otherwise(
            F.lit(
                "No immediate Post validation trigger."
            )
        )
    )
)

asset_reconciliation_scored = save_asset_engine_table(
    asset_reconciliation_scored,
    "ASSET_RECONCILIATION_SCORED"
)

rcaip_asset_reconciliation_scored = (
    asset_reconciliation_scored
)


# ---------------------------------------------------------------------------
# 18. Observed assets not represented in CMDB
# ---------------------------------------------------------------------------

observed_not_cmdb = (
    asset_reconciliation_scored

    .filter(
        F.col(
            "cmdb_present"
        )
        ==
        F.lit(
            "N"
        )
    )

    .filter(
        F.col(
            "observed_source_count_raw"
        )
        >
        F.lit(
            0
        )
    )
)

observed_not_cmdb = save_asset_engine_table(
    observed_not_cmdb,
    "OBSERVED_NOT_CMDB"
)


# ---------------------------------------------------------------------------
# 19. Source readiness and one-pass row-count metrics
# ---------------------------------------------------------------------------

asset_engine_metric_dataframes = [
    ("cmdb_row_count", cmdb_norm),
    ("nmap_row_count", nmap_asset_source),
    ("tenable_row_count", tenable_norm),
    ("intune_row_count", intune_norm),
    ("mde_row_count", mde_norm),
    ("qualys_row_count", qualys_norm),
    ("observed_asset_row_count", observed_assets),
    ("observed_asset_rollup_count", observed_assets_rollup),
    ("stale_observed_asset_count", stale_observed_assets),
    ("asset_identity_universe_count", asset_identity_universe),
    ("asset_reconciliation_count", asset_reconciliation),
    ("asset_reconciliation_scored_count", asset_reconciliation_scored),
    ("observed_not_cmdb_count", observed_not_cmdb)
]

asset_engine_metric_frames = []

for (
    metric_name,
    metric_dataframe
) in asset_engine_metric_dataframes:
    asset_engine_metric_frames.append(
        metric_dataframe
        .agg(
            F.count(
                F.lit(1)
            ).cast(
                "long"
            ).alias(
                "metric_value"
            )
        )
        .withColumn(
            "metric_name",
            F.lit(
                metric_name
            )
        )
        .select(
            "metric_name",
            "metric_value"
        )
    )

asset_engine_count_metrics_df = reduce(
    lambda left_dataframe, right_dataframe:
        left_dataframe.unionByName(
            right_dataframe
        ),
    asset_engine_metric_frames
)

asset_engine_count_metrics = {
    metric_row[
        "metric_name"
    ]:
        int(
            metric_row[
                "metric_value"
            ]
            or
            0
        )

    for metric_row
    in asset_engine_count_metrics_df.collect()
}

cmdb_row_count = asset_engine_count_metrics[
    "cmdb_row_count"
]

nmap_row_count = asset_engine_count_metrics[
    "nmap_row_count"
]

tenable_row_count = asset_engine_count_metrics[
    "tenable_row_count"
]

intune_row_count = asset_engine_count_metrics[
    "intune_row_count"
]

mde_row_count = asset_engine_count_metrics[
    "mde_row_count"
]

qualys_row_count = asset_engine_count_metrics[
    "qualys_row_count"
]

observed_asset_row_count = asset_engine_count_metrics[
    "observed_asset_row_count"
]

observed_asset_rollup_count = asset_engine_count_metrics[
    "observed_asset_rollup_count"
]

stale_observed_asset_count = asset_engine_count_metrics[
    "stale_observed_asset_count"
]

asset_identity_universe_count = asset_engine_count_metrics[
    "asset_identity_universe_count"
]

asset_reconciliation_count = asset_engine_count_metrics[
    "asset_reconciliation_count"
]

asset_reconciliation_scored_count = asset_engine_count_metrics[
    "asset_reconciliation_scored_count"
]

observed_not_cmdb_count = asset_engine_count_metrics[
    "observed_not_cmdb_count"
]

source_readiness_rows = [
    (
        "CMDB",
        RCAIP_CMDB_PATH,
        True,
        cmdb_row_count
    ),

    (
        "NMAP",
        ASSET_ENGINE_INPUT_TABLES[
            "NMAP_NORM"
        ],
        True,
        nmap_row_count
    ),

    (
        "Tenable",
        tenable_source_table,
        tenable_source_available,
        tenable_row_count
    ),

    (
        "Intune",
        intune_source_table,
        intune_source_available,
        intune_row_count
    ),

    (
        "MDE",
        mde_source_table,
        mde_source_available,
        mde_row_count
    ),

    (
        "Qualys",
        RCAIP_QUALYS_EASM_PATH,
        qualys_source_available,
        qualys_row_count
    )
]

source_readiness_schema = T.StructType([
    T.StructField(
        "source_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "source_location",
        T.StringType(),
        True
    ),

    T.StructField(
        "source_available",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "normalized_row_count",
        T.LongType(),
        False
    )
])

asset_engine_source_readiness_df = spark.createDataFrame(
    source_readiness_rows,
    schema=source_readiness_schema
)


# ---------------------------------------------------------------------------
# 20. Final certification
# ---------------------------------------------------------------------------

scored_key_profile = (
    asset_reconciliation_scored
    .groupBy(
        "asset_key"
    )
    .agg(
        F.count(
            F.lit(1)
        ).cast(
            "long"
        ).alias(
            "rows_per_asset_key"
        )
    )
    .agg(
        F.sum(
            F.when(
                F.col(
                    "asset_key"
                ).isNull(),
                F.col(
                    "rows_per_asset_key"
                )
            ).otherwise(
                F.lit(0)
            )
        ).cast(
            "long"
        ).alias(
            "null_reconciliation_key_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "asset_key"
                ).isNotNull()
                &
                (
                    F.col(
                        "rows_per_asset_key"
                    )
                    >
                    F.lit(1)
                ),
                F.lit(1)
            ).otherwise(
                F.lit(0)
            )
        ).cast(
            "long"
        ).alias(
            "duplicate_reconciliation_key_count"
        )
    )
    .first()
)

null_reconciliation_key_count = int(
    scored_key_profile[
        "null_reconciliation_key_count"
    ]
    or
    0
)

duplicate_reconciliation_key_count = int(
    scored_key_profile[
        "duplicate_reconciliation_key_count"
    ]
    or
    0
)

missing_durable_outputs = [
    physical_table_name

    for physical_table_name
    in ASSET_ENGINE_OUTPUT_TABLES.values()

    if not spark.catalog.tableExists(
        physical_table_name
    )
]

unique_written_table_count = len(
    set(
        ASSET_ENGINE_WRITTEN_TABLES
    )
)

expected_written_table_count = len(
    ASSET_ENGINE_ALL_OUTPUTS
)

nmap_tables_written_by_asset_engine = [
    written_table_name

    for written_table_name
    in ASSET_ENGINE_WRITTEN_TABLES

    if written_table_name in {
        get_rcaip_table(
            "NMAP_SCAN_PROVENANCE"
        ),
        get_rcaip_table(
            "NMAP_NORM"
        ),
        get_rcaip_table(
            "NMAP_NORM_WITH_PROVENANCE"
        ),
        get_rcaip_table(
            "NMAP_PORTS_ALL"
        ),
        get_rcaip_table(
            "NMAP_PORTS_ALL_WITH_PROVENANCE"
        )
    }
]

cross_post_output_leakage = [
    physical_table_name

    for physical_table_name
    in ASSET_ENGINE_OUTPUT_TABLES.values()

    if not physical_table_name.lower().startswith(
        expected_post_table_prefix.lower()
    )
]

post_mismatch_frames = []

for (
    logical_name,
    output_dataframe
) in ASSET_ENGINE_OUTPUT_DATAFRAMES.items():
    if "post_slug" in output_dataframe.columns:
        post_mismatch_frames.append(
            output_dataframe
            .filter(
                F.col(
                    "post_slug"
                )
                !=
                F.lit(
                    RCAIP_POST_SLUG
                )
            )
            .select(
                F.lit(
                    logical_name
                ).alias(
                    "logical_name"
                )
            )
        )

row_level_post_mismatch_count = (
    reduce(
        lambda left_dataframe, right_dataframe:
            left_dataframe.unionByName(
                right_dataframe
            ),
        post_mismatch_frames
    ).count()

    if post_mismatch_frames

    else 0
)

required_scored_columns = {
    "asset_key",
    "host_key",
    "cmdb_present",
    "nmap_present",
    "tenable_present",
    "intune_present",
    "intune_compliance_state_normalized",
    "intune_compliance_evidence_source",
    "intune_device_health_state",
    "intune_encryption_state",
    "mde_present",
    "mde_onboarding_status",
    "mde_sensor_health_state",
    "mde_exposure_level",
    "qualys_present",
    "observed_source_count_raw",
    "observed_source_count_active",
    "observed_source_count_stale",
    "current_observation_status",
    "visibility_assessment",
    "coverage_gap_summary",
    "lifecycle_reconciliation_status",
    "confidence_score",
    "reconciliation_confidence",
    "requires_post_validation",
    "validation_reason"
}

missing_scored_columns = sorted(
    required_scored_columns
    -
    set(
        asset_reconciliation_scored.columns
    )
)

certification_checks = [
    (
        "Config 000 ready",
        "True",
        str(
            RCAIP_CONFIG_000_READY
        ),
        bool(
            RCAIP_CONFIG_000_READY
        )
    ),

    (
        "CMDB normalized rows",
        "At least one",
        str(
            cmdb_row_count
        ),
        cmdb_row_count > 0
    ),

    (
        "Certified IP-001 Nmap rows",
        "At least one",
        str(
            nmap_row_count
        ),
        nmap_row_count > 0
    ),

    (
        "Observed asset records",
        "At least one",
        str(
            observed_asset_row_count
        ),
        observed_asset_row_count > 0
    ),

    (
        "Observed asset rollup",
        "At least one",
        str(
            observed_asset_rollup_count
        ),
        observed_asset_rollup_count > 0
    ),

    (
        "Reconciliation covers CMDB",
        f"At least {cmdb_row_count}",
        str(
            asset_reconciliation_count
        ),
        asset_reconciliation_count
        >=
        cmdb_row_count
    ),

    (
        "Scored reconciliation parity",
        str(
            asset_reconciliation_count
        ),
        str(
            asset_reconciliation_scored_count
        ),
        asset_reconciliation_scored_count
        ==
        asset_reconciliation_count
    ),

    (
        "Reconciliation equals identity universe",
        str(
            asset_identity_universe_count
        ),
        str(
            asset_reconciliation_count
        ),
        asset_reconciliation_count
        ==
        asset_identity_universe_count
    ),

    (
        "Null reconciliation keys",
        "0",
        str(
            null_reconciliation_key_count
        ),
        null_reconciliation_key_count == 0
    ),

    (
        "Duplicate reconciliation keys",
        "0",
        str(
            duplicate_reconciliation_key_count
        ),
        duplicate_reconciliation_key_count == 0
    ),

    (
        "Required scored columns",
        "All present",
        (
            "PASS"
            if not missing_scored_columns
            else
            " | ".join(
                missing_scored_columns
            )
        ),
        not missing_scored_columns
    ),

    (
        "Expected durable tables written",
        str(
            expected_written_table_count
        ),
        str(
            unique_written_table_count
        ),
        unique_written_table_count
        ==
        expected_written_table_count
    ),

    (
        "Missing durable outputs",
        "0",
        str(
            len(
                missing_durable_outputs
            )
        ),
        len(
            missing_durable_outputs
        )
        ==
        0
    ),

    (
        "Nmap tables overwritten by Asset Engine",
        "0",
        str(
            len(
                nmap_tables_written_by_asset_engine
            )
        ),
        len(
            nmap_tables_written_by_asset_engine
        )
        ==
        0
    ),

    (
        "Cross-Post output bindings",
        "0",
        str(
            len(
                cross_post_output_leakage
            )
        ),
        len(
            cross_post_output_leakage
        )
        ==
        0
    ),

    (
        "Cross-Post output rows",
        "0",
        str(
            row_level_post_mismatch_count
        ),
        row_level_post_mismatch_count
        ==
        0
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

asset_engine_certificate_df = spark.createDataFrame(
    certification_checks,
    schema=certificate_schema
)

failed_certification_checks = [
    certification_check

    for certification_check
    in certification_checks

    if not certification_check[
        3
    ]
]

RCAIP_ASSET_ENGINE_CERTIFIED = (
    not failed_certification_checks
)

RCAIP_ASSET_ENGINE_READY = (
    RCAIP_ASSET_ENGINE_CERTIFIED
)


# ---------------------------------------------------------------------------
# 21. Final runtime contract
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP ASSET ENGINE V3 - FINAL RUNTIME CONTRACT")
print("=" * 100)

print(
    "Status:",
    (
        "PASS"
        if RCAIP_ASSET_ENGINE_CERTIFIED
        else
        "FAIL"
    )
)

print(
    "Ready:",
    RCAIP_ASSET_ENGINE_READY
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
    "CMDB normalized rows:",
    cmdb_row_count
)

print(
    "IP-001 Nmap rows consumed:",
    nmap_row_count
)

print(
    "Tenable normalized rows:",
    tenable_row_count
)

print(
    "Intune normalized rows:",
    intune_row_count
)

print(
    "MDE normalized rows:",
    mde_row_count
)

print(
    "Qualys normalized rows:",
    qualys_row_count
)

print(
    "Observed asset records:",
    observed_asset_row_count
)

print(
    "Observed asset rollup rows:",
    observed_asset_rollup_count
)

print(
    "Stale-only observed assets:",
    stale_observed_asset_count
)

print(
    "Asset reconciliation rows:",
    asset_reconciliation_count
)

print(
    "Scored Asset Plane rows:",
    asset_reconciliation_scored_count
)

print(
    "Observed assets not in CMDB:",
    observed_not_cmdb_count
)

print(
    "Durable tables written:",
    unique_written_table_count
)

print(
    "Missing durable outputs:",
    len(
        missing_durable_outputs
    )
)

print(
    "Nmap tables overwritten by Asset Engine:",
    len(
        nmap_tables_written_by_asset_engine
    )
)

print(
    "Cross-Post output leakage:",
    len(
        cross_post_output_leakage
    )
    +
    row_level_post_mismatch_count
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


asset_engine_write_timing_schema = T.StructType([
    T.StructField(
        "logical_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "physical_table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "write_elapsed_seconds",
        T.DoubleType(),
        False
    )
])

asset_engine_write_timing_df = spark.createDataFrame(
    [
        (
            timing_record[
                "logical_name"
            ],
            timing_record[
                "physical_table_name"
            ],
            timing_record[
                "write_elapsed_seconds"
            ]
        )

        for timing_record
        in ASSET_ENGINE_WRITE_TIMINGS
    ],
    schema=asset_engine_write_timing_schema
)

print(
    "Slowest durable write:"
)

if ASSET_ENGINE_WRITE_TIMINGS:
    slowest_write = max(
        ASSET_ENGINE_WRITE_TIMINGS,
        key=lambda timing_record:
            timing_record[
                "write_elapsed_seconds"
            ]
    )

    print(
        slowest_write[
            "logical_name"
        ],
        "| seconds:",
        round(
            slowest_write[
                "write_elapsed_seconds"
            ],
            2
        )
    )


display(
    asset_engine_write_timing_df
    .orderBy(
        F.desc(
            "write_elapsed_seconds"
        )
    )
)


display(
    asset_engine_join_input_profile_df
    .orderBy(
        "join_input_name"
    )
)


display(
    asset_engine_certificate_df
    .orderBy(
        F.col(
            "check_passed"
        ).asc(),
        "certification_check"
    )
)

display(
    asset_engine_source_readiness_df
)

display(
    source_lifecycle_counts
)

display(
    asset_reconciliation_scored

    .select(
        "asset_key",
        "host_key",
        "cmdb_present",
        "observed_sources_raw",
        "observed_sources_active",
        "observed_sources_stale",
        "current_observation_status",
        "visibility_assessment",
        "lifecycle_reconciliation_status",
        "confidence_score",
        "reconciliation_confidence",
        "requires_post_validation",
        "validation_reason"
    )

    .limit(
        100
    )
)


if failed_certification_checks:
    raise RuntimeError(
        "Asset Engine certification failed: "
        +
        " | ".join(
            failed_check[
                0
            ]

            for failed_check
            in failed_certification_checks
        )
    )