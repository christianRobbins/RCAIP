# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP IP-001
# Enterprise Open-Port Intelligence and Nmap Source Ingestion
#
# Purpose:
#   Read the certified Nmap XML manifest for the active RCAIP assessment,
#   create normalized Nmap evidence tables, and produce the complete IP-001
#   open-port work package.
#
# Runtime:
#   Cell 1 runs RCAIP_Config_001_Table_Registry.
#   Config 001 runs Config 000 and resolves the certified Post and CSA run.
#
# Security model:
#   - No file:/tmp use.
#   - No WorkspaceLocalFileSystem use.
#   - XML is read through Spark binaryFile from the trusted DBFS path.
#
# Operating rules:
#   - No hard-coded Post identity.
#   - No manually entered filenames, CIDRs, dates, or output names.
#   - Every durable table resolves through Config 001.
#   - Run All is safe for a newly onboarded Post.
#   - No automatic findings, assignments, tickets, or remediation.
# ============================================================================

import hashlib
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate the inherited RCAIP runtime
# ---------------------------------------------------------------------------

required_runtime_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_RUN_ID",
    "RCAIP_RUN_MODE",
    "RCAIP_RELEASE_VERSION",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_ASSESSMENT_START_DATE",
    "RCAIP_ASSESSMENT_END_DATE",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "RCAIP_INPUT_BASE_DIR",
    "RCAIP_NMAP_SCANS",
    "RCAIP_NMAP_SCANNER_NAME",
    "RCAIP_NMAP_SCANNER_IP",
    "RCAIP_NMAP_SCANNER_TRUST",
    "RCAIP_NMAP_SCANNER_TRUST_DISPLAY",
    "RCAIP_NMAP_EVIDENCE_CONFIDENCE",
    "RCAIP_NMAP_COLLECTION_METHOD",
    "RCAIP_NMAP_COLLECTOR",
    "RCAIP_NMAP_AUTHORIZATION_REFERENCE",
    "get_rcaip_table"
]

missing_runtime_objects = [
    object_name
    for object_name in required_runtime_objects
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-001 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready runtime."
    )

allowed_ip001_run_modes = {
    "DISCOVERY",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}

if str(RCAIP_RUN_MODE).upper() not in allowed_ip001_run_modes:
    raise RuntimeError(
        "IP-001 cannot run in the current certified run mode. "
        f"Observed mode: {RCAIP_RUN_MODE}"
    )

if not RCAIP_NMAP_SCANS:
    raise RuntimeError(
        "The certified intake does not contain an Nmap scan manifest."
    )

if not callable(get_rcaip_table):
    raise TypeError(
        "get_rcaip_table must be callable."
    )

# Operator prepares this prospective manifest and executes verified source bytes
# in the existing approved Config session. No automatic version selection.
if "RCAIP_IP001_EXECUTION" not in globals() or "RCAIP_IP001_BINDING_API" not in globals():
    raise RuntimeError("Explicit prospective IP001 execution binding required")
if RCAIP_IP001_EXECUTION["state"] != "PREPARED":
    raise RuntimeError("IP001 execution binding must be freshly prepared")
if (RCAIP_IP001_EXECUTION["post_slug"] != RCAIP_POST_SLUG or
        RCAIP_IP001_EXECUTION["rcaip_run_id"] != RCAIP_RUN_ID):
    raise RuntimeError("IP001 prospective scope mismatch")
if spark.conf.get("spark.sql.session.timeZone") not in ("UTC", "Etc/UTC", "+00:00", "Z"):
    raise RuntimeError("IP001 prospective binding requires UTC session; configuration unchanged")
RCAIP_IP001_READY = False
RCAIP_PORT_REPORT_BINDINGS = RCAIP_IP001_BINDING_API.port_report_bindings(RCAIP_IP001_EXECUTION)
RCAIP_IP001_EXECUTION["state"] = "RUNNING"
IP001_GENERATED_AT_UTC = RCAIP_IP001_EXECUTION["generation"]


# ---------------------------------------------------------------------------
# 2. Registered table contracts
# ---------------------------------------------------------------------------

IP001_SOURCE_LOGICAL_TABLES = [
    "NMAP_SCAN_PROVENANCE",
    "NMAP_NORM",
    "NMAP_NORM_WITH_PROVENANCE",
    "NMAP_PORTS_ALL",
    "NMAP_PORTS_ALL_WITH_PROVENANCE"
]

IP001_PRODUCT_LOGICAL_TABLES = [
    "IP001_SCAN_PROVENANCE_SUMMARY",

    "IP001_DEVICE_PORT_PRIORITIZED",
    "IP001_DEVICE_PORT_PRIORITIZED_WITH_PROVENANCE",

    "IP001_DEVICE_PORT_ROLLUP",
    "IP001_DEVICE_PORT_ROLLUP_WITH_PROVENANCE",

    "IP001_NEN_PUBLIC_EXPOSURE",
    "IP001_NEN_PUBLIC_EXPOSURE_WITH_PROVENANCE",

    "IP001_OPEN_PORT_REVIEW",
    "IP001_OPEN_PORT_REVIEW_WITH_PROVENANCE",

    "IP001_PORT_SUMMARY",

    "IP001_POST_ACTION_TABLE",
    "IP001_POST_ACTION_TABLE_WITH_PROVENANCE",

    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW",
    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW_WITH_PROVENANCE",

    "IP001_Q1143_PRINTER_MFD_PORTS",
    "IP001_Q1143_PRINTER_MFD_PORTS_WITH_PROVENANCE",

    "IP001_Q1173_ENTWIFI_PORTS",
    "IP001_Q1173_ENTWIFI_PORTS_WITH_PROVENANCE",

    "IP001_TECHNICAL_APPENDIX",
    "IP001_TECHNICAL_APPENDIX_WITH_PROVENANCE",

    "IP001_WORK_PACKAGE_SUMMARY",
    "IP001_PORT_EMAIL_SUMMARY"
]

IP001_ALL_POST_LOGICAL_TABLES = (
    IP001_SOURCE_LOGICAL_TABLES
    +
    IP001_PRODUCT_LOGICAL_TABLES
)

IP001_PHYSICAL_TABLES = {
    logical_name:
        get_rcaip_table(
            logical_name
        )

    for logical_name
    in IP001_ALL_POST_LOGICAL_TABLES
}

IP001_PORT_KB_TABLE = get_rcaip_table(
    "KB_PORT_RULES"
)

expected_post_table_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    f"{RCAIP_POST_SLUG}_"
)

invalid_registered_bindings = {
    logical_name:
        physical_table_name

    for (
        logical_name,
        physical_table_name
    )
    in IP001_PHYSICAL_TABLES.items()

    if not str(
        physical_table_name
    ).lower().startswith(
        expected_post_table_prefix.lower()
    )
}

if invalid_registered_bindings:
    raise RuntimeError(
        "One or more IP-001 registry bindings are not scoped to the "
        "certified Post: "
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
            in invalid_registered_bindings.items()
        )
    )


# ---------------------------------------------------------------------------
# 3. Generic helpers
# ---------------------------------------------------------------------------

def dbfs_path_join(
    base_path,
    file_name
):
    return (
        str(
            base_path
        ).rstrip("/")
        +
        "/"
        +
        str(
            file_name
        ).lstrip("/")
    )


def safe_text(
    value
):
    if value is None:
        return None

    normalized_value = str(
        value
    ).strip()

    return (
        normalized_value
        if normalized_value
        else None
    )


def safe_integer(
    value
):
    try:
        return int(
            value
        )

    except Exception:
        return None


def safe_float(
    value
):
    try:
        return float(
            value
        )

    except Exception:
        return None


def epoch_to_utc_timestamp(
    epoch_value
):
    numeric_epoch = safe_float(
        epoch_value
    )

    if numeric_epoch is None:
        return None

    return (
        datetime.fromtimestamp(
            numeric_epoch,
            tz=timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )


def sha256_bytes(
    binary_content
):
    return hashlib.sha256(
        bytes(
            binary_content
        )
    ).hexdigest()


def resolve_column_name(
    dataframe,
    candidate_names
):
    column_lookup = {
        str(
            column_name
        ).lower():
            column_name

        for column_name
        in dataframe.columns
    }

    for candidate_name in candidate_names:
        matched_column = column_lookup.get(
            str(
                candidate_name
            ).lower()
        )

        if matched_column:
            return matched_column

    return None


def source_column(
    dataframe,
    candidate_names,
    cast_type="string"
):
    resolved_name = resolve_column_name(
        dataframe,
        candidate_names
    )

    if resolved_name is None:
        return F.lit(
            None
        ).cast(
            cast_type
        )

    return F.col(
        f"`{resolved_name}`"
    ).cast(
        cast_type
    )


def classify_scan_freshness(
    scan_age_days
):
    if "classify_evidence_freshness" in globals():
        return classify_evidence_freshness(
            scan_age_days
        )

    if scan_age_days is None:
        return "Unknown Scan Date"

    if scan_age_days < 0:
        return "Invalid Future Date"

    if scan_age_days <= 30:
        return "Current"

    if scan_age_days <= 90:
        return "Recent"

    return "Stale Historical"


def add_runtime_columns(
    dataframe
):
    output_dataframe = dataframe

    runtime_columns = {
        "rcaip_run_id":
            RCAIP_RUN_ID,

        "runtime_post_name":
            RCAIP_POST_NAME,

        "runtime_post_slug":
            RCAIP_POST_SLUG,

        "runtime_country_name":
            RCAIP_COUNTRY_NAME,

        "runtime_run_mode":
            RCAIP_RUN_MODE
    }

    for (
        column_name,
        column_value
    ) in runtime_columns.items():

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
                F.lit(
                    IP001_GENERATED_AT_UTC
                ).cast(
                    "timestamp"
                )
            )
        )

    return output_dataframe


IP001_WRITTEN_TABLES = []


def ip001_write_with_metadata(spark, dataframe, table, token, mode, overwrite_schema=False):
    """Sequential scoped classic-compute metadata; no fallback to untagged writes.
    All affected writes must use this helper on the same SparkSession object.
    Operators must exclude unrelated concurrent writes/config changes in session.
    """
    import threading
    key = "spark.databricks.delta.commitInfo.userMetadata"
    if not isinstance(token, str) or not token:
        raise RuntimeError("IP001_WRITE_TOKEN_REQUIRED")
    # Share the lock even when this function is extracted into another namespace.
    lock = spark.__dict__.setdefault("_rcaip_ip001_commit_metadata_lock", threading.Lock())
    with lock:
        try:
            if not spark.conf.isModifiable(key):
                raise RuntimeError("IP001_COMMIT_METADATA_UNSUPPORTED")
            previous = spark.conf.get(key, None)  # None is absent; empty stays empty.
            if previous is not None and not isinstance(previous, str):
                raise RuntimeError("IP001_COMMIT_METADATA_STATE_UNAVAILABLE")
        except Exception as exc:
            raise RuntimeError("IP001_COMMIT_METADATA_UNAVAILABLE_BEFORE_WRITE") from exc
        try:
            spark.conf.set(key, token)
            if spark.conf.get(key, None) != token:
                raise RuntimeError("IP001_COMMIT_METADATA_NOT_VERIFIED")
            writer = dataframe.write.option("userMetadata", token).mode(mode).format("delta")
            if overwrite_schema:
                writer = writer.option("overwriteSchema", "true")
            writer.saveAsTable(table)
        finally:
            try:
                if previous is None:
                    spark.conf.unset(key)
                else:
                    spark.conf.set(key, previous)
                if spark.conf.get(key, None) != previous:
                    raise RuntimeError("IP001_COMMIT_METADATA_RESTORE_NOT_VERIFIED")
            except Exception as exc:
                # A write may already have committed. STOP; never blindly retry.
                raise RuntimeError("IP001_COMMIT_METADATA_RESTORATION_FAILED") from exc


def save_post_table(
    dataframe,
    logical_name
):
    if logical_name not in IP001_PHYSICAL_TABLES:
        raise KeyError(
            "Logical table is not part of the certified IP-001 contract: "
            +
            logical_name
        )

    physical_table_name = IP001_PHYSICAL_TABLES[
        logical_name
    ]

    if not physical_table_name.lower().startswith(
        expected_post_table_prefix.lower()
    ):
        raise RuntimeError(
            "Cross-Post output protection blocked this table: "
            +
            physical_table_name
        )

    output_dataframe = add_runtime_columns(
        dataframe
    )

    prior_table_id = (RCAIP_IP001_BINDING_API.table_identity(spark, physical_table_name)
                      if spark.catalog.tableExists(physical_table_name) else None)
    write_token = RCAIP_IP001_BINDING_API.output_write_token(RCAIP_IP001_EXECUTION, logical_name)
    ip001_write_with_metadata(spark, output_dataframe, physical_table_name,
                              write_token, "overwrite", overwrite_schema=True)

    RCAIP_IP001_BINDING_API.capture_output(spark, RCAIP_IP001_EXECUTION,
        logical_name, physical_table_name, write_token, prior_table_id)

    IP001_WRITTEN_TABLES.append(
        physical_table_name
    )

    print(
        "Saved:",
        physical_table_name
    )

    return physical_table_name


def enrich_with_scan_provenance(
    source_dataframe,
    provenance_dataframe
):
    if "scan_id" not in source_dataframe.columns:
        raise RuntimeError(
            "A provenance-enriched IP-001 product does not contain scan_id."
        )

    additional_provenance_columns = [
        column_name

        for column_name
        in provenance_dataframe.columns

        if (
            column_name != "scan_id"
            and
            column_name not in source_dataframe.columns
        )
    ]

    return (
        source_dataframe.alias(
            "base"
        )

        .join(
            provenance_dataframe.alias(
                "provenance"
            ),
            on="scan_id",
            how="left"
        )

        .select(
            "base.*",

            *[
                F.col(
                    f"provenance.`{column_name}`"
                ).alias(
                    column_name
                )

                for column_name
                in additional_provenance_columns
            ]
        )
    )


# ---------------------------------------------------------------------------
# 4. Normalized Nmap schemas
# ---------------------------------------------------------------------------

nmap_scan_provenance_schema = T.StructType([
    T.StructField(
        "post_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "post_slug",
        T.StringType(),
        False
    ),

    T.StructField(
        "rcaip_run_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_domain",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_scope",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_location",
        T.StringType(),
        False
    ),

    T.StructField(
        "target_cidr",
        T.StringType(),
        True
    ),

    T.StructField(
        "source_file_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "source_file_path",
        T.StringType(),
        False
    ),

    T.StructField(
        "source_file_modified_timestamp",
        T.TimestampType(),
        True
    ),

    T.StructField(
        "source_file_size_bytes",
        T.LongType(),
        True
    ),

    T.StructField(
        "source_file_sha256",
        T.StringType(),
        False
    ),

    T.StructField(
        "nmap_version",
        T.StringType(),
        True
    ),

    T.StructField(
        "nmap_xml_output_version",
        T.StringType(),
        True
    ),

    T.StructField(
        "scan_arguments",
        T.StringType(),
        True
    ),

    T.StructField(
        "scan_target_specification",
        T.StringType(),
        True
    ),

    T.StructField(
        "scan_start_timestamp",
        T.TimestampType(),
        True
    ),

    T.StructField(
        "scan_end_timestamp",
        T.TimestampType(),
        True
    ),

    T.StructField(
        "scan_duration_seconds",
        T.DoubleType(),
        True
    ),

    T.StructField(
        "scan_age_days",
        T.LongType(),
        True
    ),

    T.StructField(
        "scan_freshness_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "scanner_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "scanner_ip",
        T.StringType(),
        False
    ),

    T.StructField(
        "scanner_trust_class",
        T.StringType(),
        False
    ),

    T.StructField(
        "scanner_trust_display",
        T.StringType(),
        False
    ),

    T.StructField(
        "evidence_confidence",
        T.StringType(),
        False
    ),

    T.StructField(
        "collection_method",
        T.StringType(),
        False
    ),

    T.StructField(
        "collector",
        T.StringType(),
        False
    ),

    T.StructField(
        "authorization_reference",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_hosts_up",
        T.LongType(),
        True
    ),

    T.StructField(
        "scan_hosts_down",
        T.LongType(),
        True
    ),

    T.StructField(
        "scan_hosts_total",
        T.LongType(),
        True
    ),

    T.StructField(
        "mapping_status",
        T.StringType(),
        False
    )
])


nmap_host_schema = T.StructType([
    T.StructField(
        "post_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "post_slug",
        T.StringType(),
        False
    ),

    T.StructField(
        "rcaip_run_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_domain",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_scope",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_location",
        T.StringType(),
        False
    ),

    T.StructField(
        "target_cidr",
        T.StringType(),
        True
    ),

    T.StructField(
        "scan_source_file",
        T.StringType(),
        False
    ),

    T.StructField(
        "host_key",
        T.StringType(),
        False
    ),

    T.StructField(
        "ip_address",
        T.StringType(),
        True
    ),

    T.StructField(
        "nmap_hostname",
        T.StringType(),
        True
    ),

    T.StructField(
        "hostname",
        T.StringType(),
        True
    ),

    T.StructField(
        "host_status",
        T.StringType(),
        True
    ),

    T.StructField(
        "mac_address",
        T.StringType(),
        True
    ),

    T.StructField(
        "vendor",
        T.StringType(),
        True
    ),

    T.StructField(
        "os_name",
        T.StringType(),
        True
    ),

    T.StructField(
        "nmap_services",
        T.StringType(),
        True
    ),

    T.StructField(
        "nmap_ports",
        T.StringType(),
        True
    ),

    T.StructField(
        "open_port_count",
        T.LongType(),
        False
    ),

    T.StructField(
        "nmap_present",
        T.StringType(),
        False
    )
])


nmap_port_schema = T.StructType([
    T.StructField(
        "post_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "post_slug",
        T.StringType(),
        False
    ),

    T.StructField(
        "rcaip_run_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_domain",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_scope",
        T.StringType(),
        False
    ),

    T.StructField(
        "scan_location",
        T.StringType(),
        False
    ),

    T.StructField(
        "target_cidr",
        T.StringType(),
        True
    ),

    T.StructField(
        "scan_source_file",
        T.StringType(),
        False
    ),

    T.StructField(
        "host_key",
        T.StringType(),
        False
    ),

    T.StructField(
        "ip_address",
        T.StringType(),
        True
    ),

    T.StructField(
        "hostname",
        T.StringType(),
        True
    ),

    T.StructField(
        "nmap_hostname",
        T.StringType(),
        True
    ),

    T.StructField(
        "host_status",
        T.StringType(),
        True
    ),

    T.StructField(
        "protocol",
        T.StringType(),
        True
    ),

    T.StructField(
        "port",
        T.IntegerType(),
        True
    ),

    T.StructField(
        "port_state",
        T.StringType(),
        True
    ),

    T.StructField(
        "port_reason",
        T.StringType(),
        True
    ),

    T.StructField(
        "service",
        T.StringType(),
        True
    ),

    T.StructField(
        "product",
        T.StringType(),
        True
    ),

    T.StructField(
        "version",
        T.StringType(),
        True
    ),

    T.StructField(
        "extrainfo",
        T.StringType(),
        True
    ),

    T.StructField(
        "tunnel",
        T.StringType(),
        True
    ),

    T.StructField(
        "service_method",
        T.StringType(),
        True
    ),

    T.StructField(
        "service_confidence",
        T.IntegerType(),
        True
    ),

    T.StructField(
        "cpe",
        T.StringType(),
        True
    )
])


# ---------------------------------------------------------------------------
# 5. Parse every XML file in the certified intake manifest
#
# Spark binaryFile reads the source from the trusted DBFS filesystem.
# No file:/tmp copy is attempted.
# ---------------------------------------------------------------------------

provenance_rows = []
host_rows = []
port_rows = []
parse_error_rows = []

required_manifest_fields = {
    "file",
    "asset_domain",
    "scan_scope",
    "scan_location",
    "target_cidr",
    "notes"
}

for (
    scan_index,
    scan_manifest_record
) in enumerate(
    RCAIP_NMAP_SCANS,
    start=1
):
    missing_manifest_fields = sorted(
        required_manifest_fields
        -
        set(
            scan_manifest_record.keys()
        )
    )

    if missing_manifest_fields:
        raise RuntimeError(
            f"Nmap manifest record {scan_index} is incomplete: "
            +
            " | ".join(
                missing_manifest_fields
            )
        )

    source_file_name = str(
        scan_manifest_record[
            "file"
        ]
    ).strip()

    asset_domain = str(
        scan_manifest_record[
            "asset_domain"
        ]
    ).strip()

    scan_scope = str(
        scan_manifest_record[
            "scan_scope"
        ]
    ).strip()

    scan_location = str(
        scan_manifest_record[
            "scan_location"
        ]
    ).strip()

    target_cidr = safe_text(
        scan_manifest_record.get(
            "target_cidr"
        )
    )

    source_file_path = dbfs_path_join(
        RCAIP_INPUT_BASE_DIR,
        source_file_name
    )

    try:
        # -------------------------------------------------------------------
        # Trusted-filesystem XML read
        # -------------------------------------------------------------------

        source_binary_rows = (
            spark.read
            .format(
                "binaryFile"
            )
            .load(
                source_file_path
            )
            .select(
                "path",
                "modificationTime",
                "length",
                "content"
            )
            .collect()
        )

        if len(
            source_binary_rows
        ) != 1:
            raise RuntimeError(
                "Expected exactly one certified Nmap XML file at "
                f"{source_file_path}; Spark returned "
                f"{len(source_binary_rows)} records."
            )

        source_binary_row = source_binary_rows[
            0
        ]

        source_file_size_bytes = safe_integer(
            source_binary_row[
                "length"
            ]
        )

        source_file_modified_timestamp = (
            source_binary_row[
                "modificationTime"
            ]
        )

        source_file_content = bytes(
            source_binary_row[
                "content"
            ]
        )

        if not source_file_content:
            raise RuntimeError(
                "The certified Nmap XML file is empty: "
                +
                source_file_path
            )

        source_file_sha256 = sha256_bytes(
            source_file_content
        )

        nmap_root = ET.fromstring(
            source_file_content
        )

        root_local_name = str(
            nmap_root.tag
        ).split(
            "}"
        )[-1]

        if root_local_name != "nmaprun":
            raise ValueError(
                "The XML root element is not nmaprun."
            )

        nmap_version = safe_text(
            nmap_root.attrib.get(
                "version"
            )
        )

        nmap_xml_output_version = safe_text(
            nmap_root.attrib.get(
                "xmloutputversion"
            )
        )

        scan_arguments = safe_text(
            nmap_root.attrib.get(
                "args"
            )
        )

        scan_start_timestamp = epoch_to_utc_timestamp(
            nmap_root.attrib.get(
                "start"
            )
        )

        finished_element = nmap_root.find(
            "./runstats/finished"
        )

        scan_end_timestamp = (
            epoch_to_utc_timestamp(
                finished_element.attrib.get(
                    "time"
                )
            )

            if finished_element is not None

            else None
        )

        scan_duration_seconds = (
            safe_float(
                finished_element.attrib.get(
                    "elapsed"
                )
            )

            if finished_element is not None

            else None
        )

        if (
            scan_end_timestamp is None
            and
            scan_start_timestamp is not None
            and
            scan_duration_seconds is not None
        ):
            scan_end_timestamp = (
                scan_start_timestamp
                +
                timedelta(
                    seconds=scan_duration_seconds
                )
            )

        if (
            scan_duration_seconds is None
            and
            scan_start_timestamp is not None
            and
            scan_end_timestamp is not None
        ):
            scan_duration_seconds = (
                scan_end_timestamp
                -
                scan_start_timestamp
            ).total_seconds()

        scan_age_days = (
            (
                RCAIP_ASSESSMENT_END_DATE
                -
                scan_start_timestamp.date()
            ).days

            if scan_start_timestamp is not None

            else None
        )

        scan_freshness_status = classify_scan_freshness(
            scan_age_days
        )

        scan_id = hashlib.sha256(
            (
                RCAIP_RUN_ID
                +
                "|"
                +
                source_file_path
                +
                "|"
                +
                source_file_sha256
            ).encode(
                "utf-8"
            )
        ).hexdigest()[
            :32
        ]

        current_scan_host_rows = []
        current_scan_port_rows = []

        for (
            host_index,
            host_element
        ) in enumerate(
            nmap_root.findall(
                "host"
            ),
            start=1
        ):
            status_element = host_element.find(
                "status"
            )

            host_status = (
                safe_text(
                    status_element.attrib.get(
                        "state"
                    )
                )

                if status_element is not None

                else None
            )

            ip_address = None
            mac_address = None
            vendor = None

            for address_element in host_element.findall(
                "address"
            ):
                address_type = str(
                    address_element.attrib.get(
                        "addrtype",
                        ""
                    )
                ).lower()

                address_value = safe_text(
                    address_element.attrib.get(
                        "addr"
                    )
                )

                if (
                    address_type == "ipv4"
                    and
                    address_value
                ):
                    ip_address = address_value

                elif (
                    address_type == "mac"
                    and
                    address_value
                ):
                    mac_address = address_value

                    vendor = safe_text(
                        address_element.attrib.get(
                            "vendor"
                        )
                    )

            hostname_values = [
                safe_text(
                    hostname_element.attrib.get(
                        "name"
                    )
                )

                for hostname_element
                in host_element.findall(
                    "./hostnames/hostname"
                )
            ]

            hostname_values = [
                hostname_value

                for hostname_value
                in hostname_values

                if hostname_value
            ]

            nmap_hostname = (
                hostname_values[
                    0
                ]

                if hostname_values

                else None
            )

            hostname = nmap_hostname

            host_identity_value = (
                hostname
                or
                ip_address
                or
                mac_address
                or
                (
                    f"{source_file_name}_"
                    f"HOST_{host_index}"
                )
            )

            host_key = str(
                host_identity_value
            ).strip().upper()

            os_match_element = host_element.find(
                "./os/osmatch"
            )

            os_name = (
                safe_text(
                    os_match_element.attrib.get(
                        "name"
                    )
                )

                if os_match_element is not None

                else None
            )

            host_port_records = []

            for port_element in host_element.findall(
                "./ports/port"
            ):
                state_element = port_element.find(
                    "state"
                )

                service_element = port_element.find(
                    "service"
                )

                protocol = safe_text(
                    port_element.attrib.get(
                        "protocol"
                    )
                )

                port_number = safe_integer(
                    port_element.attrib.get(
                        "portid"
                    )
                )

                port_state = (
                    safe_text(
                        state_element.attrib.get(
                            "state"
                        )
                    )

                    if state_element is not None

                    else None
                )

                port_reason = (
                    safe_text(
                        state_element.attrib.get(
                            "reason"
                        )
                    )

                    if state_element is not None

                    else None
                )

                service_name = (
                    safe_text(
                        service_element.attrib.get(
                            "name"
                        )
                    )

                    if service_element is not None

                    else None
                )

                product = (
                    safe_text(
                        service_element.attrib.get(
                            "product"
                        )
                    )

                    if service_element is not None

                    else None
                )

                version = (
                    safe_text(
                        service_element.attrib.get(
                            "version"
                        )
                    )

                    if service_element is not None

                    else None
                )

                extrainfo = (
                    safe_text(
                        service_element.attrib.get(
                            "extrainfo"
                        )
                    )

                    if service_element is not None

                    else None
                )

                tunnel = (
                    safe_text(
                        service_element.attrib.get(
                            "tunnel"
                        )
                    )

                    if service_element is not None

                    else None
                )

                service_method = (
                    safe_text(
                        service_element.attrib.get(
                            "method"
                        )
                    )

                    if service_element is not None

                    else None
                )

                service_confidence = (
                    safe_integer(
                        service_element.attrib.get(
                            "conf"
                        )
                    )

                    if service_element is not None

                    else None
                )

                cpe_values = (
                    [
                        safe_text(
                            cpe_element.text
                        )

                        for cpe_element
                        in service_element.findall(
                            "cpe"
                        )
                    ]

                    if service_element is not None

                    else []
                )

                cpe_values = [
                    cpe_value

                    for cpe_value
                    in cpe_values

                    if cpe_value
                ]

                cpe_text = (
                    " | ".join(
                        sorted(
                            set(
                                cpe_values
                            )
                        )
                    )

                    if cpe_values

                    else None
                )

                current_scan_port_rows.append(
                    (
                        RCAIP_POST_NAME,
                        RCAIP_POST_SLUG,
                        RCAIP_RUN_ID,

                        scan_id,

                        asset_domain,
                        scan_scope,
                        scan_location,
                        target_cidr,
                        source_file_name,

                        host_key,
                        ip_address,
                        hostname,
                        nmap_hostname,
                        host_status,

                        protocol,
                        port_number,
                        port_state,
                        port_reason,

                        service_name,
                        product,
                        version,
                        extrainfo,
                        tunnel,
                        service_method,
                        service_confidence,
                        cpe_text
                    )
                )

                host_port_records.append({
                    "port":
                        port_number,

                    "state":
                        port_state,

                    "service":
                        service_name
                })

            open_host_ports = sorted({
                port_record[
                    "port"
                ]

                for port_record
                in host_port_records

                if (
                    str(
                        port_record[
                            "state"
                        ]
                        or
                        ""
                    ).lower()
                    ==
                    "open"
                    and
                    port_record[
                        "port"
                    ] is not None
                )
            })

            open_host_services = sorted({
                port_record[
                    "service"
                ]

                for port_record
                in host_port_records

                if (
                    str(
                        port_record[
                            "state"
                        ]
                        or
                        ""
                    ).lower()
                    ==
                    "open"
                    and
                    port_record[
                        "service"
                    ]
                )
            })

            current_scan_host_rows.append(
                (
                    RCAIP_POST_NAME,
                    RCAIP_POST_SLUG,
                    RCAIP_RUN_ID,

                    scan_id,

                    asset_domain,
                    scan_scope,
                    scan_location,
                    target_cidr,
                    source_file_name,

                    host_key,
                    ip_address,
                    nmap_hostname,
                    hostname,
                    host_status,

                    mac_address,
                    vendor,
                    os_name,

                    (
                        ", ".join(
                            open_host_services
                        )

                        if open_host_services

                        else None
                    ),

                    (
                        ", ".join(
                            str(
                                port_number
                            )

                            for port_number
                            in open_host_ports
                        )

                        if open_host_ports

                        else None
                    ),

                    len(
                        open_host_ports
                    ),

                    "Y"
                )
            )

        host_rows.extend(
            current_scan_host_rows
        )

        port_rows.extend(
            current_scan_port_rows
        )

        runstats_hosts_element = nmap_root.find(
            "./runstats/hosts"
        )

        scan_hosts_up = (
            safe_integer(
                runstats_hosts_element.attrib.get(
                    "up"
                )
            )

            if runstats_hosts_element is not None

            else None
        )

        scan_hosts_down = (
            safe_integer(
                runstats_hosts_element.attrib.get(
                    "down"
                )
            )

            if runstats_hosts_element is not None

            else None
        )

        scan_hosts_total = (
            safe_integer(
                runstats_hosts_element.attrib.get(
                    "total"
                )
            )

            if runstats_hosts_element is not None

            else None
        )

        if scan_hosts_total is None:
            scan_hosts_total = len(
                current_scan_host_rows
            )

        if scan_hosts_up is None:
            scan_hosts_up = len([
                host_record

                for host_record
                in current_scan_host_rows

                if str(
                    host_record[
                        13
                    ]
                    or
                    ""
                ).lower()
                ==
                "up"
            ])

        if scan_hosts_down is None:
            scan_hosts_down = max(
                scan_hosts_total
                -
                scan_hosts_up,
                0
            )

        provenance_rows.append(
            (
                RCAIP_POST_NAME,
                RCAIP_POST_SLUG,
                RCAIP_RUN_ID,

                scan_id,

                asset_domain,
                scan_scope,
                scan_location,
                target_cidr,

                source_file_name,
                source_file_path,
                source_file_modified_timestamp,
                source_file_size_bytes,
                source_file_sha256,

                nmap_version,
                nmap_xml_output_version,
                scan_arguments,
                target_cidr,

                scan_start_timestamp,
                scan_end_timestamp,
                scan_duration_seconds,
                scan_age_days,
                scan_freshness_status,

                RCAIP_NMAP_SCANNER_NAME,
                RCAIP_NMAP_SCANNER_IP,
                RCAIP_NMAP_SCANNER_TRUST,
                RCAIP_NMAP_SCANNER_TRUST_DISPLAY,
                RCAIP_NMAP_EVIDENCE_CONFIDENCE,
                RCAIP_NMAP_COLLECTION_METHOD,
                RCAIP_NMAP_COLLECTOR,
                RCAIP_NMAP_AUTHORIZATION_REFERENCE,

                scan_hosts_up,
                scan_hosts_down,
                scan_hosts_total,

                "CERTIFIED_MANIFEST_MATCH"
            )
        )

    except Exception as parse_error:
        parse_error_rows.append(
            (
                scan_index,
                source_file_name,
                source_file_path,
                str(
                    parse_error
                )
            )
        )


if parse_error_rows:
    parse_error_text = " | ".join(
        (
            f"manifest_record={scan_index}, "
            f"file={source_file_name}, "
            f"path={source_file_path}, "
            f"error={error_message}"
        )

        for (
            scan_index,
            source_file_name,
            source_file_path,
            error_message
        )
        in parse_error_rows
    )

    raise RuntimeError(
        "One or more certified Nmap XML files failed parsing: "
        +
        parse_error_text
    )


# ---------------------------------------------------------------------------
# 6. Create normalized Nmap source DataFrames
# ---------------------------------------------------------------------------

nmap_scan_provenance_df = spark.createDataFrame(
    provenance_rows,
    schema=nmap_scan_provenance_schema
)

nmap_norm_df = spark.createDataFrame(
    host_rows,
    schema=nmap_host_schema
)

nmap_ports_all_df = spark.createDataFrame(
    port_rows,
    schema=nmap_port_schema
)

nmap_norm_with_provenance_df = enrich_with_scan_provenance(
    nmap_norm_df,
    nmap_scan_provenance_df
)

nmap_ports_all_with_provenance_df = enrich_with_scan_provenance(
    nmap_ports_all_df,
    nmap_scan_provenance_df
)


# Evidence retention guards. No control compliance or device identity is inferred.
IP001_HOST_GRAIN = ["post_slug", "rcaip_run_id", "scan_id", "host_key",
                    "ip_address", "asset_domain", "scan_scope", "scan_location",
                    "scan_source_file"]


def ip001_same_rows(before, after, columns, boundary):
    left, right = before.select(*columns), after.select(*columns)
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise RuntimeError("IP001 evidence membership/multiplicity mismatch: " + boundary)


def ip001_validate_rules(rules):
    rules = rules.dropDuplicates()
    if rules.filter(F.col("port").isNull()).limit(1).count():
        raise RuntimeError("IP001 invalid KB rule key")
    if rules.groupBy("port").count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError("IP001 conflicting KB mappings; no arbitrary rule selection")
    return rules


def ip001_validate_observations(rows):
    for key in IP001_HOST_GRAIN + ["protocol", "port"]:
        if key not in rows.columns:
            raise RuntimeError("IP001 missing observation key: " + key)
        if rows.filter(F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")).limit(1).count():
            raise RuntimeError("IP001 null/blank observation key: " + key)


def ip001_enrich_ports(observations, rules):
    ip001_validate_observations(observations)
    rules = ip001_validate_rules(rules).withColumn("_kb_present", F.lit(True))
    result = observations.join(rules, on="port", how="left")
    result = result.withColumn("port_observation", F.to_json(
        F.struct(*[F.col(c) for c in observations.columns]), {"ignoreNullFields": "false"}))
    result = result.withColumn("port_rule_status", F.when(F.col("_kb_present").isNotNull(),
        F.lit("MAPPED_PORT_NUMBER_ONLY")).otherwise(F.lit("UNMAPPED_RULE")))
    result = result.withColumn("rule_scope_limitation", F.lit(
        "KB key is port number only; protocol/service policy applicability is unestablished"))
    result = result.withColumn("required_port_review", (F.lower(F.col("protocol")) == "tcp") & F.col("port").isin(21, 23, 80))
    result = result.withColumn("required_port_review_reason", F.when(F.col("required_port_review"),
        F.lit("Required TCP 21/23/80 observation: validate applicability and configuration; not a finding")))
    result = result.withColumn("rcso_review_note", F.coalesce(F.col("standard_recommendation"),
        F.lit("Observed open service; rule unavailable. Interpretation and applicability require review.")))
    ip001_same_rows(observations, result, observations.columns, "KB enrichment")
    return result.drop("_kb_present")


def ip001_attach_members(grouped, detail, keys):
    members = detail.groupBy(*keys).agg(
        F.sort_array(F.collect_list("port_observation")).alias("port_members"),
        F.max(F.col("required_port_review").cast("int")).alias("required_port_review"))
    # Null-safe equality is needed for optional grouping labels in port summary.
    condition = F.lit(True)
    for key in keys:
        condition = condition & F.col("g.`" + key + "`").eqNullSafe(F.col("m.`" + key + "`"))
    result = grouped.alias("g").join(members.alias("m"), condition, "left").select(
        "g.*", F.col("m.port_members"), F.col("m.required_port_review"))
    expanded = result.select(F.explode("port_members").alias("port_observation"))
    ip001_same_rows(detail, expanded, ["port_observation"], "grouped members")
    return result


def ip001_mark_action_members(detail, actions):
    selected = actions.select(*IP001_HOST_GRAIN).dropDuplicates().withColumn("selected_for_action", F.lit(True))
    result = detail.join(selected, IP001_HOST_GRAIN, "left").withColumn(
        "selected_for_action", F.coalesce(F.col("selected_for_action"), F.lit(False)))
    ip001_same_rows(detail, result, detail.columns, "action selection annotation")
    ip001_same_rows(result.filter(F.col("selected_for_action")),
        actions.select(F.explode("port_members").alias("port_observation")),
        ["port_observation"], "action selected membership")
    return result


def ip001_printer_ports(detail):
    signal = F.lower(F.concat_ws(" ", "hostname", "port_label", "service", "product", "risk_theme", "csa_relevance"))
    candidates = detail.filter(signal.rlike(printer_signal_pattern) | F.lower(F.col("csa_relevance")).contains("q1143"))
    candidates = candidates.select(*IP001_HOST_GRAIN).dropDuplicates()
    result = detail.join(candidates, IP001_HOST_GRAIN, "inner")
    expected = detail.join(candidates, IP001_HOST_GRAIN, "left_semi")
    ip001_same_rows(expected, result, detail.columns, "same-observation printer context")
    return result.withColumn("printer_context_status", F.lit(
        "CANDIDATE_ONLY: identity, ownership, sample inclusion and Q1143 applicability unestablished"))


# ---------------------------------------------------------------------------
# 8. Load and normalize the shared port Knowledge Base
# ---------------------------------------------------------------------------

if not spark.catalog.tableExists(
    IP001_PORT_KB_TABLE
):
    raise RuntimeError(
        "The shared RCAIP port Knowledge Base is unavailable: "
        +
        IP001_PORT_KB_TABLE
    )

raw_port_kb_df = RCAIP_IP001_BINDING_API.pinned_kb(
    spark, RCAIP_IP001_EXECUTION, IP001_PORT_KB_TABLE
)

port_column_name = resolve_column_name(
    raw_port_kb_df,
    [
        "port",
        "port_number",
        "portid"
    ]
)

if port_column_name is None:
    raise RuntimeError(
        "The RCAIP port Knowledge Base does not contain a recognized "
        "port-number column."
    )

port_kb = (
    raw_port_kb_df

    .select(
        source_column(
            raw_port_kb_df,
            [
                "port",
                "port_number",
                "portid"
            ],
            "int"
        ).alias(
            "port"
        ),

        source_column(
            raw_port_kb_df,
            [
                "port_label",
                "label",
                "service_label"
            ]
        ).alias(
            "port_label"
        ),

        source_column(
            raw_port_kb_df,
            [
                "review_priority",
                "priority"
            ]
        ).alias(
            "review_priority"
        ),

        source_column(
            raw_port_kb_df,
            [
                "risk_theme",
                "risk_category"
            ]
        ).alias(
            "risk_theme"
        ),

        source_column(
            raw_port_kb_df,
            [
                "standard_recommendation",
                "recommendation",
                "recommended_action"
            ]
        ).alias(
            "standard_recommendation"
        ),

        source_column(
            raw_port_kb_df,
            [
                "csa_relevance",
                "csa_question",
                "csa_mapping"
            ]
        ).alias(
            "csa_relevance"
        )
    )

    .transform(ip001_validate_rules)

    .withColumn(
        "port_label",
        F.coalesce(
            F.col(
                "port_label"
            ),
            F.concat(
                F.lit(
                    "Port "
                ),
                F.col(
                    "port"
                ).cast(
                    "string"
                )
            )
        )
    )

    .withColumn(
        "review_priority",
        F.coalesce(
            F.col(
                "review_priority"
            ),
            F.lit(
                "Review"
            )
        )
    )

    .withColumn(
        "risk_theme",
        F.coalesce(
            F.col(
                "risk_theme"
            ),
            F.lit(
                "Service Exposure Review"
            )
        )
    )

    .withColumn(
        "standard_recommendation",
        F.coalesce(
            F.col(
                "standard_recommendation"
            ),
            F.lit(
                "Validate the business requirement and confirm secure "
                "configuration."
            )
        )
    )

    .withColumn(
        "csa_relevance",
        F.coalesce(
            F.col(
                "csa_relevance"
            ),
            F.lit(
                "General port and service review"
            )
        )
    )

    .dropDuplicates()
)


# ---------------------------------------------------------------------------
# 7. Save normalized Nmap source tables
# ---------------------------------------------------------------------------

if RCAIP_IP001_BINDING_API.table_identity(spark, IP001_PORT_KB_TABLE) != RCAIP_IP001_EXECUTION["kb"]["table_id"]:
    raise RuntimeError("IP001 KB identity changed during pinned input validation")
RCAIP_IP001_BINDING_API.bind_xml_inputs(RCAIP_IP001_EXECUTION,
    [row.asDict(recursive=True) for row in nmap_scan_provenance_df.limit(101).collect()])

ip001_validate_observations(nmap_ports_all_df)
ip001_same_rows(nmap_ports_all_df, nmap_ports_all_with_provenance_df,
                nmap_ports_all_df.columns, "normalized provenance")

save_post_table(
    nmap_scan_provenance_df,
    "NMAP_SCAN_PROVENANCE"
)

save_post_table(
    nmap_norm_df,
    "NMAP_NORM"
)

save_post_table(
    nmap_norm_with_provenance_df,
    "NMAP_NORM_WITH_PROVENANCE"
)

save_post_table(
    nmap_ports_all_df,
    "NMAP_PORTS_ALL"
)

save_post_table(
    nmap_ports_all_with_provenance_df,
    "NMAP_PORTS_ALL_WITH_PROVENANCE"
)


# Legacy-compatible runtime aliases
nmap_ports_table = IP001_PHYSICAL_TABLES[
    "NMAP_PORTS_ALL"
]

nmap_assets_table = IP001_PHYSICAL_TABLES[
    "NMAP_NORM"
]

port_kb_table = IP001_PORT_KB_TABLE

nmap_ports = nmap_ports_all_df
nmap_assets = nmap_norm_df


# ---------------------------------------------------------------------------
# 9. Open-port observations and detailed review
# ---------------------------------------------------------------------------

open_ports = (
    nmap_ports

    .filter(
        F.lower(
            F.col(
                "port_state"
            )
        )
        ==
        F.lit(
            "open"
        )
    )

    .filter(
        F.col(
            "port"
        ).isNotNull()
    )
)

open_port_review = ip001_enrich_ports(open_ports, port_kb)

# General review is saved after its exact action selection is annotated.


# ---------------------------------------------------------------------------
# 10. Port summary
# ---------------------------------------------------------------------------

port_summary = (
    open_port_review

    .groupBy(
        "asset_domain",
        "scan_scope",
        "scan_location",
        "protocol",
        "port",
        "port_label",
        "review_priority",
        "risk_theme",
        "csa_relevance"
    )

    .agg(
        F.countDistinct(
            "ip_address"
        ).alias(
            "affected_hosts"
        ),

        F.count(
            "*"
        ).alias(
            "review_port_instances"
        )
    )

    .orderBy(
        "asset_domain",
        "scan_scope",
        "scan_location",
        "protocol",
        "port"
    )
)

port_summary = ip001_attach_members(port_summary, open_port_review, ["asset_domain", "scan_scope", "scan_location", "protocol", "port", "port_label", "review_priority", "risk_theme", "csa_relevance"])

save_post_table(
    port_summary,
    "IP001_PORT_SUMMARY"
)


# ---------------------------------------------------------------------------
# 11. Device-level port rollup
# ---------------------------------------------------------------------------

priority_rank_expression = (
    F.when(
        F.lower(
            F.col(
                "review_priority"
            )
        )
        ==
        F.lit(
            "critical"
        ),
        F.lit(
            4
        )
    )

    .when(
        F.lower(
            F.col(
                "review_priority"
            )
        )
        ==
        F.lit(
            "high"
        ),
        F.lit(
            3
        )
    )

    .when(
        F.lower(
            F.col(
                "review_priority"
            )
        )
        ==
        F.lit(
            "medium"
        ),
        F.lit(
            2
        )
    )

    .when(
        F.lower(
            F.col(
                "review_priority"
            )
        )
        ==
        F.lit(
            "low"
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
)

device_port_rollup = (
    open_port_review

    .withColumn(
        "priority_rank",
        priority_rank_expression
    )

    .groupBy(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "scan_id",

        "asset_domain",
        "scan_scope",
        "scan_location",
        "target_cidr",
        "scan_source_file",

        "host_key",
        "ip_address",
        "hostname"
    )

    .agg(
        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    F.col(
                        "port"
                    ).cast(
                        "string"
                    )
                )
            )
        ).alias(
            "open_review_ports"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "port_label"
                )
            )
        ).alias(
            "port_labels"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "service"
                )
            )
        ).alias(
            "services"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "product"
                )
            )
        ).alias(
            "products"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "version"
                )
            )
        ).alias(
            "versions"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "risk_theme"
                )
            )
        ).alias(
            "risk_themes"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "csa_relevance"
                )
            )
        ).alias(
            "csa_relevance"
        ),

        F.countDistinct(
            "port"
        ).alias(
            "review_port_count"
        ),

        F.max(
            "priority_rank"
        ).alias(
            "max_priority_rank"
        )
    )

    .withColumn(
        "highest_review_priority",

        F.when(
            F.col(
                "max_priority_rank"
            )
            >=
            F.lit(
                4
            ),
            F.lit(
                "Critical"
            )
        )

        .when(
            F.col(
                "max_priority_rank"
            )
            ==
            F.lit(
                3
            ),
            F.lit(
                "High"
            )
        )

        .when(
            F.col(
                "max_priority_rank"
            )
            ==
            F.lit(
                2
            ),
            F.lit(
                "Medium"
            )
        )

        .when(
            F.col(
                "max_priority_rank"
            )
            ==
            F.lit(
                1
            ),
            F.lit(
                "Low"
            )
        )

        .otherwise(
            F.lit(
                "Review"
            )
        )
    )
)

device_port_rollup = ip001_attach_members(device_port_rollup, open_port_review, IP001_HOST_GRAIN + ["post_name", "target_cidr", "hostname"])

save_post_table(
    device_port_rollup,
    "IP001_DEVICE_PORT_ROLLUP"
)


# ---------------------------------------------------------------------------
# 12. Prioritized device work items
# ---------------------------------------------------------------------------

device_port_prioritized = (
    device_port_rollup

    .withColumn(
        "final_priority",

        F.when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "Critical"
            ),
            F.lit(
                "Critical"
            )
        )

        .when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "High"
            ),
            F.lit(
                "High"
            )
        )

        .when(
            (
                F.col(
                    "highest_review_priority"
                )
                ==
                F.lit(
                    "Medium"
                )
            )
            &
            (
                F.col(
                    "review_port_count"
                )
                >=
                F.lit(
                    4
                )
            ),
            F.lit(
                "High"
            )
        )

        .when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "Medium"
            ),
            F.lit(
                "Medium"
            )
        )

        .when(
            (
                F.col(
                    "highest_review_priority"
                )
                ==
                F.lit(
                    "Low"
                )
            )
            &
            (
                F.col(
                    "review_port_count"
                )
                >=
                F.lit(
                    4
                )
            ),
            F.lit(
                "Medium"
            )
        )

        .when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "Low"
            ),
            F.lit(
                "Low"
            )
        )

        .otherwise(
            F.lit(
                "Review"
            )
        )
    )

    .withColumn(
        "priority_reason",

        F.when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "Critical"
            ),
            F.lit(
                "At least one critical-priority service was observed."
            )
        )

        .when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "High"
            ),
            F.lit(
                "At least one high-priority service was observed."
            )
        )

        .when(
            (
                F.col(
                    "highest_review_priority"
                )
                ==
                F.lit(
                    "Medium"
                )
            )
            &
            (
                F.col(
                    "review_port_count"
                )
                >=
                F.lit(
                    4
                )
            ),
            F.lit(
                "Multiple medium-priority services were observed on the "
                "same host."
            )
        )

        .when(
            F.col(
                "highest_review_priority"
            )
            ==
            F.lit(
                "Medium"
            ),
            F.lit(
                "A medium-priority review service was observed."
            )
        )

        .when(
            (
                F.col(
                    "highest_review_priority"
                )
                ==
                F.lit(
                    "Low"
                )
            )
            &
            (
                F.col(
                    "review_port_count"
                )
                >=
                F.lit(
                    4
                )
            ),
            F.lit(
                "Several low-priority services were observed and should "
                "be validated."
            )
        )

        .otherwise(
            F.lit(
                "Review observed services for business need and secure "
                "configuration."
            )
        )
    )

    .withColumn(
        "recommended_action",
        F.concat(
            F.lit(
                "Validate whether the observed services are required and "
                "securely configured. Ports observed: "
            ),
            F.col(
                "open_review_ports"
            ),
            F.lit(
                "."
            )
        )
    )
)

save_post_table(
    device_port_prioritized,
    "IP001_DEVICE_PORT_PRIORITIZED"
)


# ---------------------------------------------------------------------------
# 13. Post-facing action table
# ---------------------------------------------------------------------------

post_action_table = (
    device_port_prioritized

    .filter(
        F.col(
            "final_priority"
        ).isin("Critical", "High", "Medium") | (F.col("required_port_review") == 1)
    )

    .select(
        "post_slug", "rcaip_run_id", "port_members", "required_port_review",
        "scan_id",
        "final_priority",

        "asset_domain",
        "scan_scope",
        "scan_location",

        "host_key",
        "ip_address",
        "hostname",

        "open_review_ports",
        "port_labels",
        "services",
        "products",
        "versions",
        "review_port_count",

        "risk_themes",
        "csa_relevance",
        "priority_reason",
        "recommended_action",

        "target_cidr",
        "scan_source_file"
    )

    .withColumn("required_port_review_reason", F.when(F.col("required_port_review") == 1,
        F.lit("Required TCP port evidence retained for validation; not an automatic finding")))
    .withColumn(
        "post_response",
        F.lit(
            ""
        )
    )

    .withColumn(
        "post_notes",
        F.lit(
            ""
        )
    )
)

open_port_review = ip001_mark_action_members(open_port_review, post_action_table)
save_post_table(
    open_port_review,
    "IP001_OPEN_PORT_REVIEW"
)

save_post_table(
    post_action_table,
    "IP001_POST_ACTION_TABLE"
)


# ---------------------------------------------------------------------------
# 14. Technical appendix
# ---------------------------------------------------------------------------

technical_appendix = (
    open_port_review

    .select(
        "post_name",
        "post_slug",
        "rcaip_run_id",
        "scan_id",

        "asset_domain",
        "scan_scope",
        "scan_location",
        "target_cidr",
        "scan_source_file",

        "host_key",
        "ip_address",
        "hostname",
        "host_status",

        "protocol",
        "port",
        "service",
        "product",
        "version",

        "port_label",
        "review_priority",
        "risk_theme",
        "csa_relevance",
        "standard_recommendation",
        "rcso_review_note"
    )
)

save_post_table(
    technical_appendix,
    "IP001_TECHNICAL_APPENDIX"
)


# ---------------------------------------------------------------------------
# 15. Work-package summary
# ---------------------------------------------------------------------------

nmap_asset_summary = (
    nmap_assets

    .groupBy(
        "asset_domain",
        "scan_scope",
        "scan_location"
    )

    .agg(
        F.countDistinct(
            "ip_address"
        ).alias(
            "hosts_observed"
        ),

        F.sum(
            "open_port_count"
        ).alias(
            "total_open_ports"
        )
    )
)

review_summary = (
    device_port_prioritized

    .groupBy(
        "asset_domain",
        "scan_scope",
        "scan_location"
    )

    .agg(
        F.countDistinct(
            "ip_address"
        ).alias(
            "hosts_with_review_ports"
        ),

        F.sum(
            F.when(
                F.col(
                    "final_priority"
                )
                ==
                F.lit(
                    "Critical"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        ).alias(
            "critical_hosts"
        ),

        F.sum(
            F.when(
                F.col(
                    "final_priority"
                )
                ==
                F.lit(
                    "High"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        ).alias(
            "high_hosts"
        ),

        F.sum(
            F.when(
                F.col(
                    "final_priority"
                )
                ==
                F.lit(
                    "Medium"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        ).alias(
            "medium_hosts"
        ),

        F.sum(
            F.when(
                F.col(
                    "final_priority"
                )
                ==
                F.lit(
                    "Low"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        ).alias(
            "low_hosts"
        ),

        F.sum(
            "review_port_count"
        ).alias(
            "review_port_instances"
        )
    )
)

work_package_summary = (
    nmap_asset_summary

    .join(
        review_summary,
        on=[
            "asset_domain",
            "scan_scope",
            "scan_location"
        ],
        how="left"
    )

    .fillna(
        0
    )
)

save_post_table(
    work_package_summary,
    "IP001_WORK_PACKAGE_SUMMARY"
)


# ---------------------------------------------------------------------------
# 16. Port-by-port communication summary
# ---------------------------------------------------------------------------

port_email_summary = (
    open_port_review

    .groupBy(
        "asset_domain",
        "scan_scope",
        "scan_location",
        "protocol",
        "port",
        "port_label",
        "review_priority",
        "risk_theme",
        "standard_recommendation",
        "csa_relevance"
    )

    .agg(
        F.countDistinct(
            "ip_address"
        ).alias(
            "affected_hosts"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "ip_address"
                )
            )
        ).alias(
            "affected_ip_addresses"
        )
    )

    .withColumn(
        "email_sentence",
        F.concat(
            F.lit(
                "Port "
            ),
            F.col(
                "port"
            ).cast(
                "string"
            ),
            F.lit(
                " "
            ),
            F.upper(
                F.coalesce(
                    F.col(
                        "protocol"
                    ),
                    F.lit(
                        "unknown"
                    )
                )
            ),
            F.lit(
                " ("
            ),
            F.col(
                "port_label"
            ),
            F.lit(
                ") was observed on "
            ),
            F.col(
                "affected_hosts"
            ).cast(
                "string"
            ),
            F.lit(
                " host(s) in "
            ),
            F.col(
                "asset_domain"
            ),
            F.lit(
                " / "
            ),
            F.col(
                "scan_scope"
            ),
            F.lit(
                " / "
            ),
            F.col(
                "scan_location"
            ),
            F.lit(
                ". "
            ),
            F.col(
                "standard_recommendation"
            )
        )
    )
)

save_post_table(
    port_email_summary,
    "IP001_PORT_EMAIL_SUMMARY"
)


# ---------------------------------------------------------------------------
# 17. CSA Q1143 printer and MFD evidence
# ---------------------------------------------------------------------------

printer_signal_pattern = (
    "prnt|print|printer|jetdirect|ipp|lpd|xerox|ricoh|canon|"
    "lexmark|laserjet|officejet|mfp|mfd"
)

q1143_printer_mfd_ports = ip001_printer_ports(open_port_review)

save_post_table(
    q1143_printer_mfd_ports,
    "IP001_Q1143_PRINTER_MFD_PORTS"
)


q1143_printer_mfd_device_review = device_port_prioritized.join(
    q1143_printer_mfd_ports.select(*IP001_HOST_GRAIN).dropDuplicates(),
    IP001_HOST_GRAIN, "inner"
).withColumn("printer_context_status", F.lit(
    "CANDIDATE_ONLY: identity, ownership, sample inclusion and Q1143 applicability unestablished"
)).withColumn("post_response", F.lit("")).withColumn("post_notes", F.lit(""))
ip001_same_rows(q1143_printer_mfd_ports,
    q1143_printer_mfd_device_review.select(F.explode("port_members").alias("port_observation")),
    ["port_observation"], "printer detail/device membership")

save_post_table(
    q1143_printer_mfd_device_review,
    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW"
)


# ---------------------------------------------------------------------------
# 18. CSA Q1173 Enterprise Wi-Fi evidence
# ---------------------------------------------------------------------------

q1173_entwifi_ports = (
    open_port_review

    .filter(
        F.lower(
            F.col(
                "scan_scope"
            )
        ).contains(
            "entwifi"
        )
    )

    .select(
        "scan_id",
        "post_name",

        "asset_domain",
        "scan_scope",
        "scan_location",

        "host_key",
        "ip_address",
        "hostname",

        "protocol",
        "port",
        "service",
        "product",
        "version",

        "port_label",
        "review_priority",
        "risk_theme",
        "standard_recommendation",

        "target_cidr",
        "scan_source_file"
    )
)

save_post_table(
    q1173_entwifi_ports,
    "IP001_Q1173_ENTWIFI_PORTS"
)


# ---------------------------------------------------------------------------
# 19. NEN public-exposure evidence
# ---------------------------------------------------------------------------

nen_public_exposure = (
    open_ports

    .filter(
        F.upper(
            F.col(
                "asset_domain"
            )
        )
        ==
        F.lit(
            "NEN"
        )
    )

    .select(
        "scan_id",
        "post_name",

        "asset_domain",
        "scan_scope",
        "scan_location",
        "target_cidr",
        "scan_source_file",

        "host_key",
        "ip_address",
        "hostname",
        "host_status",

        "protocol",
        "port",
        "service",
        "product",
        "version"
    )
)

save_post_table(
    nen_public_exposure,
    "IP001_NEN_PUBLIC_EXPOSURE"
)


# ---------------------------------------------------------------------------
# 20. Provenance-enriched IP-001 products
# ---------------------------------------------------------------------------

provenance_enrichment_contract = {
    "IP001_OPEN_PORT_REVIEW_WITH_PROVENANCE":
        open_port_review,

    "IP001_DEVICE_PORT_ROLLUP_WITH_PROVENANCE":
        device_port_rollup,

    "IP001_DEVICE_PORT_PRIORITIZED_WITH_PROVENANCE":
        device_port_prioritized,

    "IP001_POST_ACTION_TABLE_WITH_PROVENANCE":
        post_action_table,

    "IP001_Q1143_PRINTER_MFD_PORTS_WITH_PROVENANCE":
        q1143_printer_mfd_ports,

    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW_WITH_PROVENANCE":
        q1143_printer_mfd_device_review,

    "IP001_Q1173_ENTWIFI_PORTS_WITH_PROVENANCE":
        q1173_entwifi_ports,

    "IP001_NEN_PUBLIC_EXPOSURE_WITH_PROVENANCE":
        nen_public_exposure,

    "IP001_TECHNICAL_APPENDIX_WITH_PROVENANCE":
        technical_appendix
}

provenance_unmatched_rows = 0

for (
    provenance_logical_name,
    source_dataframe
) in provenance_enrichment_contract.items():

    enriched_dataframe = enrich_with_scan_provenance(
        source_dataframe,
        nmap_scan_provenance_df
    )

    ip001_same_rows(source_dataframe, enriched_dataframe, source_dataframe.columns,
                    "provenance enrichment")
    unmatched_rows = (
        enriched_dataframe

        .filter(
            F.col(
                "source_file_sha256"
            ).isNull()
        )

        .count()
    )

    provenance_unmatched_rows += unmatched_rows

    save_post_table(
        enriched_dataframe,
        provenance_logical_name
    )


# ---------------------------------------------------------------------------
# 21. Scan-provenance work-package summary
# ---------------------------------------------------------------------------

hosts_by_scan = (
    nmap_norm_df

    .groupBy(
        "scan_id"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "normalized_host_rows"
        ),

        F.countDistinct(
            "ip_address"
        ).alias(
            "distinct_ip_addresses"
        ),

        F.sum(
            "open_port_count"
        ).alias(
            "normalized_open_port_count"
        )
    )
)

ports_by_scan = (
    nmap_ports_all_df

    .groupBy(
        "scan_id"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "port_observation_rows"
        ),

        F.sum(
            F.when(
                F.lower(
                    F.col(
                        "port_state"
                    )
                )
                ==
                F.lit(
                    "open"
                ),
                F.lit(
                    1
                )
            ).otherwise(
                F.lit(
                    0
                )
            )
        ).alias(
            "open_port_observation_rows"
        )
    )
)

review_by_scan = (
    open_port_review

    .groupBy(
        "scan_id"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "open_port_review_rows"
        ),

        F.countDistinct(
            "ip_address"
        ).alias(
            "hosts_with_review_ports"
        )
    )
)

actions_by_scan = (
    post_action_table

    .groupBy(
        "scan_id"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "post_action_rows"
        )
    )
)

ip001_scan_provenance_summary_df = (
    nmap_scan_provenance_df

    .join(
        hosts_by_scan,
        on="scan_id",
        how="left"
    )

    .join(
        ports_by_scan,
        on="scan_id",
        how="left"
    )

    .join(
        review_by_scan,
        on="scan_id",
        how="left"
    )

    .join(
        actions_by_scan,
        on="scan_id",
        how="left"
    )

    .fillna(
        0,
        subset=[
            "normalized_host_rows",
            "distinct_ip_addresses",
            "normalized_open_port_count",
            "port_observation_rows",
            "open_port_observation_rows",
            "open_port_review_rows",
            "hosts_with_review_ports",
            "post_action_rows"
        ]
    )
)

save_post_table(
    ip001_scan_provenance_summary_df,
    "IP001_SCAN_PROVENANCE_SUMMARY"
)


# ---------------------------------------------------------------------------
# 22. Final certification
# ---------------------------------------------------------------------------

configured_scan_count = len(
    RCAIP_NMAP_SCANS
)

parsed_scan_count = nmap_scan_provenance_df.count()

normalized_host_count = nmap_norm_df.count()

port_observation_count = nmap_ports_all_df.count()

open_port_observation_count = open_ports.count()

open_port_review_count = open_port_review.count()

device_review_count = device_port_prioritized.count()

post_action_count = post_action_table.count()

distinct_scan_id_count = (
    nmap_scan_provenance_df

    .select(
        "scan_id"
    )

    .distinct()

    .count()
)

blank_hash_count = (
    nmap_scan_provenance_df

    .filter(
        F.col(
            "source_file_sha256"
        ).isNull()
        |
        (
            F.trim(
                F.col(
                    "source_file_sha256"
                )
            )
            ==
            F.lit(
                ""
            )
        )
    )

    .count()
)

duplicate_source_file_rows = (
    nmap_scan_provenance_df

    .groupBy(
        "source_file_path"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        >
        F.lit(
            1
        )
    )

    .count()
)

incorrect_runtime_post_rows = (
    nmap_norm_df

    .filter(
        F.col(
            "post_slug"
        )
        !=
        F.lit(
            RCAIP_POST_SLUG
        )
    )

    .count()
    +
    nmap_ports_all_df

    .filter(
        F.col(
            "post_slug"
        )
        !=
        F.lit(
            RCAIP_POST_SLUG
        )
    )

    .count()
)

expected_written_table_count = len(
    IP001_ALL_POST_LOGICAL_TABLES
)

unique_written_table_count = len(
    set(
        IP001_WRITTEN_TABLES
    )
)

missing_durable_tables = [
    physical_table_name

    for physical_table_name
    in IP001_PHYSICAL_TABLES.values()

    if not spark.catalog.tableExists(
        physical_table_name
    )
]

cross_post_output_leakage = [
    physical_table_name

    for physical_table_name
    in IP001_PHYSICAL_TABLES.values()

    if not physical_table_name.lower().startswith(
        expected_post_table_prefix.lower()
    )
]

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
        "Certified Nmap manifest",
        "At least one record",
        str(
            configured_scan_count
        ),
        configured_scan_count > 0
    ),

    (
        "Configured scans parsed",
        str(
            configured_scan_count
        ),
        str(
            parsed_scan_count
        ),
        parsed_scan_count
        ==
        configured_scan_count
    ),

    (
        "Distinct scan identifiers",
        str(
            configured_scan_count
        ),
        str(
            distinct_scan_id_count
        ),
        distinct_scan_id_count
        ==
        configured_scan_count
    ),

    (
        "Duplicate source-file records",
        "0",
        str(
            duplicate_source_file_rows
        ),
        duplicate_source_file_rows
        ==
        0
    ),

    (
        "Blank source-file hashes",
        "0",
        str(
            blank_hash_count
        ),
        blank_hash_count
        ==
        0
    ),

    (
        "Normalized Nmap hosts",
        "At least one",
        str(
            normalized_host_count
        ),
        normalized_host_count > 0
    ),

    (
        "Runtime Post contamination",
        "0",
        str(
            incorrect_runtime_post_rows
        ),
        incorrect_runtime_post_rows
        ==
        0
    ),

    (
        "Provenance join failures",
        "0",
        str(
            provenance_unmatched_rows
        ),
        provenance_unmatched_rows
        ==
        0
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
        "Missing durable tables",
        "0",
        str(
            len(
                missing_durable_tables
            )
        ),
        len(
            missing_durable_tables
        )
        ==
        0
    ),

    (
        "Cross-Post output leakage",
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

ip001_certificate_df = spark.createDataFrame(
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

RCAIP_IP001_CERTIFIED = (
    not failed_certification_checks
)

RCAIP_IP001_READY = (
    RCAIP_IP001_CERTIFIED
)


# ---------------------------------------------------------------------------
# 23. Final runtime output
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP IP-001 - FINAL RUNTIME CONTRACT")
print("=" * 100)

print(
    "Status:",
    (
        "PASS"
        if RCAIP_IP001_CERTIFIED
        else
        "FAIL"
    )
)

print(
    "Ready:",
    RCAIP_IP001_READY
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
    "Input directory:",
    RCAIP_INPUT_BASE_DIR
)

print(
    "Nmap scans configured:",
    configured_scan_count
)

print(
    "Nmap scans parsed:",
    parsed_scan_count
)

print(
    "Normalized Nmap hosts:",
    normalized_host_count
)

print(
    "Nmap port observations:",
    port_observation_count
)

print(
    "Open-port observations:",
    open_port_observation_count
)

print(
    "Open-port review records:",
    open_port_review_count
)

print(
    "Prioritized device records:",
    device_review_count
)

print(
    "Post action-review rows:",
    post_action_count
)

print(
    "Durable tables written:",
    unique_written_table_count
)

print(
    "Missing durable tables:",
    len(
        missing_durable_tables
    )
)

print(
    "Provenance join failures:",
    provenance_unmatched_rows
)

print(
    "Cross-Post output leakage:",
    len(
        cross_post_output_leakage
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
    ip001_certificate_df
    .orderBy(
        F.col(
            "check_passed"
        ).asc(),
        "certification_check"
    )
)

display(
    ip001_scan_provenance_summary_df
)

display(
    work_package_summary
)

display(
    port_summary
)

display(
    post_action_table.limit(
        100
    )
)


if failed_certification_checks:
    raise RuntimeError(
        "IP-001 certification failed: "
        +
        " | ".join(
            failed_check[
                0
            ]

            for failed_check
            in failed_certification_checks
        )
    )
RCAIP_IP001_EXECUTION["state"] = "CERTIFIED"
RCAIP_PORT_REPORT_BINDINGS = RCAIP_IP001_BINDING_API.port_report_bindings(RCAIP_IP001_EXECUTION)
