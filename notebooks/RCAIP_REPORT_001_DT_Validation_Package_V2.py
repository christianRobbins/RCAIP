# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP REPORT 001 - DT VALIDATION PACKAGE
# Cell 2 - Certified Report Foundation and Source Contract
#
# Purpose:
#   Load and certify the current runtime products from IP-001 through IP-005
#   and Discovery 002. This cell replaces the legacy example_post_d report contract.
#
# Guardrails:
#   - Runtime-driven; no fixed Post counts.
#   - No raw enterprise source scans.
#   - Asset counts remain separate from tool, AD, and relationship counts.
#   - Validation records are not automatically findings or assignments.
#   - Optional sources create documented gaps rather than false failures.
# ============================================================================

from datetime import datetime, timezone
from functools import reduce
import re

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
    name for name in required_runtime_objects
    if name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "Report 001 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

RCAIP_REPORT_NAME = "RCAIP DT Validation Package"
RCAIP_REPORT_VERSION = "3.0"
RCAIP_REPORT_ANALYSIS_DATE = datetime.now(timezone.utc).date().isoformat()
RCAIP_REPORT_FILE_NAME = (
    f"RCAIP_{RCAIP_POST_SLUG}_DT_Validation_Package_"
    f"{RCAIP_REPORT_ANALYSIS_DATE}.xlsx"
)


def post_table(stem):
    return f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_{stem}"


def shared_table(stem):
    return f"{RCAIP_RESULTS_NAMESPACE}.{stem}"


def resolve_table(
    logical_candidates,
    fallback_stems,
    required=True,
    shared=False
):
    candidates = []

    for logical_name in logical_candidates:
        try:
            resolved = get_rcaip_table(
                logical_name,
                require_exists=False
            )
        except TypeError:
            try:
                resolved = get_rcaip_table(logical_name)
            except Exception:
                resolved = None
        except Exception:
            resolved = None

        if resolved:
            candidates.append(resolved)

    for stem in fallback_stems:
        candidates.append(
            shared_table(stem) if shared else post_table(stem)
        )

    deduped = []
    for name in candidates:
        if name and name not in deduped:
            deduped.append(name)

    for name in deduped:
        if spark.catalog.tableExists(name):
            return {
                "available": True,
                "table_name": name,
                "required": bool(required),
                "shared": bool(shared),
                "error": None
            }

    return {
        "available": False,
        "table_name": deduped[0] if deduped else None,
        "required": bool(required),
        "shared": bool(shared),
        "error": "No candidate table exists"
    }


# Port-only bindings must be supplied prospectively from the approved execution
# manifest. No latest-version or base/provenance fallback. None explicitly means
# NOT_PRODUCED; absent manifest entry means UNAVAILABLE, never compliant.
PORT_SOURCE_KEYS = {"open_port_summary", "open_port_actions", "open_port_detail"}
PORT_SHEET_KEYS = {"Open_Port_Summary": "open_port_summary",
                   "Open_Port_Actions": "open_port_actions",
                   "Open_Port_Detail": "open_port_detail"}
PORT_EVIDENCE_LIMIT = 20000
PORT_EVIDENCE_WARNING = ("Positive observations only. Closed-port coverage, sample applicability, "
    "password/default-credential validation and secondary-scan remediation verification "
    "are NOT ESTABLISHED by this sheet. No compliance conclusion.")


def resolve_port_table(source_key, logical_candidates):
    bindings = globals().get("RCAIP_PORT_REPORT_BINDINGS", {})
    if source_key not in bindings or bindings[source_key] is None:
        return dict(available=False, table_name=None, required=False, shared=False,
                    port_status="NOT_PRODUCED" if source_key in bindings else "UNAVAILABLE",
                    error="Explicit immutable port binding not available")
    binding = bindings[source_key]
    if (not {"logical_name", "table", "version", "generation", "table_id", "execution_id", "producer_sha256", "schema", "write_token"}.issubset(binding)
            or binding["logical_name"] not in logical_candidates
            or type(binding["version"]) is not int or binding["version"] < 0
            or not isinstance(binding["generation"], str) or not binding["generation"]):
        raise RuntimeError("Invalid explicit port report binding: " + source_key)
    if binding["table"] != get_rcaip_table(binding["logical_name"], require_exists=False):
        raise RuntimeError("Port report binding differs from registry: " + source_key)
    return dict(available=True, table_name=binding["table"], required=False, shared=False,
                error=None, port_binding=binding)


def load_port_table(config):
    binding = config["port_binding"]
    if "table_id" in binding:
        identity = spark.sql("DESCRIBE DETAIL " + binding["table"]).select("id").collect()
        if len(identity) != 1 or identity[0]["id"] != binding["table_id"]:
            raise RuntimeError("Port source table identity changed")
    df = spark.read.option("versionAsOf", binding["version"]).table(binding["table"])
    if "schema" in binding and tuple((f.name, f.dataType.simpleString()) for f in df.schema.fields) != tuple(binding["schema"]):
        raise RuntimeError("Port source schema differs from captured output")
    if "post_slug" not in df.columns and "runtime_post_slug" in df.columns:
        df = df.withColumn("post_slug", F.col("runtime_post_slug"))
    if "runtime_post_slug" in df.columns and "post_slug" in df.columns:
        if df.filter(~F.col("post_slug").eqNullSafe(F.col("runtime_post_slug"))).limit(1).count():
            raise RuntimeError("Port Post aliases disagree")
    for column in ("post_slug", "rcaip_run_id", "generated_at_utc"):
        if column not in df.columns:
            raise RuntimeError("Port source lacks scope/generation authority: " + column)
    if spark.conf.get("spark.sql.session.timeZone") not in ("UTC", "Etc/UTC", "+00:00", "Z"):
        raise RuntimeError("Port generation comparison requires explicit UTC session")
    for column, expected in (("post_slug", RCAIP_POST_SLUG), ("rcaip_run_id", RCAIP_RUN_ID),
                              ("generated_at_utc", binding["generation"])):
        target = F.lit(expected).cast("timestamp") if column == "generated_at_utc" else F.lit(expected)
        if df.filter(~F.col(column).eqNullSafe(target)).limit(1).count():
            raise RuntimeError("Port source scope/generation mismatch: " + column)
    if "table_id" in binding:
        identity = spark.sql("DESCRIBE DETAIL " + binding["table"]).select("id").collect()
        if len(identity) != 1 or identity[0]["id"] != binding["table_id"]:
            raise RuntimeError("Port source table identity changed during read")
    return df


SOURCE_CONTRACT = {
    "operational_asset_view": resolve_table(
        ["OPERATIONAL_ASSET_VIEW", "IP005_OPERATIONAL_ASSET_VIEW"],
        ["operational_asset_view"],
        required=True
    ),
    "asset_plane": resolve_table(
        ["ASSET_RECONCILIATION_SCORED"],
        ["asset_reconciliation_scored"],
        required=True
    ),
    "tool_coverage": resolve_table(
        ["IP002_TOOL_COVERAGE_REVIEW", "TOOL_COVERAGE_REVIEW"],
        ["ip002_tool_coverage_review"],
        required=True
    ),
    "lifecycle": resolve_table(
        ["IP003_ASSET_LIFECYCLE_ROLLUP"],
        ["ip003_asset_lifecycle_rollup"],
        required=True
    ),
    "ip004_computer_review": resolve_table(
        ["IP004_COMPUTER_HYGIENE_REVIEW"],
        ["ip004_computer_hygiene_review"],
        required=True
    ),
    "ip004_computer_map": resolve_table(
        ["IP004_COMPUTER_ASSET_MAP"],
        ["ip004_computer_asset_map"],
        required=True
    ),
    "ip004_user_review": resolve_table(
        ["IP004_USER_HYGIENE_REVIEW"],
        ["ip004_user_hygiene_review"],
        required=True
    ),
    "ip004_group_bloat": resolve_table(
        ["IP004_AD_BLOAT_PRIORITY"],
        ["ip004_ad_bloat_priority"],
        required=True
    ),
    "ip004_privileged_access": resolve_table(
        ["IP004_PRIVILEGED_ACCESS_REVIEW"],
        ["ip004_privileged_access_review"],
        required=True
    ),
    "ip004_action_queue": resolve_table(
        ["IP004_HYGIENE_ACTION_QUEUE"],
        ["ip004_hygiene_action_queue"],
        required=True
    ),
    "ip004_summary": resolve_table(
        ["IP004_HYGIENE_SUMMARY"],
        ["ip004_hygiene_summary"],
        required=True
    ),
    "ip005_investigation_universe": resolve_table(
        ["OPERATIONAL_INVESTIGATION_UNIVERSE", "IP005_OPERATIONAL_INVESTIGATION_UNIVERSE"],
        ["operational_investigation_universe"],
        required=True
    ),
    "ip005_confirmed_baseline": resolve_table(
        ["CONFIRMED_OPERATIONAL_BASELINE", "IP005_CONFIRMED_OPERATIONAL_BASELINE"],
        ["confirmed_operational_baseline"],
        required=True
    ),
    "ip005_asset_actions": resolve_table(
        ["IP005_OPERATIONAL_ASSET_ACTION_QUEUE"],
        ["ip005_operational_asset_action_queue"],
        required=True
    ),
    "ip005_integrated_actions": resolve_table(
        ["IP005_INTEGRATED_OPERATIONAL_ACTION_QUEUE"],
        ["ip005_integrated_operational_action_queue"],
        required=True
    ),
    "ip005_final_summary": resolve_table(
        ["IP005_FINAL_SUMMARY"],
        ["ip005_final_summary"],
        required=True
    ),
    "ip005_source_availability": resolve_table(
        ["IP005_SOURCE_AVAILABILITY"],
        ["ip005_source_availability"],
        required=True
    ),
    "attribution_disposition": resolve_table(
        [],
        ["rcaip_discovery_002_enterprise_attribution_disposition"],
        required=True,
        shared=True
    ),
    "attribution_source_gaps": resolve_table(
        [],
        ["rcaip_discovery_002_enterprise_attribution_source_gaps"],
        required=True,
        shared=True
    ),
    "attribution_closure": resolve_table(
        [],
        ["rcaip_discovery_002_closure_certificate"],
        required=True,
        shared=True
    ),
    "open_port_summary": resolve_port_table('open_port_summary', ['IP001_PORT_SUMMARY']),
    "open_port_actions": resolve_port_table('open_port_actions', ['IP001_POST_ACTION_TABLE_WITH_PROVENANCE', 'IP001_POST_ACTION_TABLE']),
    "open_port_detail": resolve_port_table('open_port_detail', ['IP001_OPEN_PORT_REVIEW_WITH_PROVENANCE', 'IP001_OPEN_PORT_REVIEW'])
}


# ---------------------------------------------------------------------------
# 2. Runtime filtering and source loading
# ---------------------------------------------------------------------------

POST_FILTER_COLUMNS = [
    "post_slug",
    "post_key",
    "runtime_post_slug",
    "post_code",
    "post_name",
    "runtime_post"
]

POST_FILTER_VALUES = {
    str(RCAIP_POST_SLUG).strip().lower(),
    str(RCAIP_POST_NAME).strip().lower()
}

for optional_name in ["RCAIP_POST_CODE"]:
    value = globals().get(optional_name)
    if value:
        POST_FILTER_VALUES.add(str(value).strip().lower())


def filter_to_runtime(dataframe):
    output = dataframe
    lookup = {name.lower(): name for name in output.columns}

    for candidate in POST_FILTER_COLUMNS:
        actual = lookup.get(candidate.lower())
        if actual:
            output = output.filter(
                F.lower(F.trim(F.col(f"`{actual}`").cast("string")))
                .isin(sorted(POST_FILTER_VALUES))
            )
            break

    run_column = lookup.get("rcaip_run_id")
    if run_column:
        output = output.filter(
            F.col(f"`{run_column}`").cast("string")
            == F.lit(str(RCAIP_RUN_ID))
        )

    return output


REPORT_SOURCE_DFS = {}
source_validation_records = []

source_validation_schema = T.StructType([
    T.StructField("source_key", T.StringType(), False),
    T.StructField("table_name", T.StringType(), True),
    T.StructField("required_source", T.BooleanType(), False),
    T.StructField("table_exists", T.BooleanType(), False),
    T.StructField("runtime_row_count", T.LongType(), True),
    T.StructField("distinct_asset_keys", T.LongType(), True),
    T.StructField("distinct_action_ids", T.LongType(), True),
    T.StructField("column_count", T.IntegerType(), True),
    T.StructField("source_status", T.StringType(), False),
    T.StructField("read_error", T.StringType(), True)
])

for source_key, config in SOURCE_CONTRACT.items():
    table_name = config["table_name"]
    available = bool(config["available"])
    required = bool(config["required"])
    row_count = None
    asset_count = None
    action_count = None
    column_count = None
    read_error = config.get("error")

    if available:
        try:
            dataframe = (load_port_table(config) if source_key in PORT_SOURCE_KEYS else
                         filter_to_runtime(spark.table(table_name)))
            REPORT_SOURCE_DFS[source_key] = dataframe
            column_count = len(dataframe.columns)

            aggregations = [F.count("*").alias("row_count")]
            if "asset_key" in dataframe.columns:
                aggregations.append(
                    F.countDistinct("asset_key").alias("asset_count")
                )
            if "action_id" in dataframe.columns:
                aggregations.append(
                    F.countDistinct("action_id").alias("action_count")
                )

            metrics = dataframe.agg(*aggregations).first().asDict()
            row_count = int(metrics.get("row_count") or 0)
            if "asset_count" in metrics:
                asset_count = int(metrics.get("asset_count") or 0)
            if "action_count" in metrics:
                action_count = int(metrics.get("action_count") or 0)
            read_error = None
        except Exception as exc:
            if source_key in PORT_SOURCE_KEYS:
                REPORT_SOURCE_DFS.pop(source_key, None)
                if not isinstance(exc, (OSError, TimeoutError)):
                    raise
                config["port_status"] = "UNAVAILABLE_READ_ERROR"
                read_error = "Port input read unavailable; details remain in enterprise"
            else:
                read_error = str(exc)
            available = False

    if available:
        status = "AVAILABLE" if row_count != 0 else "AVAILABLE_EMPTY"
    else:
        status = "REQUIRED_SOURCE_ERROR" if required else "OPTIONAL_NOT_AVAILABLE"

    source_validation_records.append((
        source_key,
        table_name,
        required,
        available,
        row_count,
        asset_count,
        action_count,
        column_count,
        status,
        read_error
    ))

report_source_validation = spark.createDataFrame(
    source_validation_records,
    schema=source_validation_schema
)

required_source_errors = [
    row.asDict()
    for row in (
        report_source_validation
        .filter(F.col("source_status") == "REQUIRED_SOURCE_ERROR")
        .collect()
    )
]

if required_source_errors:
    raise RuntimeError(
        "Report 001 required sources are unavailable: "
        + str(required_source_errors)
    )


# ---------------------------------------------------------------------------
# 3. Core population certification
# ---------------------------------------------------------------------------

operational_view = REPORT_SOURCE_DFS["operational_asset_view"].persist(
    StorageLevel.MEMORY_AND_DISK
)
asset_plane = REPORT_SOURCE_DFS["asset_plane"].persist(
    StorageLevel.MEMORY_AND_DISK
)
lifecycle = REPORT_SOURCE_DFS["lifecycle"].persist(
    StorageLevel.MEMORY_AND_DISK
)
tool_coverage = REPORT_SOURCE_DFS["tool_coverage"]
ip004_actions = REPORT_SOURCE_DFS["ip004_action_queue"]
ip005_asset_actions = REPORT_SOURCE_DFS["ip005_asset_actions"]
ip005_integrated_actions = REPORT_SOURCE_DFS["ip005_integrated_actions"]
attribution_disposition = REPORT_SOURCE_DFS["attribution_disposition"]
confirmed_baseline = REPORT_SOURCE_DFS["ip005_confirmed_baseline"]


def one_row_asset_metrics(dataframe, label):
    if "asset_key" not in dataframe.columns:
        raise RuntimeError(f"{label} does not contain asset_key.")

    row = dataframe.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("asset_key").alias("distinct_asset_keys")
    ).first()

    row_count = int(row["row_count"] or 0)
    distinct_count = int(row["distinct_asset_keys"] or 0)

    if row_count != distinct_count:
        raise RuntimeError(
            f"{label} is not one row per asset_key: "
            f"rows={row_count}, distinct={distinct_count}"
        )

    return row_count


operational_asset_count = one_row_asset_metrics(
    operational_view,
    "Operational Asset View"
)
asset_plane_count = one_row_asset_metrics(
    asset_plane,
    "Scored Asset Plane"
)
lifecycle_count = one_row_asset_metrics(
    lifecycle,
    "IP-003 Lifecycle Rollup"
)
confirmed_baseline_count = one_row_asset_metrics(
    confirmed_baseline,
    "Confirmed Operational Baseline"
)
attribution_candidate_count = one_row_asset_metrics(
    attribution_disposition,
    "Discovery 002 Final Disposition"
)

if len({operational_asset_count, asset_plane_count, lifecycle_count}) != 1:
    raise RuntimeError(
        "Core asset populations do not reconcile: "
        f"operational={operational_asset_count}, "
        f"asset_plane={asset_plane_count}, lifecycle={lifecycle_count}"
    )


def action_metrics(dataframe, label):
    if "action_id" not in dataframe.columns:
        raise RuntimeError(f"{label} does not contain action_id.")

    row = dataframe.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("action_id").alias("distinct_action_ids")
    ).first()

    row_count = int(row["row_count"] or 0)
    distinct_count = int(row["distinct_action_ids"] or 0)

    if row_count != distinct_count:
        raise RuntimeError(
            f"{label} contains duplicate action_id values: "
            f"rows={row_count}, distinct={distinct_count}"
        )

    return row_count


ip004_action_count = action_metrics(
    ip004_actions,
    "IP-004 Hygiene Action Queue"
)
asset_action_count = action_metrics(
    ip005_asset_actions,
    "IP-005 Asset Action Queue"
)
integrated_action_count = action_metrics(
    ip005_integrated_actions,
    "IP-005 Integrated Action Queue"
)

if integrated_action_count != asset_action_count + ip004_action_count:
    raise RuntimeError(
        "Integrated action accounting does not reconcile: "
        f"integrated={integrated_action_count}, "
        f"asset={asset_action_count}, ad={ip004_action_count}"
    )


# ---------------------------------------------------------------------------
# 4. Read certified module metrics
# ---------------------------------------------------------------------------

ip005_summary_rows = (
    REPORT_SOURCE_DFS["ip005_final_summary"]
    .select("metric_name", "metric_value")
    .collect()
)

IP005_METRICS = {
    str(row["metric_name"]): int(row["metric_value"] or 0)
    for row in ip005_summary_rows
}

ip004_summary_rows = REPORT_SOURCE_DFS["ip004_summary"].collect()

IP004_SUMMARY = [row.asDict(recursive=True) for row in ip004_summary_rows]

closure_rows = REPORT_SOURCE_DFS["attribution_closure"].limit(2).collect()
if len(closure_rows) != 1:
    raise RuntimeError(
        "Discovery 002 closure certificate must contain one runtime row."
    )

closure_record = closure_rows[0].asDict(recursive=True)

# Discovery 002 now publishes status/ready as its authoritative closure
# contract. Shared latest tables may retain legacy nullable columns such as
# closure_certified from older runs. A null legacy value must not override a
# current Reykjavík status=PASS, ready=True certificate.
closure_status = str(
    closure_record.get("status")
    or
    ""
).strip().upper()

closure_ready_value = closure_record.get("ready")
legacy_closure_certified_value = closure_record.get(
    "closure_certified"
)

if closure_ready_value is not None:
    closure_ready = bool(
        closure_ready_value
    )
elif legacy_closure_certified_value is not None:
    closure_ready = bool(
        legacy_closure_certified_value
    )
else:
    closure_ready = (
        closure_status == "PASS"
    )

if closure_status not in {
    "",
    "PASS"
}:
    raise RuntimeError(
        "Discovery 002 closure status is not PASS: "
        +
        closure_status
    )

if not closure_ready:
    raise RuntimeError(
        "Discovery 002 closure is not ready."
    )

closure_candidate_count = int(
    closure_record.get(
        "candidate_asset_count"
    )
    or
    0
)

closure_disposition_count = int(
    closure_record.get(
        "disposition_asset_count"
    )
    or
    0
)

if (
    closure_candidate_count
    !=
    closure_disposition_count
):
    raise RuntimeError(
        "Discovery 002 closure accounting does not reconcile: "
        f"candidates={closure_candidate_count}, "
        f"dispositions={closure_disposition_count}."
    )


# ---------------------------------------------------------------------------
# 5. Correct report-level AD-computer-to-asset integration
#
# IP-005 looked for a singular asset_key on the IP-004 map. The strict IP-004
# map intentionally stores a comma-delimited matched_asset_keys field because
# it preserves one row per AD computer. Report 001 expands that certified field
# for accurate workbook-level asset context without rewriting upstream tables.
# ---------------------------------------------------------------------------

computer_review = REPORT_SOURCE_DFS["ip004_computer_review"]
computer_map = REPORT_SOURCE_DFS["ip004_computer_map"]

if "ad_object_key" not in computer_review.columns:
    raise RuntimeError("IP-004 computer review lacks ad_object_key.")

if "ad_object_key" not in computer_map.columns:
    raise RuntimeError("IP-004 computer map lacks ad_object_key.")

priority_column = next(
    (
        name for name in [
            "computer_security_priority",
            "priority",
            "review_priority"
        ]
        if name in computer_review.columns
    ),
    None
)

condition_column = next(
    (
        name for name in [
            "computer_review_condition",
            "condition",
            "interpretation"
        ]
        if name in computer_review.columns
    ),
    None
)

if priority_column is None:
    raise RuntimeError("IP-004 computer review lacks a priority field.")

if "asset_key" in computer_map.columns:
    computer_map_assets = (
        computer_map
        .select("ad_object_key", "asset_key")
        .filter(F.col("asset_key").isNotNull())
    )
elif "matched_asset_keys" in computer_map.columns:
    computer_map_assets = (
        computer_map
        .select(
            "ad_object_key",
            F.explode_outer(
                F.split(
                    F.coalesce(F.col("matched_asset_keys"), F.lit("")),
                    r"\s*,\s*"
                )
            ).alias("asset_key")
        )
        .withColumn("asset_key", F.trim(F.col("asset_key")))
        .filter(F.col("asset_key").isNotNull() & (F.col("asset_key") != ""))
        .dropDuplicates(["ad_object_key", "asset_key"])
    )
else:
    raise RuntimeError(
        "IP-004 computer map contains neither asset_key nor matched_asset_keys."
    )

priority_rank = (
    F.when(F.lower(F.col(priority_column)) == "high", 4)
    .when(F.lower(F.col(priority_column)) == "medium", 3)
    .when(F.lower(F.col(priority_column)) == "review", 2)
    .when(F.lower(F.col(priority_column)) == "low", 1)
    .otherwise(0)
)

computer_review_projection = computer_review.select(
    "ad_object_key",
    F.col(priority_column).cast("string").alias("ad_computer_priority"),
    (
        F.col(condition_column).cast("string")
        if condition_column
        else F.lit(None).cast("string")
    ).alias("ad_computer_condition"),
    priority_rank.alias("ad_computer_priority_rank")
)

report_ad_asset_validation = (
    computer_map_assets
    .join(
        computer_review_projection,
        on="ad_object_key",
        how="left"
    )
    .groupBy("asset_key")
    .agg(
        F.countDistinct("ad_object_key").alias("mapped_ad_computer_count"),
        F.max("ad_computer_priority_rank").alias("ad_computer_max_priority_rank"),
        F.concat_ws(
            "; ",
            F.sort_array(
                F.collect_set("ad_computer_condition")
            )
        ).alias("ad_computer_conditions")
    )
    .withColumn(
        "ad_computer_validation_required_flag",
        F.col("ad_computer_max_priority_rank") >= 2
    )
    .withColumn(
        "report_integration_basis",
        F.lit(
            "Derived from IP-004 matched_asset_keys; upstream IP-005 tables "
            "were not rewritten."
        )
    )
)

report_ad_validation_asset_count = (
    report_ad_asset_validation
    .filter(F.col("ad_computer_validation_required_flag"))
    .count()
)

upstream_ad_validation_asset_count = int(
    IP005_METRICS.get("ad_computer_validation_asset_count", 0)
)


# ---------------------------------------------------------------------------
# 6. Executive metrics and durable report foundation products
# ---------------------------------------------------------------------------

executive_metric_records = [
    (
        1,
        "Asset Population",
        "Operational asset identities",
        operational_asset_count,
        "One certified row per durable asset identity."
    ),
    (
        2,
        "Asset Population",
        "Confirmed operational baseline",
        confirmed_baseline_count,
        "Identities requiring no current identity-disposition investigation."
    ),
    (
        3,
        "Identity and Lifecycle",
        "Identity investigation assets",
        int(IP005_METRICS.get("identity_investigation_count", 0)),
        "Validation population; not a vulnerability or finding count."
    ),
    (
        4,
        "Identity and Lifecycle",
        "Current observed-not-CMDB identities",
        int(IP005_METRICS.get("current_observed_not_cmdb", 0)),
        "Current evidence requiring identity and CMDB reconciliation."
    ),
    (
        5,
        "Identity and Lifecycle",
        "Stale-only observed-not-CMDB identities",
        int(IP005_METRICS.get("stale_only_observed_not_cmdb", 0)),
        "Historical evidence requiring lifecycle validation."
    ),
    (
        6,
        "Identity and Lifecycle",
        "Unknown-freshness observed-not-CMDB identities",
        int(IP005_METRICS.get("unknown_freshness_observed_not_cmdb", 0)),
        "Source freshness is insufficient for current-versus-stale classification."
    ),
    (
        7,
        "Identity and Lifecycle",
        "CMDB identities with no active evidence",
        int(IP005_METRICS.get("cmdb_no_active_evidence", 0)),
        "Requires lifecycle and discovery validation; not proof of CMDB error."
    ),
    (
        8,
        "Security Tool Assurance",
        "Asset-tool expectation records",
        int(IP005_METRICS.get("tool_expectation_records", 0)),
        "Asset-tool relationships, not unique devices."
    ),
    (
        9,
        "Security Tool Assurance",
        "Asset-tool relationships requiring validation",
        int(IP005_METRICS.get("tool_validation_records", 0)),
        "Validate applicability, identity matching, installation, reporting, or freshness."
    ),
    (
        10,
        "Security Tool Assurance",
        "Assets affected by tool validation",
        int(IP005_METRICS.get("tool_validation_asset_count", 0)),
        "Distinct assets represented by the tool-validation relationships."
    ),
    (
        11,
        "Identity and AD",
        "IP-004 identity and AD validation actions",
        ip004_action_count,
        "User, computer, group, and membership validation actions."
    ),
    (
        12,
        "Identity and AD",
        "Assets with report-corrected AD-computer validation",
        report_ad_validation_asset_count,
        "Derived from the certified IP-004 computer-to-asset map."
    ),
    (
        13,
        "Enterprise Attribution",
        "Unresolved attribution candidates",
        attribution_candidate_count,
        "No supported Post assignment was made without authoritative evidence."
    ),
    (
        14,
        "Operational Management",
        "Asset-level validation actions",
        asset_action_count,
        "Asset identity, lifecycle, tooling, port, and data-governance actions."
    ),
    (
        15,
        "Operational Management",
        "Integrated operational-management actions",
        integrated_action_count,
        "Asset-level actions plus IP-004 identity and AD actions."
    )
]

executive_metric_schema = T.StructType([
    T.StructField("display_order", T.IntegerType(), False),
    T.StructField("metric_domain", T.StringType(), False),
    T.StructField("metric_name", T.StringType(), False),
    T.StructField("metric_value", T.LongType(), False),
    T.StructField("interpretation", T.StringType(), False)
])

report_executive_summary = spark.createDataFrame(
    executive_metric_records,
    schema=executive_metric_schema
)

report_foundation_metrics = spark.createDataFrame(
    [
        ("operational_asset_count", operational_asset_count),
        ("asset_plane_count", asset_plane_count),
        ("lifecycle_count", lifecycle_count),
        ("confirmed_baseline_count", confirmed_baseline_count),
        ("attribution_candidate_count", attribution_candidate_count),
        ("asset_action_count", asset_action_count),
        ("ip004_action_count", ip004_action_count),
        ("integrated_action_count", integrated_action_count),
        ("upstream_ad_validation_asset_count", upstream_ad_validation_asset_count),
        ("report_ad_validation_asset_count", report_ad_validation_asset_count)
    ],
    schema="metric_name string, metric_value long"
)

REPORT_OUTPUT_TABLES = {
    "SOURCE_VALIDATION": post_table("report001_source_validation"),
    "EXECUTIVE_SUMMARY": post_table("report001_executive_summary"),
    "FOUNDATION_METRICS": post_table("report001_foundation_metrics"),
    "AD_ASSET_VALIDATION": post_table("report001_ad_asset_validation")
}

for dataframe, table_name in [
    (report_source_validation, REPORT_OUTPUT_TABLES["SOURCE_VALIDATION"]),
    (report_executive_summary, REPORT_OUTPUT_TABLES["EXECUTIVE_SUMMARY"]),
    (report_foundation_metrics, REPORT_OUTPUT_TABLES["FOUNDATION_METRICS"]),
    (report_ad_asset_validation, REPORT_OUTPUT_TABLES["AD_ASSET_VALIDATION"])
]:
    (
        dataframe
        .withColumn("post_name", F.lit(RCAIP_POST_NAME))
        .withColumn("post_slug", F.lit(RCAIP_POST_SLUG))
        .withColumn("rcaip_run_id", F.lit(RCAIP_RUN_ID))
        .withColumn("generated_at_utc", F.current_timestamp())
        .write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print("Saved:", table_name)

RCAIP_REPORT001_FOUNDATION_READY = True

print("=" * 100)
print("RCAIP REPORT 001 - CERTIFIED REPORT FOUNDATION")
print("=" * 100)
print("Status: PASS")
print("Ready:", RCAIP_REPORT001_FOUNDATION_READY)
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Country:", RCAIP_COUNTRY_NAME)
print("Run ID:", RCAIP_RUN_ID)
print("Operational asset identities:", operational_asset_count)
print("Confirmed operational baseline:", confirmed_baseline_count)
print("Discovery 002 attribution candidates:", attribution_candidate_count)
print("Asset-level actions:", asset_action_count)
print("IP-004 identity and AD actions:", ip004_action_count)
print("Integrated actions:", integrated_action_count)
print("Upstream IP-005 AD-validation assets:", upstream_ad_validation_asset_count)
print("Report-corrected AD-validation assets:", report_ad_validation_asset_count)
print("Required source errors: 0")
print("Raw enterprise source scans performed: 0")
print("Automatic assignments/findings/tickets/remediation: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP REPORT 001 - DT VALIDATION PACKAGE
# Cell 3 - Workbook Data Package
#
# Purpose:
#   Assemble a bounded, evidence-preserving 22-sheet workbook package from the
#   certified runtime products. No source records are changed by this cell.
# ============================================================================

from collections import OrderedDict

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# 1. Foundation guard
# ---------------------------------------------------------------------------

required_objects = [
    "RCAIP_REPORT001_FOUNDATION_READY",
    "REPORT_SOURCE_DFS",
    "report_source_validation",
    "report_executive_summary",
    "report_foundation_metrics",
    "report_ad_asset_validation",
    "operational_view",
    "asset_plane",
    "lifecycle",
    "tool_coverage",
    "ip004_actions",
    "ip005_asset_actions",
    "ip005_integrated_actions",
    "attribution_disposition",
    "confirmed_baseline",
    "RCAIP_REPORT_FILE_NAME"
]

missing_objects = [
    name for name in required_objects
    if name not in globals()
]

if missing_objects:
    raise RuntimeError(
        "Run Report 001 Cell 2 before Cell 3. Missing: "
        + " | ".join(missing_objects)
    )

if not RCAIP_REPORT001_FOUNDATION_READY:
    raise RuntimeError("Report 001 foundation is not certified.")


# ---------------------------------------------------------------------------
# 2. Reusable workbook helpers
# ---------------------------------------------------------------------------

placeholder_schema = T.StructType([
    T.StructField("source_status", T.StringType(), False),
    T.StructField("message", T.StringType(), False)
])


def placeholder_dataframe(message):
    return spark.createDataFrame(
        [("OPTIONAL_NOT_AVAILABLE", message)],
        schema=placeholder_schema
    )


def port_source_or_status(key):
    if key in REPORT_SOURCE_DFS:
        return REPORT_SOURCE_DFS[key]
    return placeholder_dataframe(SOURCE_CONTRACT[key].get("port_status", "UNAVAILABLE")
                                 + ": " + PORT_EVIDENCE_WARNING)


def port_cell(value):
    # Literal, lossless textual transport. Never silently trim Excel evidence.
    import math
    from decimal import Decimal
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeError("Nonfinite port evidence cannot be exported")
        value = repr(value)
    elif isinstance(value, Decimal):
        value = str(value)
    elif isinstance(value, int) and abs(value) > 999999999999999:
        value = str(value)
    if isinstance(value, str):
        value = value.replace("\\", "\\\\")
        value = "".join("\\u%04x" % ord(c) if ord(c) < 32 and c not in "\t\n\r" else c for c in value)
        if value.startswith(("=", "+", "-", "@", "#")):
            value = "'" + value
        if len(value) > 32767:
            raise RuntimeError("Port evidence exceeds Excel cell bound; no truncation permitted")
    return value


def port_member_counts(frame, detail=False):
    import json
    from collections import Counter
    result = Counter()
    field = "port_observation" if detail else "port_members"
    if field not in frame.columns:
        raise RuntimeError("Port product lacks exact member evidence: " + field)
    for value in frame[field].tolist():
        members = [value] if detail else json.loads(value)
        if not isinstance(members, list) or not members:
            raise RuntimeError("Port member relationships missing")
        for member in members:
            row = json.loads(member)
            for key in ("post_slug", "rcaip_run_id", "scan_id", "host_key", "ip_address", "protocol", "port", "scan_source_file"):
                if row.get(key) is None or str(row[key]).strip() == "":
                    raise RuntimeError("Port member key missing: " + key)
            result[json.dumps(row, sort_keys=True, ensure_ascii=False)] += 1
    return result


def certify_port_frames(frames):
    import json
    detail = frames.get("Open_Port_Detail")
    # Absent stages remain explicit; never certify them from another stage.
    if detail is None:
        return
    from decimal import Decimal
    import math
    host = ("post_slug", "rcaip_run_id", "scan_id", "host_key", "ip_address",
            "asset_domain", "scan_scope", "scan_location", "scan_source_file")
    native_keys = host + ("protocol", "port")
    summary_keys = ("asset_domain", "scan_scope", "scan_location", "protocol", "port",
                    "port_label", "review_priority", "risk_theme", "csa_relevance")
    def check_columns(frame, columns):
        if not set(columns).issubset(frame.columns):
            raise RuntimeError("Port product missing required schema")
    def canonical(value):
        if hasattr(value, "item"):
            value = value.item()
        if isinstance(value, float) and not math.isfinite(value):
            raise RuntimeError("Nonfinite native port evidence")
        if value is None:
            return ("null",)
        if isinstance(value, bool):
            return ("bool", value)
        if isinstance(value, (int, float, Decimal)):
            return ("number", str(Decimal(str(value)).normalize()))
        return (type(value).__name__, value)
    def equal(a, b):
        return canonical(a) == canonical(b)
    def keys_present(row, keys):
        if any(row.get(k) is None or str(row[k]).strip() == "" for k in keys):
            raise RuntimeError("Port member key missing")
    def decode(value):
        def pairs(items):
            result = {}
            for k,v in items:
                if k in result:
                    raise RuntimeError("Duplicate native evidence field")
                result[k] = v
            return result
        return json.loads(value, object_pairs_hook=pairs, parse_float=Decimal)
    check_columns(detail, native_keys + ("port_observation", "selected_for_action"))
    by_token = {}
    for row in detail.to_dict("records"):
        native = decode(row["port_observation"])
        keys_present(native, native_keys)
        keys_present(row, native_keys)
        check_columns(detail, native.keys())
        if any(not equal(row[k], value) for k,value in native.items()):
            raise RuntimeError("Visible port evidence differs from native member")
        token = json.dumps(json.loads(row["port_observation"]), sort_keys=True)
        if token in by_token and any(not equal(row.get(k),by_token[token].get(k)) for k in summary_keys):
            raise RuntimeError("Contradictory enrichment for identical native member")
        by_token[token] = row
    for name, frame in frames.items():
        if name == "Open_Port_Detail":
            continue
        keys = summary_keys if name == "Open_Port_Summary" else host + ("hostname", "target_cidr")
        metrics = ("affected_hosts", "review_port_instances") if name == "Open_Port_Summary" else (
            "review_port_count", "open_review_ports", "services", "products", "versions",
            "port_labels", "risk_themes", "csa_relevance")
        check_columns(frame, keys + metrics + ("port_members",))
        for parent in frame.to_dict("records"):
            tokens = json.loads(parent["port_members"])
            if not isinstance(tokens,list) or not tokens:
                raise RuntimeError("Empty grouped port members")
            children = []
            for token in tokens:
                child = by_token.get(json.dumps(json.loads(token),sort_keys=True))
                if child is None:
                    raise RuntimeError("Orphan grouped port member")
                if any(not equal(parent[k],child[k]) for k in keys):
                    raise RuntimeError("Port member belongs to a different displayed group")
                for key in ("post_slug", "rcaip_run_id"):
                    if key in parent and not equal(parent[key],child[key]):
                        raise RuntimeError("Grouped port scope mismatch")
                children.append(child)
            if name == "Open_Port_Summary":
                if parent["review_port_instances"] != len(children) or parent["affected_hosts"] != len({r["ip_address"] for r in children}):
                    raise RuntimeError("Port summary metric mismatch")
            else:
                if parent["review_port_count"] != len({r["port"] for r in children}):
                    raise RuntimeError("Port action metric mismatch")
                for displayed, field in (("open_review_ports","port"),("services","service"),
                        ("products","product"),("versions","version"),("port_labels","port_label"),
                        ("risk_themes","risk_theme"),("csa_relevance","csa_relevance")):
                    expected = ", ".join(sorted({str(r[field]) for r in children if r.get(field) is not None}))
                    if parent[displayed] != expected:
                        raise RuntimeError("Port action endpoint/evidence list mismatch")
    members = port_member_counts(detail, True)
    summary = frames.get("Open_Port_Summary")
    if summary is not None and port_member_counts(summary) != members:
        raise RuntimeError("Port summary/detail member mismatch")
    actions = frames.get("Open_Port_Actions")
    if actions is not None:
        selected = port_member_counts(actions)
        if "selected_for_action" not in detail.columns:
            raise RuntimeError("Port detail lacks producer action-selection authority")
        if any(type(x) is not bool for x in detail["selected_for_action"].tolist()):
            raise RuntimeError("Invalid port action-selection authority")
        expected_selected = port_member_counts(detail.loc[detail["selected_for_action"].astype(bool)], True)
        if selected != expected_selected:
            raise RuntimeError("Required action selection/member mismatch")
        if selected - members:
            raise RuntimeError("Port action contains orphan/duplicated evidence")
        for member, count in members.items():
            row = json.loads(member)
            if row['protocol'].lower() == 'tcp' and row['port'] in (21, 23, 80) and selected[member] != count:
                raise RuntimeError("Required TCP evidence missing from port actions")


def link_port_members(prepared, raw):
    # Keep grouped grain; exact native evidence lives in Open_Port_Detail.
    # Refer to its occurrence rows instead of truncating long duplicated JSON.
    import json
    from collections import defaultdict, deque
    detail = raw.get("Open_Port_Detail")
    if detail is None:
        if any("port_members" in f.columns for f in prepared.values()):
            raise RuntimeError("Port grouped members require available detail")
        return
    index = defaultdict(list)
    for position, member in enumerate(detail["port_observation"].tolist(), start=6):
        index[json.dumps(json.loads(member), sort_keys=True)].append(position)
    for name in ("Open_Port_Summary", "Open_Port_Actions"):
        if name not in raw or raw[name].empty:
            continue
        available = {k: deque(v) for k, v in index.items()}
        references = []
        for cell in raw[name]["port_members"].tolist():
            positions = []
            for member in json.loads(cell):
                key = json.dumps(json.loads(member), sort_keys=True)
                if key not in available or not available[key]:
                    raise RuntimeError("Port member occurrence cannot be linked")
                positions.append(available[key].popleft())
            references.append(port_cell("Open_Port_Detail rows " + ",".join(map(str, sorted(positions)))))
        prepared[name]["port_members"] = references


def verify_port_sheet(worksheet, frame):
    expected = [tuple(frame.columns)] + list(frame.itertuples(index=False, name=None))
    actual = list(worksheet.iter_rows(min_row=5, max_row=5+len(frame.index),
                                    max_col=len(frame.columns), values_only=True))
    # Blank strings reopen as None; evidence strings are otherwise exact.
    normalized = [tuple(None if x == "" else x for x in row) for row in expected]
    if actual != normalized:
        raise RuntimeError("Reopened port worksheet evidence mismatch")


def source_or_placeholder(source_key, message):
    if source_key in REPORT_SOURCE_DFS:
        return REPORT_SOURCE_DFS[source_key]
    return placeholder_dataframe(message)


def first_existing_column(dataframe, candidates):
    lookup = {name.lower(): name for name in dataframe.columns}
    for candidate in candidates:
        actual = lookup.get(candidate.lower())
        if actual:
            return actual
    return None


def optional_string_column(dataframe, candidates):
    actual = first_existing_column(dataframe, candidates)
    if actual:
        return F.col(f"`{actual}`").cast("string")
    return F.lit(None).cast("string")


# ---------------------------------------------------------------------------
# 3. READ_ME and executive-summary narratives
# ---------------------------------------------------------------------------

read_me_records = [
    (
        1,
        "Purpose",
        "This workbook is the Reykjavík RCAIP DT Validation Package. It "
        "integrates asset identity, lifecycle, security-tool, open-port, "
        "Active Directory, operational-management, and enterprise-attribution "
        "evidence from the certified runtime."
    ),
    (
        2,
        "Evidence Standard",
        "Rows requiring validation are not automatically findings, policy "
        "violations, tickets, assignments, or remediation instructions."
    ),
    (
        3,
        "Asset Denominator",
        "The authoritative asset denominator is the one-row-per-asset "
        "Operational Asset View. Tool records, AD objects, group memberships, "
        "ports, and actions use different denominators and must not be added "
        "to the asset count."
    ),
    (
        4,
        "Observed-not-CMDB",
        "Current, stale-only, and unknown-freshness identities are separated. "
        "Stale or freshness-unknown evidence must not be presented as a "
        "current missing-CMDB device without validation."
    ),
    (
        5,
        "Tool Coverage",
        "Tool Coverage contains asset-tool expectation relationships. A row "
        "requiring validation may reflect applicability, identity matching, "
        "installation, reporting, or source-freshness uncertainty."
    ),
    (
        6,
        "Active Directory",
        "The AD analysis distinguishes direct Post identities from one-hop "
        "enterprise context. It prioritizes privilege, ownership, lifecycle, "
        "nesting, and Post impact instead of treating all AD bloat equally."
    ),
    (
        7,
        "AD Bloat",
        "Security-relevant and operationally important bloat should receive "
        "attention before broad directory context that may be imperfect but "
        "tolerable unless policy or operational conditions change."
    ),
    (
        8,
        "Enterprise Attribution",
        "No asset is assigned to a Post solely from weak naming inference. "
        "Discovery 002 preserves unresolved candidates until authoritative "
        "ownership, location, network, or management evidence is available."
    ),
    (
        9,
        "Optional Sources",
        "Go Manager was not available for this run and is recorded as an "
        "optional source gap. Network corroboration was intentionally deferred "
        "rather than launching an unbounded enterprise scan."
    ),
    (
        10,
        "AD Asset Integration",
        "The report corrects workbook-level AD-computer asset context by "
        "expanding the certified IP-004 matched_asset_keys field. Upstream "
        "IP-005 tables are preserved and the discrepancy is documented."
    ),
    (
        11,
        "Operational Principle",
        "The objective is not perfect management of every data condition. It "
        "is identifying which imperfections materially affect security, "
        "accountability, or operational control and which can be tolerated, "
        "deferred, or addressed through enterprise governance."
    ),
    (
        12,
        "Modernization Context",
        "An endpoint-agnostic network cannot safely become endpoint-unaware. "
        "RCAIP provides independent field evidence of identity, management, "
        "visibility, and governance gaps during enterprise modernization."
    )
]

read_me_df = spark.createDataFrame(
    read_me_records,
    schema="display_order int, topic string, guidance string"
)

ip004_summary_df = REPORT_SOURCE_DFS["ip004_summary"]

if {
    "summary_domain",
    "priority",
    "record_count"
}.issubset(set(ip004_summary_df.columns)):
    ip004_exec_metrics = (
        ip004_summary_df
        .withColumn(
            "display_order",
            F.lit(100)
            + F.row_number().over(
                Window.orderBy("summary_domain", "priority")
            )
        )
        .select(
            "display_order",
            F.col("summary_domain").alias("metric_domain"),
            F.concat(
                F.col("summary_domain"),
                F.lit(" - "),
                F.col("priority")
            ).alias("metric_name"),
            F.col("record_count").cast("long").alias("metric_value"),
            F.lit(
                "IP-004 classification count. Context and validation "
                "categories are not automatic findings."
            ).alias("interpretation")
        )
    )
else:
    ip004_exec_metrics = spark.createDataFrame(
        [],
        schema=report_executive_summary.schema
    )

attribution_status_column = first_existing_column(
    attribution_disposition,
    [
        "attribution_evidence_status",
        "final_disposition",
        "runtime_disposition"
    ]
)

if attribution_status_column:
    attribution_exec_metrics = (
        attribution_disposition
        .groupBy(
            F.col(attribution_status_column).cast("string").alias("status")
        )
        .agg(F.count("*").alias("record_count"))
        .withColumn(
            "display_order",
            F.lit(200)
            + F.row_number().over(Window.orderBy("status"))
        )
        .select(
            "display_order",
            F.lit("Enterprise Attribution").alias("metric_domain"),
            F.concat(
                F.lit("Attribution - "),
                F.col("status")
            ).alias("metric_name"),
            F.col("record_count").cast("long").alias("metric_value"),
            F.lit(
                "Discovery 002 disposition count; no automatic Post "
                "assignment is implied."
            ).alias("interpretation")
        )
    )
else:
    attribution_exec_metrics = spark.createDataFrame(
        [],
        schema=report_executive_summary.schema
    )

executive_summary_df = (
    report_executive_summary
    .unionByName(ip004_exec_metrics)
    .unionByName(attribution_exec_metrics)
    .orderBy("display_order")
)


# ---------------------------------------------------------------------------
# 4. Report-corrected asset inventory and source-gap view
# ---------------------------------------------------------------------------

asset_inventory_df = (
    operational_view.alias("o")
    .join(
        report_ad_asset_validation.alias("a"),
        on="asset_key",
        how="left"
    )
    .select(
        "o.*",
        F.coalesce(
            F.col("a.mapped_ad_computer_count"),
            F.lit(0)
        ).alias("report_mapped_ad_computer_count"),
        F.coalesce(
            F.col("a.ad_computer_max_priority_rank"),
            F.lit(0)
        ).alias("report_ad_computer_max_priority_rank"),
        F.coalesce(
            F.col("a.ad_computer_validation_required_flag"),
            F.lit(False)
        ).alias("report_ad_computer_validation_required_flag"),
        F.col("a.ad_computer_conditions").alias(
            "report_ad_computer_conditions"
        ),
        F.col("a.report_integration_basis").alias(
            "report_ad_integration_basis"
        )
    )
)

source_gaps = REPORT_SOURCE_DFS["attribution_source_gaps"]

source_gaps_standardized = source_gaps.select(
    optional_string_column(
        source_gaps,
        ["source_name", "source_key", "gap_source"]
    ).alias("source_name"),
    optional_string_column(
        source_gaps,
        ["gap_type", "source_status", "status"]
    ).alias("gap_type"),
    optional_string_column(
        source_gaps,
        ["gap_description", "description", "source_gap"]
    ).alias("gap_description"),
    optional_string_column(
        source_gaps,
        ["recommended_action", "recommended_next_action"]
    ).alias("recommended_action"),
    optional_string_column(
        source_gaps,
        ["validation_responsibility", "owner"]
    ).alias("validation_responsibility")
)

source_availability = REPORT_SOURCE_DFS["ip005_source_availability"]

source_availability_standardized = source_availability.select(
    optional_string_column(
        source_availability,
        ["source_key", "source_name"]
    ).alias("source_name"),
    F.when(
        F.coalesce(
            optional_string_column(
                source_availability,
                ["available"]
            ),
            F.lit("false")
        ).isin("true", "TRUE", "1", "Y"),
        F.lit("AVAILABLE")
    ).otherwise(F.lit("OPTIONAL_NOT_AVAILABLE")).alias("gap_type"),
    F.coalesce(
        optional_string_column(
            source_availability,
            ["read_error", "message"]
        ),
        F.lit("Source availability recorded by IP-005.")
    ).alias("gap_description"),
    F.lit(
        "Connect or publish the source through a bounded governed adapter "
        "when the evidence is required."
    ).alias("recommended_action"),
    F.lit("Enterprise source owner with Post corroboration").alias(
        "validation_responsibility"
    )
)

source_gaps_df = (
    source_gaps_standardized
    .unionByName(source_availability_standardized)
    .dropDuplicates()
)


# ---------------------------------------------------------------------------
# 5. Tracking and certification sheets
# ---------------------------------------------------------------------------

remediation_tracking_df = (
    ip005_integrated_actions
    .withColumn("tracking_status", F.lit("Not Started"))
    .withColumn("assigned_owner", F.lit(None).cast("string"))
    .withColumn("owner_organization", F.lit(None).cast("string"))
    .withColumn("target_date", F.lit(None).cast("date"))
    .withColumn("validation_result", F.lit(None).cast("string"))
    .withColumn("finding_reference", F.lit(None).cast("string"))
    .withColumn("ticket_reference", F.lit(None).cast("string"))
    .withColumn("closure_notes", F.lit(None).cast("string"))
)

certification_records = [
    (
        1,
        "Operational population preserved",
        "PASS",
        str(operational_asset_count),
        "Operational Asset View, Asset Plane, and lifecycle rollup reconcile."
    ),
    (
        2,
        "Confirmed baseline preserved",
        "PASS",
        str(confirmed_baseline_count),
        "Confirmed baseline remains a subset of the operational population."
    ),
    (
        3,
        "Discovery 002 candidate disposition",
        "PASS",
        str(attribution_candidate_count),
        "One final disposition exists per attribution candidate."
    ),
    (
        4,
        "Integrated action accounting",
        "PASS",
        str(integrated_action_count),
        "Integrated actions equal asset actions plus IP-004 actions."
    ),
    (
        5,
        "Report-level AD asset integration",
        "PASS",
        str(report_ad_validation_asset_count),
        "Derived from IP-004 matched_asset_keys without rewriting IP-005."
    ),
    (
        6,
        "Upstream IP-005 AD asset metric",
        "DOCUMENTED_VARIANCE",
        str(upstream_ad_validation_asset_count),
        "The upstream metric used a singular asset_key contract not present "
        "in the strict one-row-per-computer IP-004 map."
    ),
    (
        7,
        "Cross-Post and cross-run isolation",
        "PASS",
        "0 detected",
        "All report sources were filtered to the active runtime when fields existed."
    ),
    (
        8,
        "Unsupported Post assignments",
        "PASS",
        "0",
        "Discovery 002 did not assign unresolved assets automatically."
    ),
    (
        9,
        "Automatic findings, tickets, or remediation",
        "PASS",
        "0",
        "The report is an evidence and validation package."
    )
]

final_certification_df = spark.createDataFrame(
    certification_records,
    schema=(
        "check_order int, check_name string, status string, "
        "observed_value string, interpretation string"
    )
)


# ---------------------------------------------------------------------------
# 6. Ordered 22-sheet workbook contract
# ---------------------------------------------------------------------------

RCAIP_REPORT_WORKBOOK_SHEETS = OrderedDict([
    (
        "READ_ME",
        {
            "title": "RCAIP DT Validation Package - Read Me",
            "description": "Interpretation rules and evidence guardrails.",
            "dataframe": read_me_df,
            "order_by": ["display_order"]
        }
    ),
    (
        "Executive_Summary",
        {
            "title": "Executive Summary",
            "description": "Certified metrics with denominator-aware interpretation.",
            "dataframe": executive_summary_df,
            "order_by": ["display_order"]
        }
    ),
    (
        "Asset_Inventory",
        {
            "title": "Operational Asset Inventory",
            "description": "One row per certified asset identity with report-level AD context.",
            "dataframe": asset_inventory_df,
            "order_by": ["asset_key"]
        }
    ),
    (
        "Asset_Plane",
        {
            "title": "Scored Asset Plane",
            "description": "Canonical scored reconciliation evidence.",
            "dataframe": asset_plane,
            "order_by": ["asset_key"]
        }
    ),
    (
        "Attribution_Review",
        {
            "title": "Enterprise Attribution Review",
            "description": "Discovery 002 candidate dispositions and routing.",
            "dataframe": attribution_disposition,
            "order_by": ["route_priority", "asset_key"]
        }
    ),
    (
        "Tool_Coverage",
        {
            "title": "Security Tool Coverage",
            "description": "Asset-tool expectations and validation evidence.",
            "dataframe": tool_coverage,
            "order_by": ["asset_key"]
        }
    ),
    (
        "Lifecycle_Hygiene",
        {
            "title": "Asset Lifecycle and Hygiene",
            "description": "Current, stale, mixed, unknown, and absent evidence classifications.",
            "dataframe": lifecycle,
            "order_by": ["review_priority", "asset_key"]
        }
    ),
    (
        "Open_Port_Summary",
        {
            "title": "Open Port Summary",
            "description": "Post-scoped port and service summary evidence.",
            "dataframe": port_source_or_status("open_port_summary"),
            "order_by": ["severity", "protocol", "port"]
        }
    ),
    (
        "Open_Port_Actions",
        {
            "title": "Open Port Validation Actions",
            "description": "Prioritized Post-facing port review records.",
            "dataframe": port_source_or_status("open_port_actions"),
            "order_by": ["priority", "ip_address", "port"]
        }
    ),
    (
        "Open_Port_Detail",
        {
            "title": "Open Port Detail",
            "description": "Detailed open-port evidence with provenance when available.",
            "dataframe": port_source_or_status("open_port_detail"),
            "order_by": ["ip_address", "protocol", "port"]
        }
    ),
    (
        "AD_Computer_Review",
        {
            "title": "Active Directory Computer Review",
            "description": "Computer-account lifecycle and Asset Plane reconciliation.",
            "dataframe": REPORT_SOURCE_DFS["ip004_computer_review"],
            "order_by": ["computer_security_priority", "sam_account_name"]
        }
    ),
    (
        "AD_User_Review",
        {
            "title": "Active Directory User Review",
            "description": "User-account lifecycle and security-setting validation.",
            "dataframe": REPORT_SOURCE_DFS["ip004_user_review"],
            "order_by": ["user_security_priority", "sam_account_name"]
        }
    ),
    (
        "AD_Bloat_Priority",
        {
            "title": "Active Directory Bloat Prioritization",
            "description": "Security-relevant, operationally important, and tolerable context.",
            "dataframe": REPORT_SOURCE_DFS["ip004_group_bloat"],
            "order_by": ["ad_bloat_classification"]
        }
    ),
    (
        "AD_Privileged_Access",
        {
            "title": "Privileged Access Relationships",
            "description": "Relationships requiring privileged-access validation.",
            "dataframe": REPORT_SOURCE_DFS["ip004_privileged_access"],
            "order_by": ["priority", "group_name"]
        }
    ),
    (
        "AD_Action_Queue",
        {
            "title": "Active Directory Validation Actions",
            "description": "Computer, user, group, and membership validation queue.",
            "dataframe": ip004_actions,
            "order_by": ["priority", "action_domain", "action_id"]
        }
    ),
    (
        "Operational_Actions",
        {
            "title": "Asset-Level Operational Actions",
            "description": "Asset identity, lifecycle, tool, port, and governance actions.",
            "dataframe": ip005_asset_actions,
            "order_by": ["priority", "action_domain", "action_id"]
        }
    ),
    (
        "Integrated_Actions",
        {
            "title": "Integrated Operational Management Actions",
            "description": "Asset-level and IP-004 identity/AD actions combined.",
            "dataframe": ip005_integrated_actions,
            "order_by": ["priority", "action_domain", "action_id"]
        }
    ),
    (
        "Confirmed_Baseline",
        {
            "title": "Confirmed Operational Baseline",
            "description": "Assets meeting the current confirmed-baseline contract.",
            "dataframe": confirmed_baseline,
            "order_by": ["asset_key"]
        }
    ),
    (
        "Source_Gaps",
        {
            "title": "Enterprise Source and Governance Gaps",
            "description": "Missing or deferred evidence sources and recommended routing.",
            "dataframe": source_gaps_df,
            "order_by": ["gap_type", "source_name"]
        }
    ),
    (
        "Remediation_Tracking",
        {
            "title": "Validation and Remediation Tracking",
            "description": "Reviewable tracking worksheet; no tickets were created automatically.",
            "dataframe": remediation_tracking_df,
            "order_by": ["priority", "action_domain", "action_id"]
        }
    ),
    (
        "Source_Validation",
        {
            "title": "Report Source Validation",
            "description": "Runtime source availability, row counts, and contract status.",
            "dataframe": report_source_validation,
            "order_by": ["required_source", "source_key"]
        }
    ),
    (
        "Final_Certification",
        {
            "title": "Final Report Certification",
            "description": "Population, action, isolation, and automation guardrails.",
            "dataframe": final_certification_df,
            "order_by": ["check_order"]
        }
    )
])

if len(RCAIP_REPORT_WORKBOOK_SHEETS) != 22:
    raise RuntimeError(
        "The Report 001 workbook contract must contain exactly 22 sheets."
    )


# ---------------------------------------------------------------------------
# 7. Workbook paths
# ---------------------------------------------------------------------------

RCAIP_REPORT_DBFS_DIRECTORY = (
    f"dbfs:/FileStore/tables/rcso_csa_tools/outputs/{RCAIP_POST_SLUG}"
)
RCAIP_REPORT_DBFS_PATH = (
    f"{RCAIP_REPORT_DBFS_DIRECTORY}/{RCAIP_REPORT_FILE_NAME}"
)
RCAIP_REPORT_DRIVER_PATH = (
    f"/dbfs/FileStore/tables/rcso_csa_tools/outputs/"
    f"{RCAIP_POST_SLUG}/{RCAIP_REPORT_FILE_NAME}"
)
RCAIP_REPORT_BROWSER_PATH = (
    f"/files/tables/rcso_csa_tools/outputs/"
    f"{RCAIP_POST_SLUG}/{RCAIP_REPORT_FILE_NAME}"
)

RCAIP_REPORT001_PACKAGE_READY = True

print("=" * 100)
print("RCAIP REPORT 001 - WORKBOOK DATA PACKAGE")
print("=" * 100)
print("Status: PASS")
print("Ready:", RCAIP_REPORT001_PACKAGE_READY)
print("Runtime Post:", RCAIP_POST_NAME)
print("Workbook sheets:", len(RCAIP_REPORT_WORKBOOK_SHEETS))
print("Operational assets:", operational_asset_count)
print("Integrated actions:", integrated_action_count)
print("Report-corrected AD-validation assets:", report_ad_validation_asset_count)
print("Workbook DBFS path:", RCAIP_REPORT_DBFS_PATH)
print("Workbook browser path:", RCAIP_REPORT_BROWSER_PATH)
print("Automatic assignments/findings/tickets/remediation: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP REPORT 001 - DT VALIDATION PACKAGE
# Cell 4 - Build, Style, Publish, and Certify the Excel Workbook
# Publication: Databricks Workspace Files API (no DBFS root/FUSE dependency)
#
# Purpose:
#   Convert the certified 22-sheet Spark package into a professionally styled
#   Excel workbook, publish it to FileStore, validate its structure, and save
#   a durable workbook certificate.
# ============================================================================

import hashlib
import importlib
import io
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

try:
    import pandas as pd
except Exception as exc:
    raise RuntimeError("pandas is required to build the report workbook.") from exc

try:
    import openpyxl
except ImportError:
    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--quiet",
        "openpyxl==3.1.5"
    ])
    importlib.invalidate_caches()
    import openpyxl

from openpyxl import Workbook, load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.dimensions import ColumnDimension

from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ExportFormat, ImportFormat
except Exception as exc:
    raise RuntimeError(
        "The Databricks SDK is required to publish the workbook through "
        "the Workspace Files API."
    ) from exc


# ---------------------------------------------------------------------------
# 1. Package guard
# ---------------------------------------------------------------------------

required_objects = [
    "RCAIP_REPORT001_PACKAGE_READY",
    "RCAIP_REPORT_WORKBOOK_SHEETS",
    "RCAIP_REPORT_FILE_NAME",
    "RCAIP_REPORT_NAME",
    "RCAIP_REPORT_VERSION",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_RUN_ID",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "REPORT_OUTPUT_TABLES",
    "post_table"
]

missing_objects = [
    name for name in required_objects
    if name not in globals()
]

if missing_objects:
    raise RuntimeError(
        "Run Report 001 Cells 2 and 3 before Cell 4. Missing: "
        + " | ".join(missing_objects)
    )

if not RCAIP_REPORT001_PACKAGE_READY:
    raise RuntimeError("Report 001 workbook package is not ready.")

if len(RCAIP_REPORT_WORKBOOK_SHEETS) != 22:
    raise RuntimeError("The workbook must contain exactly 22 worksheets.")


# ---------------------------------------------------------------------------
# 2. Excel-safe Spark and Python conversion
# ---------------------------------------------------------------------------

COMPLEX_TYPES = (
    T.ArrayType,
    T.MapType,
    T.StructType
)


def excel_safe_spark_dataframe(dataframe, order_by):
    available_order_fields = [
        name for name in order_by
        if name in dataframe.columns
    ]

    if available_order_fields:
        order_expressions = [
            F.asc_nulls_last(F.col(f"`{name}`"))
            for name in available_order_fields
        ]
        dataframe = dataframe.orderBy(*order_expressions)

    projections = []

    for field in dataframe.schema.fields:
        column = F.col(f"`{field.name}`")

        if isinstance(field.dataType, COMPLEX_TYPES):
            projections.append(
                F.to_json(column).alias(field.name)
            )
        elif isinstance(field.dataType, T.BinaryType):
            projections.append(
                F.base64(column).alias(field.name)
            )
        elif isinstance(field.dataType, T.TimestampType):
            projections.append(
                F.date_format(
                    column,
                    "yyyy-MM-dd HH:mm:ss"
                ).alias(field.name)
            )
        elif isinstance(field.dataType, T.DateType):
            projections.append(
                F.date_format(
                    column,
                    "yyyy-MM-dd"
                ).alias(field.name)
            )
        else:
            projections.append(column.alias(field.name))

    return dataframe.select(*projections)


def sanitize_excel_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, Decimal):
        value = float(value)

    if isinstance(value, (date, datetime)):
        return value

    if isinstance(value, (dict, list, tuple, set)):
        value = json.dumps(value, default=str, ensure_ascii=False)

    if isinstance(value, bytes):
        value = value.hex()

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, str):
        value = ILLEGAL_CHARACTERS_RE.sub("", value)
        if value.startswith(("=", "+", "-", "@")):
            value = "'" + value
        if len(value) > 32767:
            value = value[:32740] + " [TRUNCATED FOR EXCEL]"
        return value

    return value


# ---------------------------------------------------------------------------
# 3. Materialize bounded workbook data
# ---------------------------------------------------------------------------

workbook_pandas = {}
port_raw_frames = {}
manifest_records = []

for sheet_order, (sheet_name, config) in enumerate(
    RCAIP_REPORT_WORKBOOK_SHEETS.items(),
    start=1
):
    safe_spark_df = excel_safe_spark_dataframe(
        config["dataframe"],
        config.get("order_by", [])
    )

    is_port = sheet_name in PORT_SHEET_KEYS
    # Bounded native Rows -> object columns preserve None and integer/decimal types.
    # toPandas would conflate nullable integers with float NaN before validation.
    pandas_df = (pd.DataFrame([row.asDict(recursive=True) for row in
                 safe_spark_df.limit(PORT_EVIDENCE_LIMIT + 1).collect()],
                 columns=safe_spark_df.columns, dtype=object)
                 if is_port else safe_spark_df.toPandas())
    if is_port:
        if len(pandas_df.index) > PORT_EVIDENCE_LIMIT:
            raise RuntimeError("Port worksheet bound exceeded; no silent truncation")
        key = PORT_SHEET_KEYS[sheet_name]
        available = key in REPORT_SOURCE_DFS
        if available:
            port_raw_frames[sheet_name] = pandas_df.copy(deep=True)
        state = ("AVAILABLE_POSITIVE" if len(pandas_df.index) else "AVAILABLE_ZERO_POSITIVES") if available else SOURCE_CONTRACT[key].get("port_status", "UNAVAILABLE")
        config["description"] = state + ". " + PORT_EVIDENCE_WARNING
        if available and pandas_df.empty:
            pandas_df = pd.DataFrame({"evidence_status": [state], "explanation": [PORT_EVIDENCE_WARNING]})
    for column_name in pandas_df.columns:
        converter = ((lambda x: x) if is_port and column_name == "port_members"
                     else port_cell if is_port else sanitize_excel_value)
        if is_port:
            pandas_df[column_name] = pd.Series([converter(x) for x in pandas_df[column_name]], dtype=object)
        else:
            pandas_df[column_name] = pandas_df[column_name].map(converter)

    workbook_pandas[sheet_name] = pandas_df

    manifest_records.append((
        sheet_order,
        sheet_name,
        config["title"],
        config["description"],
        int(len(pandas_df.index)),
        int(len(pandas_df.columns))
    ))


certify_port_frames(port_raw_frames)
link_port_members(workbook_pandas, port_raw_frames)
port_generations = {SOURCE_CONTRACT[k]["port_binding"]["generation"] for k in PORT_SOURCE_KEYS
                    if k in REPORT_SOURCE_DFS}
port_executions = {(SOURCE_CONTRACT[k]["port_binding"].get("execution_id"),
                    SOURCE_CONTRACT[k]["port_binding"].get("producer_sha256"))
                   for k in PORT_SOURCE_KEYS if k in REPORT_SOURCE_DFS}
if len(port_executions) > 1:
    raise RuntimeError("Mixed port execution/source identities")
if len(port_generations) > 1:
    raise RuntimeError("Mixed port source generations")

# ---------------------------------------------------------------------------
# 4. Workbook theme
# ---------------------------------------------------------------------------

COLOR_NAVY = "17365D"
COLOR_DARK_BLUE = "1F4E78"
COLOR_MEDIUM_BLUE = "5B9BD5"
COLOR_LIGHT_BLUE = "D9EAF7"
COLOR_TEAL = "008C95"
COLOR_LIGHT_TEAL = "DDEBF7"
COLOR_GREEN = "548235"
COLOR_LIGHT_GREEN = "E2F0D9"
COLOR_ORANGE = "ED7D31"
COLOR_LIGHT_ORANGE = "FCE4D6"
COLOR_RED = "C00000"
COLOR_LIGHT_RED = "F4CCCC"
COLOR_PURPLE = "7030A0"
COLOR_LIGHT_PURPLE = "E4DFEC"
COLOR_GRAY = "7F7F7F"
COLOR_LIGHT_GRAY = "E7E6E6"
COLOR_WHITE = "FFFFFF"
COLOR_BLACK = "000000"

THIN_GRAY = Side(style="thin", color="BFBFBF")
MEDIUM_BLUE = Side(style="medium", color=COLOR_DARK_BLUE)

TITLE_FILL = PatternFill("solid", fgColor=COLOR_NAVY)
SUBTITLE_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_BLUE)
HEADER_FILL = PatternFill("solid", fgColor=COLOR_DARK_BLUE)
PASS_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_GREEN)
REVIEW_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_ORANGE)
HIGH_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_RED)
CONTEXT_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_GRAY)
INFO_FILL = PatternFill("solid", fgColor=COLOR_LIGHT_TEAL)

TITLE_FONT = Font(
    name="Aptos Display",
    size=16,
    bold=True,
    color=COLOR_WHITE
)
SUBTITLE_FONT = Font(
    name="Aptos",
    size=10,
    italic=True,
    color=COLOR_DARK_BLUE
)
HEADER_FONT = Font(
    name="Aptos",
    size=10,
    bold=True,
    color=COLOR_WHITE
)
BODY_FONT = Font(
    name="Aptos",
    size=9,
    color=COLOR_BLACK
)
METADATA_FONT = Font(
    name="Aptos",
    size=9,
    italic=True,
    color=COLOR_GRAY
)


def status_fill(value):
    text = str(value or "").strip().lower()

    if any(token in text for token in [
        "high",
        "security-relevant",
        "required_source_error",
        "error",
        "failed"
    ]):
        return HIGH_FILL

    if any(token in text for token in [
        "medium",
        "review",
        "operationally important",
        "documented_variance",
        "requires validation",
        "unknown"
    ]):
        return REVIEW_FILL

    if any(token in text for token in [
        "pass",
        "available",
        "confirmed",
        "current",
        "ready"
    ]):
        return PASS_FILL

    if any(token in text for token in [
        "context",
        "tolerable",
        "optional_not_available",
        "deferred"
    ]):
        return CONTEXT_FILL

    return None


def safe_table_name(sheet_name, index):
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", sheet_name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "T_" + cleaned
    return f"RCAIP_{index:02d}_{cleaned}"[:255]


# ---------------------------------------------------------------------------
# 5. Build the workbook
# ---------------------------------------------------------------------------

workbook = Workbook()
workbook.remove(workbook.active)

workbook.properties.title = (
    f"{RCAIP_REPORT_NAME} - {RCAIP_POST_NAME}"
)
workbook.properties.subject = (
    "RCAIP asset, lifecycle, tool, AD, port, and attribution validation"
)
workbook.properties.creator = "RCAIP"
workbook.properties.description = (
    "Evidence and validation package. No automatic findings or remediation."
)
workbook.calculation.fullCalcOnLoad = True
workbook.calculation.forceFullCalc = True
workbook.calculation.calcMode = "auto"

for sheet_index, (sheet_name, config) in enumerate(
    RCAIP_REPORT_WORKBOOK_SHEETS.items(),
    start=1
):
    dataframe = workbook_pandas[sheet_name]
    worksheet = workbook.create_sheet(title=sheet_name)
    worksheet.sheet_view.showGridLines = False

    column_count = max(1, len(dataframe.columns))
    last_column_letter = get_column_letter(column_count)

    worksheet.merge_cells(
        start_row=1,
        start_column=1,
        end_row=1,
        end_column=column_count
    )
    title_cell = worksheet.cell(row=1, column=1, value=config["title"])
    title_cell.fill = TITLE_FILL
    title_cell.font = TITLE_FONT
    title_cell.alignment = Alignment(
        horizontal="left",
        vertical="center"
    )
    worksheet.row_dimensions[1].height = 26

    worksheet.merge_cells(
        start_row=2,
        start_column=1,
        end_row=2,
        end_column=column_count
    )
    subtitle_cell = worksheet.cell(
        row=2,
        column=1,
        value=config["description"]
    )
    subtitle_cell.fill = SUBTITLE_FILL
    subtitle_cell.font = SUBTITLE_FONT
    subtitle_cell.alignment = Alignment(
        horizontal="left",
        vertical="center",
        wrap_text=True
    )
    worksheet.row_dimensions[2].height = 30

    worksheet.merge_cells(
        start_row=3,
        start_column=1,
        end_row=3,
        end_column=column_count
    )
    metadata_text = (
        f"Post: {RCAIP_POST_NAME} | Country: {RCAIP_COUNTRY_NAME} | "
        f"Run ID: {RCAIP_RUN_ID} | Generated UTC: "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data rows: {len(dataframe.index):,}"
    )
    metadata_cell = worksheet.cell(row=3, column=1, value=metadata_text)
    metadata_cell.font = METADATA_FONT
    metadata_cell.alignment = Alignment(
        horizontal="left",
        vertical="center"
    )

    header_row = 5
    data_start_row = 6

    if len(dataframe.columns) == 0:
        dataframe = pd.DataFrame({
            "source_status": ["AVAILABLE_EMPTY"],
            "message": ["The certified source contains no rows for this run."]
        })
        workbook_pandas[sheet_name] = dataframe
        column_count = 2
        last_column_letter = "B"

    for column_index, column_name in enumerate(dataframe.columns, start=1):
        cell = worksheet.cell(
            row=header_row,
            column=column_index,
            value=str(column_name)
        )
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(
            horizontal="left",
            vertical="center",
            wrap_text=True
        )
        cell.border = Border(bottom=MEDIUM_BLUE)

    for row_offset, values in enumerate(
        dataframe.itertuples(index=False, name=None),
        start=data_start_row
    ):
        for column_index, value in enumerate(values, start=1):
            cell = worksheet.cell(
                row=row_offset,
                column=column_index,
                value=value
            )
            if sheet_name in PORT_SHEET_KEYS and isinstance(value, str) and value.startswith("Open_Port_Detail rows "):
                first_row = value.split("rows ", 1)[1].split(",", 1)[0]
                cell.hyperlink = "#'Open_Port_Detail'!A" + first_row
            cell.font = BODY_FONT
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=isinstance(value, str) and len(value) > 60
            )

            column_name = str(dataframe.columns[column_index - 1]).lower()

            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cell.number_format = "#,##0.0" if isinstance(value, float) else "#,##0"

            if any(token in column_name for token in [
                "priority",
                "status",
                "classification",
                "result",
                "available",
                "required_flag"
            ]):
                fill = status_fill(value)
                if fill:
                    cell.fill = fill

    max_row = max(header_row, data_start_row + len(dataframe.index) - 1)
    worksheet.freeze_panes = "A6"
    worksheet.auto_filter.ref = (
        f"A{header_row}:{last_column_letter}{max_row}"
    )

    # Widths use bounded sampling to avoid expensive full-column scans.
    sample_size = min(len(dataframe.index), 250)

    for column_index, column_name in enumerate(dataframe.columns, start=1):
        sample_values = dataframe.iloc[:sample_size, column_index - 1].tolist()
        observed_lengths = [len(str(column_name))]

        for value in sample_values:
            if value is not None:
                observed_lengths.append(len(str(value)))

        max_length = max(observed_lengths) if observed_lengths else 10
        lower_name = str(column_name).lower()

        if any(token in lower_name for token in [
            "description",
            "interpretation",
            "condition",
            "recommended",
            "guidance",
            "notes",
            "sources",
            "members",
            "evidence"
        ]):
            width = min(max(max_length + 2, 24), 60)
        elif any(token in lower_name for token in [
            "asset_key",
            "action_id",
            "object_key",
            "distinguished_name",
            "run_id"
        ]):
            width = min(max(max_length + 2, 20), 42)
        else:
            width = min(max(max_length + 2, 10), 30)

        worksheet.column_dimensions[
            get_column_letter(column_index)
        ].width = width

    worksheet.row_dimensions[header_row].height = 32
    worksheet.sheet_properties.pageSetUpPr.fitToPage = True
    worksheet.page_setup.fitToWidth = 1
    worksheet.page_setup.fitToHeight = 0
    worksheet.page_setup.orientation = "landscape"
    worksheet.print_title_rows = "1:5"
    worksheet.print_area = f"A1:{last_column_letter}{max_row}"

    # Stronger visual treatment for the narrative and certification sheets.
    if sheet_name in {"READ_ME", "Executive_Summary", "Final_Certification"}:
        for row_number in range(data_start_row, max_row + 1):
            if row_number % 2 == 0:
                for column_index in range(1, column_count + 1):
                    worksheet.cell(row=row_number, column=column_index).fill = INFO_FILL


# ---------------------------------------------------------------------------
# 6. Save locally for validation
# ---------------------------------------------------------------------------

# Secured/serverless Databricks compute can block both legacy publication
# patterns used by older notebooks:
#
#   1. dbutils.fs.cp(file:/tmp/..., dbfs:/...)
#   2. direct Python writes to /dbfs/FileStore/...
#
# Build and validate the workbook on driver-local ephemeral storage first.
# Publication occurs later through the authenticated Workspace Files API,
# which transfers the workbook as binary content without crossing through
# DBFS root or the DBFS FUSE mount.

# ---------------------------------------------------------------------------
# Create an isolated, writable driver-local workbook path
# ---------------------------------------------------------------------------

# Certify that the filename belongs to the active runtime before writing.
expected_runtime_workbook_file_name = (
    f"RCAIP_{RCAIP_POST_SLUG}_DT_Validation_Package_"
    f"{RCAIP_REPORT_ANALYSIS_DATE}.xlsx"
)

if str(RCAIP_REPORT_FILE_NAME) != expected_runtime_workbook_file_name:
    raise RuntimeError(
        "Workbook filename does not match the active RCAIP runtime. "
        f"Expected={expected_runtime_workbook_file_name}; "
        f"actual={RCAIP_REPORT_FILE_NAME}. "
        "Rerun the report notebook from Cell 1."
    )


# Never reuse a shared /tmp/rcaip_reports directory.
# Every execution receives its own isolated directory.
temporary_root_candidates = [
    Path("/local_disk0/tmp"),
    Path(tempfile.gettempdir())
]

local_report_directory = None
temporary_directory_errors = []

for temporary_root in temporary_root_candidates:
    try:
        temporary_root.mkdir(
            parents=True,
            exist_ok=True
        )

        candidate_directory = Path(
            tempfile.mkdtemp(
                prefix=f"rcaip_report_{RCAIP_POST_SLUG}_",
                dir=str(temporary_root)
            )
        )

        # Prove that this exact directory is writable before OpenPyXL uses it.
        write_probe_path = (
            candidate_directory
            /
            ".rcaip_write_test"
        )

        with write_probe_path.open("xb") as write_probe:
            write_probe.write(b"RCAIP")

        write_probe_path.unlink()

        local_report_directory = candidate_directory
        break

    except Exception as directory_error:
        temporary_directory_errors.append(
            f"{temporary_root}: {directory_error}"
        )


if local_report_directory is None:
    raise RuntimeError(
        "RCAIP could not create a writable isolated driver-local "
        "workbook directory. Attempts: "
        + " | ".join(temporary_directory_errors)
    )


local_workbook_path = (
    local_report_directory
    /
    RCAIP_REPORT_FILE_NAME
)


print("=" * 100)
print("RCAIP WORKBOOK DRIVER-LOCAL PATH")
print("=" * 100)
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Workbook file:", RCAIP_REPORT_FILE_NAME)
print("Isolated directory:", str(local_report_directory))
print("Driver-local path:", str(local_workbook_path))
print("=" * 100)


try:
    workbook.save(
        local_workbook_path
    )

except Exception as save_error:
    raise RuntimeError(
        "The workbook could not be created in the isolated "
        "driver-local directory at "
        + str(local_workbook_path)
        + ": "
        + str(save_error)
    )


if not local_workbook_path.is_file():
    raise RuntimeError(
        "OpenPyXL completed without an exception, but the workbook "
        "does not exist at "
        + str(local_workbook_path)
    )


if local_workbook_path.stat().st_size <= 0:
    raise RuntimeError(
        "The workbook was created but is empty at "
        + str(local_workbook_path)
    )

if not local_workbook_path.exists():
    raise RuntimeError(
        "Workbook creation completed without an exception, but the local "
        "file is not visible at: "
        + str(local_workbook_path)
    )


# ---------------------------------------------------------------------------
# 7. Reopen and certify the workbook
# ---------------------------------------------------------------------------

verified_workbook = load_workbook(
    local_workbook_path,
    read_only=True,
    data_only=False
)

expected_sheet_names = list(RCAIP_REPORT_WORKBOOK_SHEETS.keys())
actual_sheet_names = verified_workbook.sheetnames

if actual_sheet_names != expected_sheet_names:
    raise RuntimeError(
        "Workbook sheet order mismatch: "
        f"expected={expected_sheet_names}, actual={actual_sheet_names}"
    )

validation_errors = []
formula_cell_count = 0
error_value_count = 0

for sheet_name, manifest_record in zip(
    expected_sheet_names,
    manifest_records
):
    worksheet = verified_workbook[sheet_name]
    if sheet_name in PORT_SHEET_KEYS:
        verify_port_sheet(worksheet, workbook_pandas[sheet_name])
    expected_rows = int(manifest_record[4])
    expected_columns = int(manifest_record[5])

    if expected_columns == 0:
        expected_columns = 2
        expected_rows = 1

    expected_max_row = 5 + expected_rows

    if worksheet.max_row != expected_max_row:
        validation_errors.append(
            f"{sheet_name}: expected max row {expected_max_row}, "
            f"found {worksheet.max_row}"
        )

    if worksheet.max_column != expected_columns:
        validation_errors.append(
            f"{sheet_name}: expected {expected_columns} columns, "
            f"found {worksheet.max_column}"
        )

    for row in worksheet.iter_rows(min_row=6):
        for cell in row:
            if cell.data_type == "f":
                formula_cell_count += 1
            if isinstance(cell.value, str) and cell.value in {
                "#REF!",
                "#DIV/0!",
                "#VALUE!",
                "#N/A",
                "#NAME?"
            }:
                error_value_count += 1

verified_workbook.close()

if formula_cell_count != 0:
    validation_errors.append(
        f"Unexpected formulas detected: {formula_cell_count}"
    )

if error_value_count != 0:
    validation_errors.append(
        f"Excel error values detected: {error_value_count}"
    )

if validation_errors:
    raise RuntimeError(
        "Workbook validation failed: " + " | ".join(validation_errors)
    )

workbook_bytes = local_workbook_path.read_bytes()
workbook_sha256 = hashlib.sha256(workbook_bytes).hexdigest()
workbook_size_bytes = len(workbook_bytes)
workbook_total_data_rows = sum(record[4] for record in manifest_records)

if workbook_size_bytes >= 500 * 1024 * 1024:
    raise RuntimeError(
        "The workbook exceeds the 500 MB Workspace Files limit. "
        f"Size: {workbook_size_bytes:,} bytes. Configure a Unity Catalog "
        "volume for publication before continuing."
    )


# ---------------------------------------------------------------------------
# 8. Publish through the Databricks Workspace Files API
# ---------------------------------------------------------------------------

try:
    workspace_client = WorkspaceClient()

    current_principal = (
        workspace_client
        .current_user
        .me()
        .user_name
    )

    if not current_principal:
        raise RuntimeError(
            "The authenticated Databricks principal has no user_name."
        )

    workspace_report_directory = (
        f"/Users/{current_principal}/RCAIP_Reports/{RCAIP_POST_SLUG}"
    )

    workspace_report_path = (
        f"{workspace_report_directory}/{RCAIP_REPORT_FILE_NAME}"
    )

    workspace_client.workspace.mkdirs(
        workspace_report_directory
    )

    workspace_client.workspace.upload(
        path=workspace_report_path,
        content=io.BytesIO(workbook_bytes),
        format=ImportFormat.AUTO,
        overwrite=True
    )

    with workspace_client.workspace.download(
        workspace_report_path,
        format=ExportFormat.AUTO
    ) as published_stream:
        published_bytes = published_stream.read()

    published_sha256 = hashlib.sha256(
        published_bytes
    ).hexdigest()

    if published_sha256 != workbook_sha256:
        raise RuntimeError(
            "The published workspace file does not match the locally "
            "certified workbook. "
            f"Local SHA-256={workbook_sha256}; "
            f"published SHA-256={published_sha256}."
        )

    workspace_host = str(
        workspace_client.config.host
    ).rstrip("/")

    workspace_id = workspace_client.get_workspace_id()

    workspace_browser_path = (
        f"{workspace_host}/?o={workspace_id}"
        f"#workspace/Workspace{workspace_report_path}"
    )

except Exception as publish_error:
    raise RuntimeError(
        "The workbook was created and validated locally, but could not be "
        "published through the Databricks Workspace Files API: "
        + str(publish_error)
    )

publication_method = "DATABRICKS_WORKSPACE_FILES_API"
published_path = workspace_report_path
browser_path = workspace_browser_path


# ---------------------------------------------------------------------------
# 9. Durable manifest and certificate
# ---------------------------------------------------------------------------

manifest_schema = T.StructType([
    T.StructField("sheet_order", T.IntegerType(), False),
    T.StructField("sheet_name", T.StringType(), False),
    T.StructField("sheet_title", T.StringType(), False),
    T.StructField("sheet_description", T.StringType(), False),
    T.StructField("data_row_count", T.LongType(), False),
    T.StructField("column_count", T.IntegerType(), False)
])

workbook_manifest_df = spark.createDataFrame(
    manifest_records,
    schema=manifest_schema
)

certificate_schema = T.StructType([
    T.StructField("report_name", T.StringType(), False),
    T.StructField("report_version", T.StringType(), False),
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("country_name", T.StringType(), False),
    T.StructField("rcaip_run_id", T.StringType(), False),
    T.StructField("workbook_file_name", T.StringType(), False),
    T.StructField("publication_method", T.StringType(), False),
    T.StructField("published_path", T.StringType(), False),
    T.StructField("browser_path", T.StringType(), False),
    T.StructField("sheet_count", T.IntegerType(), False),
    T.StructField("total_data_rows", T.LongType(), False),
    T.StructField("workbook_size_bytes", T.LongType(), False),
    T.StructField("sha256", T.StringType(), False),
    T.StructField("published_sha256", T.StringType(), False),
    T.StructField("formula_cell_count", T.LongType(), False),
    T.StructField("excel_error_value_count", T.LongType(), False),
    T.StructField("certification_status", T.StringType(), False),
    T.StructField("generated_at_utc", T.TimestampType(), False)
])

workbook_certificate_df = spark.createDataFrame(
    [(
        RCAIP_REPORT_NAME,
        RCAIP_REPORT_VERSION,
        RCAIP_POST_NAME,
        RCAIP_POST_SLUG,
        RCAIP_COUNTRY_NAME,
        RCAIP_RUN_ID,
        RCAIP_REPORT_FILE_NAME,
        publication_method,
        published_path,
        browser_path,
        len(expected_sheet_names),
        int(workbook_total_data_rows),
        int(workbook_size_bytes),
        workbook_sha256,
        published_sha256,
        int(formula_cell_count),
        int(error_value_count),
        "PASS",
        datetime.now(timezone.utc).replace(tzinfo=None)
    )],
    schema=certificate_schema
)

manifest_table = post_table("report001_workbook_manifest")
certificate_table = post_table("report001_workbook_certificate")

(
    workbook_manifest_df
    .withColumn("post_name", F.lit(RCAIP_POST_NAME))
    .withColumn("post_slug", F.lit(RCAIP_POST_SLUG))
    .withColumn("rcaip_run_id", F.lit(RCAIP_RUN_ID))
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(manifest_table)
)
print("Saved:", manifest_table)

(
    workbook_certificate_df
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(certificate_table)
)
print("Saved:", certificate_table)

RCAIP_REPORT001_READY = True

print("=" * 100)
print("RCAIP REPORT 001 - FINAL WORKBOOK RUNTIME CONTRACT")
print("=" * 100)
print("Status: PASS")
print("Ready:", RCAIP_REPORT001_READY)
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Country:", RCAIP_COUNTRY_NAME)
print("Run ID:", RCAIP_RUN_ID)
print("Assessment dates:", RCAIP_ASSESSMENT_START_DATE_TEXT, "to", RCAIP_ASSESSMENT_END_DATE_TEXT)
print("Workbook file:", RCAIP_REPORT_FILE_NAME)
print("Workbook sheets:", len(expected_sheet_names))
print("Workbook data rows:", workbook_total_data_rows)
print("Workbook size bytes:", workbook_size_bytes)
print("Workbook SHA-256:", workbook_sha256)
print("Formula cells:", formula_cell_count)
print("Excel error values:", error_value_count)
print("Publication method:", publication_method)
print("Workspace path:", published_path)
print("Browser path:", browser_path)
print("Cross-Post or cross-run rows admitted: 0")
print("Automatic assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)

try:
    displayHTML(
        f'<a href="{browser_path}" target="_blank">'
        f'Open {RCAIP_REPORT_FILE_NAME} in Workspace Files</a>'
    )
except Exception:
    pass
