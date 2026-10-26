# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 003 - Intune and MDE Control-Evidence Catalog
# Version: 0.1.0
# Mode: METADATA_ONLY_READ_ONLY
#
# Purpose:
#   Revisit the upstream Intune and MDE source schemas specifically for
#   presence, configuration, health, policy, reporting, identity, network,
#   and management evidence that the Asset Engine may not currently normalize.
#
# Design rules:
#   - Runtime-driven; no Post names, table names, or counts are hard-coded.
#   - Reads schema metadata only from upstream enterprise source tables.
#   - Reads current-run normalized schemas for comparison.
#   - Does not collect raw enterprise data rows.
#   - Does not change the Asset Engine, registry, rules, or durable products.
#
# Notebook structure:
#   Cell 1: %run ./RCAIP_Config_001_Table_Registry
#   Cell 2: this code
# ============================================================================

import re

from pyspark.sql import types as T

required_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "RCAIP_TABLES",
    "get_rcaip_table",
]

missing_objects = [name for name in required_objects if name not in globals()]
if missing_objects:
    raise RuntimeError(
        "Discovery 003 is missing Config 001 runtime objects: "
        + " | ".join(missing_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

resolved_runtime_sources = globals().get("RCAIP_RESOLVED_SOURCE_TABLES", {}) or {}

source_specs = [
    {
        "source_name": "INTUNE",
        "raw_table": (
            resolved_runtime_sources.get("intune_devices")
            or globals().get("RCAIP_INTUNE_TABLE")
        ),
        "normalized_logical_name": "INTUNE_NORM",
    },
    {
        "source_name": "MDE",
        "raw_table": (
            resolved_runtime_sources.get("mde_device_info")
            or globals().get("RCAIP_MDE_TABLE")
        ),
        "normalized_logical_name": "MDE_NORM",
    },
]

# The categories deliberately overlap. One source field may provide more than
# one kind of evidence and should be preserved in every relevant category.
candidate_patterns = {
    "CONFIGURATION_OR_COMPLIANCE": [
        r"compliance", r"compliant", r"configuration", r"config_?state",
        r"policy_?state", r"policy_?status", r"baseline", r"secure_?score",
        r"encryption", r"bitlocker", r"firewall", r"tamper", r"antivirus",
        r"anti_?malware", r"defender_?status", r"protection_?status",
    ],
    "HEALTH_OR_ONBOARDING": [
        r"health", r"sensor", r"onboard", r"managed_?status",
        r"management_?state", r"agent_?status", r"client_?status",
        r"risk_?level", r"exposure", r"threat", r"remediation",
    ],
    "REPORTING_OR_FRESHNESS": [
        r"last_?seen", r"last_?contact", r"last_?sync", r"last_?check",
        r"check_?in", r"heartbeat", r"timestamp", r"event_?time",
        r"updated", r"modified", r"reporting", r"report_?time",
    ],
    "IDENTITY_OR_JOIN": [
        r"device_?name", r"hostname", r"computer_?name", r"machine_?id",
        r"device_?id", r"azure.*device", r"aad.*device", r"serial",
        r"host_?key", r"machine_?group",
    ],
    "NETWORK_OR_BOUNDARY": [
        r"public_?ip", r"private_?ip", r"ip_?address", r"network",
        r"subnet", r"domain", r"location", r"site", r"tenant",
    ],
    "OWNERSHIP_OR_MANAGEMENT": [
        r"owner", r"managed_?by", r"management", r"machine_?group",
        r"tag", r"group", r"category", r"device_?type", r"operating_?system",
    ],
}

catalog_rows = []
source_status_rows = []

for spec in source_specs:
    source_name = spec["source_name"]
    raw_table = spec["raw_table"]
    normalized_logical_name = spec["normalized_logical_name"]

    normalized_table = (
        get_rcaip_table(normalized_logical_name)
        if normalized_logical_name in RCAIP_TABLES
        else None
    )

    normalized_columns = set()
    normalized_status = "NOT_REGISTERED"

    if normalized_table:
        if spark.catalog.tableExists(normalized_table):
            normalized_columns = set(spark.table(normalized_table).columns)
            normalized_status = "AVAILABLE"
        else:
            normalized_status = "REGISTERED_NOT_AVAILABLE"

    raw_status = "NOT_RESOLVED"
    raw_column_count = 0
    candidate_row_count = 0

    if raw_table and spark.catalog.tableExists(raw_table):
        raw_status = "AVAILABLE"
        raw_schema = spark.table(raw_table).schema
        raw_column_count = len(raw_schema.fields)

        for field in raw_schema.fields:
            normalized_field_name = re.sub(
                r"[^a-z0-9]+", "_", str(field.name).strip().lower()
            ).strip("_")

            matched_categories = []
            matched_patterns = []

            for category, patterns in candidate_patterns.items():
                category_matches = [
                    pattern
                    for pattern in patterns
                    if re.search(pattern, normalized_field_name, flags=re.IGNORECASE)
                ]
                if category_matches:
                    matched_categories.append(category)
                    matched_patterns.extend(category_matches)

            if not matched_categories:
                continue

            candidate_row_count += 1
            catalog_rows.append((
                source_name,
                str(raw_table),
                str(field.name),
                field.dataType.simpleString(),
                "|".join(sorted(set(matched_categories))),
                "|".join(sorted(set(matched_patterns))),
                normalized_logical_name,
                str(normalized_table) if normalized_table else None,
                normalized_status,
                (
                    "EXACT_NAME_PRESENT_IN_NORMALIZED_CONTRACT"
                    if str(field.name) in normalized_columns
                    or normalized_field_name in {
                        re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
                        for name in normalized_columns
                    }
                    else "NOT_EXACTLY_PRESENT_REQUIRES_MAPPING_REVIEW"
                ),
            ))

    elif raw_table:
        raw_status = "RESOLVED_NOT_AVAILABLE"

    source_status_rows.append((
        source_name,
        str(raw_table) if raw_table else None,
        raw_status,
        int(raw_column_count),
        int(candidate_row_count),
        normalized_logical_name,
        str(normalized_table) if normalized_table else None,
        normalized_status,
        int(len(normalized_columns)),
    ))

catalog_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), False),
    T.StructField("raw_field_name", T.StringType(), False),
    T.StructField("raw_field_type", T.StringType(), False),
    T.StructField("candidate_categories", T.StringType(), False),
    T.StructField("matched_patterns", T.StringType(), False),
    T.StructField("normalized_logical_name", T.StringType(), False),
    T.StructField("normalized_table", T.StringType(), True),
    T.StructField("normalized_status", T.StringType(), False),
    T.StructField("normalization_mapping_status", T.StringType(), False),
])

status_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), True),
    T.StructField("raw_source_status", T.StringType(), False),
    T.StructField("raw_column_count", T.IntegerType(), False),
    T.StructField("candidate_field_count", T.IntegerType(), False),
    T.StructField("normalized_logical_name", T.StringType(), False),
    T.StructField("normalized_table", T.StringType(), True),
    T.StructField("normalized_status", T.StringType(), False),
    T.StructField("normalized_column_count", T.IntegerType(), False),
])

catalog_df = spark.createDataFrame(catalog_rows, schema=catalog_schema)
status_df = spark.createDataFrame(source_status_rows, schema=status_schema)

print("Upstream and normalized source resolution")
display(status_df.orderBy("source_name"))

print("Candidate Intune and MDE control-evidence fields in upstream schemas")
display(
    catalog_df.orderBy(
        "source_name",
        "candidate_categories",
        "raw_field_name",
    )
)

print("Candidate configuration, compliance, health, and onboarding fields requiring review")
display(
    catalog_df.filter(
        catalog_df.candidate_categories.contains("CONFIGURATION_OR_COMPLIANCE")
        | catalog_df.candidate_categories.contains("HEALTH_OR_ONBOARDING")
    ).orderBy("source_name", "raw_field_name")
)

available_raw_sources = sum(
    1 for row in source_status_rows if row[2] == "AVAILABLE"
)
total_candidate_fields = sum(int(row[4]) for row in source_status_rows)

print("=" * 100)
print("RCAIP DISCOVERY 003 INTUNE AND MDE CONTROL-EVIDENCE CATALOG CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Mode: METADATA_ONLY_READ_ONLY")
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Source specifications:", len(source_specs))
print("Available upstream source schemas:", available_raw_sources)
print("Candidate raw fields identified:", total_candidate_fields)
print("Raw enterprise data rows collected: 0")
print("Delta tables written: 0")
print("Existing normalizers changed: 0")
print("Existing rules changed: 0")
print(
    "Next gate: review candidate configuration/health fields, then approve a "
    "bounded Post-scoped value profile before changing INTUNE_NORM or MDE_NORM."
)
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 003 - Candidate Control-Evidence Field Review
# Cell 3 - Metadata-only prioritization of Intune and MDE candidates
#
# Prerequisite:
#   Run Cell 1 (%run Config 001) and Discovery 003 Cell 2 first so catalog_df
#   and status_df exist in the current notebook session.
#
# Safety:
#   - Reads catalog metadata only.
#   - Reads zero raw enterprise rows.
#   - Writes no tables or files.
#   - Makes no changes to normalizers or rules.
# ============================================================================

from pyspark.sql import functions as F

required_objects = ["catalog_df", "status_df"]
missing_objects = [name for name in required_objects if name not in globals()]
if missing_objects:
    raise RuntimeError(
        "Discovery 003 candidate review requires Cell 2 objects: "
        + " | ".join(missing_objects)
    )

priority_1_pattern = (
    r"compliance|compliant|onboard|sensor|health|antivirus|anti_?malware|"
    r"firewall|encryption|bitlocker|tamper|defender|protection|risk|exposure"
)
priority_2_pattern = (
    r"policy|configuration|config|managed_?status|management_?state|"
    r"agent_?status|client_?status|secure_?score|threat|remediation"
)

candidate_review_df = (
    catalog_df
    .filter(
        F.col("candidate_categories").contains("CONFIGURATION_OR_COMPLIANCE")
        | F.col("candidate_categories").contains("HEALTH_OR_ONBOARDING")
    )
    .withColumn("field_name_normalized", F.lower(F.col("raw_field_name")))
    .withColumn(
        "review_priority",
        F.when(F.col("field_name_normalized").rlike(priority_1_pattern), "P1")
        .when(F.col("field_name_normalized").rlike(priority_2_pattern), "P2")
        .otherwise("P3"),
    )
    .withColumn(
        "recommended_profile_method",
        F.when(
            F.lower(F.col("raw_field_type")).rlike(
                r"^(boolean|bool|tinyint|smallint|int|bigint|float|double|decimal|string)"
            ),
            "BOUNDED_VALUE_DISTRIBUTION",
        )
        .when(
            F.lower(F.col("raw_field_type")).rlike(r"timestamp|date"),
            "FRESHNESS_OR_RANGE_PROFILE",
        )
        .when(
            F.lower(F.col("raw_field_type")).rlike(r"array|map|struct"),
            "SCHEMA_AND_BOUNDED_SHAPE_REVIEW",
        )
        .otherwise("MANUAL_TYPE_REVIEW"),
    )
    .withColumn(
        "normalizer_action_candidate",
        F.when(
            F.col("normalization_mapping_status") == "EXACT_NAME_PRESENT_IN_NORMALIZED_CONTRACT",
            "ALREADY_PRESENT_VERIFY_SEMANTICS",
        ).otherwise("NOT_NORMALIZED_MAPPING_REQUIRED"),
    )
    .select(
        "review_priority",
        "source_name",
        "raw_source_table",
        "raw_field_name",
        "raw_field_type",
        "candidate_categories",
        "matched_patterns",
        "recommended_profile_method",
        "normalizer_action_candidate",
        "normalized_logical_name",
        "normalized_table",
        "normalization_mapping_status",
    )
)

candidate_review_summary_df = (
    candidate_review_df
    .groupBy(
        "source_name",
        "review_priority",
        "recommended_profile_method",
        "normalizer_action_candidate",
    )
    .agg(F.count(F.lit(1)).cast("long").alias("candidate_field_count"))
    .orderBy(
        "source_name",
        "review_priority",
        "recommended_profile_method",
        "normalizer_action_candidate",
    )
)

print("Discovery 003 candidate field review summary")
display(candidate_review_summary_df)

print("P1 candidate configuration, compliance, health, and onboarding fields")
display(
    candidate_review_df
    .filter(F.col("review_priority") == "P1")
    .orderBy("source_name", "raw_field_name")
)

print("P2 candidate fields")
display(
    candidate_review_df
    .filter(F.col("review_priority") == "P2")
    .orderBy("source_name", "raw_field_name")
)

print("All candidate configuration/health fields for export and review")
display(
    candidate_review_df.orderBy(
        "review_priority",
        "source_name",
        "raw_field_name",
    )
)

candidate_review_count = candidate_review_df.count()
p1_count = candidate_review_df.filter(F.col("review_priority") == "P1").count()
p2_count = candidate_review_df.filter(F.col("review_priority") == "P2").count()
p3_count = candidate_review_df.filter(F.col("review_priority") == "P3").count()

print("=" * 100)
print("RCAIP DISCOVERY 003 CANDIDATE FIELD REVIEW CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Mode: METADATA_ONLY_READ_ONLY")
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Configuration/health candidate fields:", candidate_review_count)
print("P1 fields:", p1_count)
print("P2 fields:", p2_count)
print("P3 fields:", p3_count)
print("Raw enterprise data rows collected: 0")
print("Delta tables written: 0")
print("Normalizers changed: 0")
print("Rules changed: 0")
print(
    "Next gate: export the P1 and P2 candidate displays, approve exact fields and "
    "Post-scoping joins, then run a bounded value profile."
)
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 003 - Bounded P1 Intune/MDE Value Profile
# Version: 0.2.0-EFFICIENT
# Mode: READ_ONLY_POST_SCOPED_VALUE_PROFILE
#
# Purpose:
#   Profile only the P1 configuration/health fields identified by Discovery
#   003, using the current Post's normalized Intune/MDE identities as the
#   bounding population.
#
# Design rules:
#   - Runtime-driven; no Post names, table names, field names, or counts are
#     hard-coded.
#   - Uses candidate_review_df from Discovery 003 Cell 3.
#   - Reads only identity and P1 fields from resolved upstream sources.
#   - Uses one equality-key scan per source; no OR/nested-loop raw join.
#   - Persists the bounded Post subset until every profile action finishes.
#   - Bounds upstream rows through exact normalized device-name and/or serial
#     matches from the current Post/run normalized source products.
#   - Refuses to profile a source when no safe identity join can be resolved.
#   - Does not change normalizers, rules, the Asset Plane, or durable products.
#
# Prerequisites:
#   Cell 1: %run ./RCAIP_Config_001_Table_Registry
#   Cell 2: Discovery 003 control-evidence catalog
#   Cell 3: Discovery 003 candidate field review
# ============================================================================

from functools import reduce
import operator
import re

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# 1. Validate the in-memory Discovery 003 and runtime contracts
# ---------------------------------------------------------------------------

required_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "candidate_review_df",
    "catalog_df",
    "status_df",
]

missing_objects = [name for name in required_objects if name not in globals()]
if missing_objects:
    raise RuntimeError(
        "Discovery 003 bounded P1 profile is missing required objects: "
        + " | ".join(missing_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")


# ---------------------------------------------------------------------------
# 2. Small metadata collections only - no upstream enterprise rows collected
# ---------------------------------------------------------------------------

p1_metadata = [
    row.asDict(recursive=True)
    for row in (
        candidate_review_df
        .filter(F.col("review_priority") == "P1")
        .orderBy("source_name", "raw_field_name")
        .collect()
    )
]

if not p1_metadata:
    raise RuntimeError("Discovery 003 produced no P1 fields to profile.")

status_metadata = {
    row["source_name"]: row.asDict(recursive=True)
    for row in status_df.collect()
}


# ---------------------------------------------------------------------------
# 3. Reusable helpers
# ---------------------------------------------------------------------------

def quoted_col(column_name, alias=None):
    escaped = str(column_name).replace("`", "``")
    if alias:
        return F.col(f"{alias}.`{escaped}`")
    return F.col(f"`{escaped}`")


def normalized_text(column_expression):
    return F.upper(F.trim(column_expression.cast("string")))


def normalized_field_token(field_name):
    return re.sub(r"[^a-z0-9]+", "", str(field_name).lower())


def matching_columns(columns, exact_tokens=None, regex_tokens=None):
    exact_tokens = {
        normalized_field_token(value)
        for value in (exact_tokens or [])
    }
    regex_tokens = list(regex_tokens or [])

    matches = []
    for column_name in columns:
        token = normalized_field_token(column_name)
        if token in exact_tokens:
            matches.append(column_name)
            continue
        if any(re.search(pattern, token, flags=re.IGNORECASE) for pattern in regex_tokens):
            matches.append(column_name)

    # Preserve schema order while removing duplicates.
    return list(dict.fromkeys(matches))


def union_all(frames):
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right), frames)


def nonblank_string(column_expression):
    return (
        column_expression.isNotNull()
        & (F.length(F.trim(column_expression.cast("string"))) > 0)
    )


def make_scope_frame(normalized_df, source_name, column_name, join_type, confidence):
    host_key_expression = (
        quoted_col("host_key")
        if "host_key" in normalized_df.columns
        else F.lit(None).cast("string")
    )

    return (
        normalized_df
        .filter(nonblank_string(quoted_col(column_name)))
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(join_type).alias("join_type"),
            normalized_text(quoted_col(column_name)).alias("join_value"),
            F.coalesce(
                host_key_expression.cast("string"),
                quoted_col(column_name).cast("string"),
            ).alias("scope_host_key"),
            F.lit(column_name).alias("normalized_join_field"),
            F.lit(confidence).alias("scope_match_confidence"),
        )
        .dropDuplicates(["source_name", "join_type", "join_value", "scope_host_key"])
    )


def profile_scalar_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    value_expression = (
        F.when(quoted_col(field_name).isNull(), F.lit("<NULL>"))
        .when(
            F.length(F.trim(quoted_col(field_name).cast("string"))) == 0,
            F.lit("<BLANK>"),
        )
        .otherwise(quoted_col(field_name).cast("string"))
    )

    grouped = (
        scoped_df
        .withColumn("profile_value", value_expression)
        .groupBy("profile_value")
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("scoped_record_count"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identity_count"),
            F.max("scope_match_confidence").alias("highest_scope_match_confidence"),
        )
        .withColumn("source_name", F.lit(source_name))
        .withColumn("raw_source_table", F.lit(raw_table))
        .withColumn("raw_field_name", F.lit(field_name))
        .withColumn("raw_field_type", F.lit(field_type))
        .withColumn("profile_method", F.lit("BOUNDED_VALUE_DISTRIBUTION"))
        .withColumn("scope_method", F.lit(scope_method))
        .withColumn("total_scoped_source_records", F.lit(int(total_scoped_records)).cast("long"))
        .withColumn(
            "percent_of_scoped_records",
            F.round(
                F.col("scoped_record_count")
                / F.when(
                    F.col("total_scoped_source_records") > 0,
                    F.col("total_scoped_source_records"),
                ).otherwise(F.lit(None).cast("long"))
                * F.lit(100.0),
                2,
            ),
        )
    )

    ranking_window = Window.orderBy(
        F.desc("scoped_record_count"),
        F.asc("profile_value"),
    )

    return (
        grouped
        .withColumn("value_rank", F.row_number().over(ranking_window))
        .filter(F.col("value_rank") <= 100)
        .select(
            "source_name",
            "raw_source_table",
            "raw_field_name",
            "raw_field_type",
            "profile_method",
            "scope_method",
            "profile_value",
            "scoped_record_count",
            "distinct_post_identity_count",
            "highest_scope_match_confidence",
            "total_scoped_source_records",
            "percent_of_scoped_records",
            "value_rank",
        )
    )


def profile_temporal_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    timestamp_expression = quoted_col(field_name).cast("timestamp")
    age_days_expression = F.datediff(F.current_timestamp(), timestamp_expression)

    bucket_expression = (
        F.when(timestamp_expression.isNull(), "<NULL_OR_UNPARSEABLE>")
        .when(age_days_expression < 0, "FUTURE_TIMESTAMP")
        .when(age_days_expression <= 1, "AGE_LE_1_DAY")
        .when(age_days_expression <= 3, "AGE_LE_3_DAYS")
        .when(age_days_expression <= 7, "AGE_LE_7_DAYS")
        .when(age_days_expression <= 14, "AGE_LE_14_DAYS")
        .when(age_days_expression <= 30, "AGE_LE_30_DAYS")
        .otherwise("AGE_GT_30_DAYS")
    )

    grouped = (
        scoped_df
        .withColumn("profile_value", bucket_expression)
        .groupBy("profile_value")
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("scoped_record_count"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identity_count"),
            F.max("scope_match_confidence").alias("highest_scope_match_confidence"),
        )
        .withColumn("source_name", F.lit(source_name))
        .withColumn("raw_source_table", F.lit(raw_table))
        .withColumn("raw_field_name", F.lit(field_name))
        .withColumn("raw_field_type", F.lit(field_type))
        .withColumn("profile_method", F.lit("FRESHNESS_OR_RANGE_PROFILE"))
        .withColumn("scope_method", F.lit(scope_method))
        .withColumn("total_scoped_source_records", F.lit(int(total_scoped_records)).cast("long"))
        .withColumn(
            "percent_of_scoped_records",
            F.round(
                F.col("scoped_record_count")
                / F.when(
                    F.col("total_scoped_source_records") > 0,
                    F.col("total_scoped_source_records"),
                ).otherwise(F.lit(None).cast("long"))
                * F.lit(100.0),
                2,
            ),
        )
    )

    ranking_window = Window.orderBy(
        F.desc("scoped_record_count"),
        F.asc("profile_value"),
    )

    return (
        grouped
        .withColumn("value_rank", F.row_number().over(ranking_window))
        .select(
            "source_name",
            "raw_source_table",
            "raw_field_name",
            "raw_field_type",
            "profile_method",
            "scope_method",
            "profile_value",
            "scoped_record_count",
            "distinct_post_identity_count",
            "highest_scope_match_confidence",
            "total_scoped_source_records",
            "percent_of_scoped_records",
            "value_rank",
        )
    )


def profile_complex_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    profile_value_expression = F.when(
        quoted_col(field_name).isNull(),
        F.lit("<NULL>"),
    ).otherwise(F.lit("<NON_NULL_COMPLEX_VALUE>"))

    return profile_scalar_field(
        scoped_df.withColumn(field_name, profile_value_expression),
        source_name,
        raw_table,
        field_name,
        field_type,
        scope_method,
        total_scoped_records,
    ).withColumn("profile_method", F.lit("SCHEMA_AND_BOUNDED_SHAPE_REVIEW"))


# ---------------------------------------------------------------------------
# 4. Build a safe Post-scoped identity bridge for each source and profile P1
# ---------------------------------------------------------------------------

metadata_by_source = {}
for row in p1_metadata:
    metadata_by_source.setdefault(row["source_name"], []).append(row)

profile_frames = []
scope_status_rows = []
profiled_field_count = 0
persisted_scoped_dfs = []

for source_name in sorted(metadata_by_source.keys()):
    source_status = status_metadata.get(source_name)
    if not source_status:
        raise RuntimeError(f"No Discovery 003 source status exists for {source_name}.")

    raw_table = source_status.get("raw_source_table")
    normalized_table = source_status.get("normalized_table")

    if source_status.get("raw_source_status") != "AVAILABLE":
        raise RuntimeError(f"Upstream {source_name} source is not available: {raw_table}")
    if source_status.get("normalized_status") != "AVAILABLE":
        raise RuntimeError(
            f"Post-scoped normalized {source_name} source is not available: "
            f"{normalized_table}"
        )

    raw_df = spark.table(raw_table)
    normalized_df = spark.table(normalized_table)

    if "rcaip_run_id" in normalized_df.columns:
        normalized_df = normalized_df.filter(F.col("rcaip_run_id") == RCAIP_RUN_ID)

    normalized_name_exact = {
        "INTUNE": ["intune_device_name", "canonical_name"],
        "MDE": ["mde_device_name", "canonical_name"],
    }.get(source_name, ["canonical_name"])

    normalized_serial_exact = {
        "INTUNE": ["intune_serial_number", "canonical_serial"],
        "MDE": ["canonical_serial"],
    }.get(source_name, ["canonical_serial"])

    raw_name_exact = [
        "device_name", "devicename", "hostname", "computer_name",
        "computername", "machine_name", "machinename",
        "properties_device_name", "properties_devicename",
    ]
    raw_name_patterns = [
        r"devicename$", r"hostname$", r"computername$", r"machinename$",
    ]

    raw_serial_exact = [
        "serial_number", "serialnumber", "device_serial_number",
        "properties_serial_number", "properties_serialnumber",
    ]
    raw_serial_patterns = [r"serialnumber$", r"serial$"]

    normalized_name_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_name_exact,
    )
    normalized_serial_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_serial_exact,
    )
    raw_name_columns = matching_columns(
        raw_df.columns,
        exact_tokens=raw_name_exact,
        regex_tokens=raw_name_patterns,
    )
    raw_serial_columns = matching_columns(
        raw_df.columns,
        exact_tokens=raw_serial_exact,
        regex_tokens=raw_serial_patterns,
    )

    scope_frames = []
    for column_name in normalized_serial_columns:
        scope_frames.append(
            make_scope_frame(
                normalized_df,
                source_name,
                column_name,
                "SERIAL",
                "HIGH_EXACT_SERIAL",
            )
        )
    for column_name in normalized_name_columns:
        scope_frames.append(
            make_scope_frame(
                normalized_df,
                source_name,
                column_name,
                "DEVICE_NAME",
                "MODERATE_EXACT_DEVICE_NAME",
            )
        )

    scope_keys_df = union_all(scope_frames)
    if scope_keys_df is None:
        raise RuntimeError(
            f"No safe normalized identity fields were resolved for {source_name}."
        )

    scope_key_count = scope_keys_df.count()
    if scope_key_count == 0:
        raise RuntimeError(
            f"No current-run normalized identity values were available for {source_name}."
        )
    if scope_key_count > 50000:
        raise RuntimeError(
            f"The {source_name} Post scope unexpectedly contains {scope_key_count} "
            "identity keys; review the normalized source before profiling."
        )

    # Persist the small Post-scoped key set once. It is reused for the bounded
    # equality join and must never be recomputed against the normalized table
    # during each downstream profile.
    scope_keys_df = scope_keys_df.persist(StorageLevel.MEMORY_AND_DISK)

    # Build one compact equality-key representation of the raw source. The
    # prior OR-based join could force a broadcast nested-loop comparison across
    # the enterprise source. Exploding raw identity keys converts the operation
    # into a broadcast hash join on (_join_type, _join_value).
    raw_id_columns = matching_columns(
        raw_df.columns,
        exact_tokens=[
            "device_id", "deviceid", "managed_device_id", "manageddeviceid",
            "machine_id", "machineid", "aad_device_id", "aaddeviceid",
            "azure_ad_device_id", "azureaddeviceid",
        ],
        regex_tokens=[
            r"manageddeviceid$", r"machineid$", r"aaddeviceid$", r"azureaddeviceid$",
        ],
    )

    approved_profile_fields = [
        row["raw_field_name"]
        for row in metadata_by_source[source_name]
    ]

    # Read only fields needed for identity scoping, record identity, and the
    # approved P1 profiles. Do not carry every raw enterprise column through
    # the join or into the persisted Post-scoped subset.
    required_raw_columns = list(dict.fromkeys(
        raw_id_columns
        + raw_serial_columns
        + raw_name_columns
        + approved_profile_fields
    ))

    missing_required_raw_columns = [
        column_name
        for column_name in required_raw_columns
        if column_name not in raw_df.columns
    ]
    if missing_required_raw_columns:
        raise RuntimeError(
            f"Required upstream fields are absent from {raw_table}: "
            + " | ".join(missing_required_raw_columns)
        )

    raw_minimal_df = raw_df.select(
        *[
            quoted_col(column_name).alias(column_name)
            for column_name in required_raw_columns
        ]
    )

    key_candidates = []
    for column_name in raw_id_columns + raw_serial_columns + raw_name_columns:
        key_candidates.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.concat(
                    F.lit(f"{column_name}="),
                    normalized_text(quoted_col(column_name)),
                ),
            )
        )

    if key_candidates:
        raw_record_key_expression = F.coalesce(*key_candidates)
    else:
        raise RuntimeError(
            f"No stable upstream record-key candidates were resolved for {source_name}."
        )

    raw_join_key_expressions = []
    for column_name in raw_serial_columns:
        raw_join_key_expressions.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.struct(
                    F.lit("SERIAL").alias("join_type"),
                    normalized_text(quoted_col(column_name)).alias("join_value"),
                    F.lit(column_name).alias("raw_join_field"),
                ),
            )
        )
    for column_name in raw_name_columns:
        raw_join_key_expressions.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.struct(
                    F.lit("DEVICE_NAME").alias("join_type"),
                    normalized_text(quoted_col(column_name)).alias("join_value"),
                    F.lit(column_name).alias("raw_join_field"),
                ),
            )
        )

    if not raw_join_key_expressions:
        raise RuntimeError(
            f"No safe upstream identity join fields were resolved for {source_name}. "
            "Review the Discovery 003 identity candidates before profiling values."
        )

    raw_keyed_df = (
        raw_minimal_df
        .withColumn("_raw_record_key", raw_record_key_expression)
        .filter(F.col("_raw_record_key").isNotNull())
        .withColumn(
            "_join_key",
            F.explode(F.array(*raw_join_key_expressions)),
        )
        .filter(F.col("_join_key").isNotNull())
        .select(
            *[quoted_col(column_name) for column_name in required_raw_columns],
            "_raw_record_key",
            F.col("_join_key.join_type").alias("_join_type"),
            F.col("_join_key.join_value").alias("_join_value"),
            F.col("_join_key.raw_join_field").alias("raw_join_field"),
        )
    )

    scope_join_df = (
        scope_keys_df
        .select(
            F.col("join_type").alias("_join_type"),
            F.col("join_value").alias("_join_value"),
            "scope_host_key",
            "normalized_join_field",
            "scope_match_confidence",
        )
        .dropDuplicates([
            "_join_type",
            "_join_value",
            "scope_host_key",
        ])
    )

    scoped_df = (
        raw_keyed_df
        .join(
            F.broadcast(scope_join_df),
            ["_join_type", "_join_value"],
            "inner",
        )
        .select(
            *[quoted_col(column_name) for column_name in required_raw_columns],
            "_raw_record_key",
            "scope_host_key",
            F.col("_join_type").alias("scope_join_type"),
            "normalized_join_field",
            "scope_match_confidence",
            "raw_join_field",
        )
        .dropDuplicates(["_raw_record_key", "scope_host_key"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # One action materializes the expensive raw-source join and calculates both
    # metrics. Every field profile below reads only this compact persisted
    # Post-scoped subset.
    scope_metrics = (
        scoped_df
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("total_scoped_records"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identities"),
        )
        .first()
    )

    total_scoped_records = int(scope_metrics["total_scoped_records"] or 0)
    distinct_post_identities = int(scope_metrics["distinct_post_identities"] or 0)

    # The expensive join has now been materialized, so the small key DataFrame
    # can be released. Keep scoped_df persisted until all value profiles and
    # displays have completed.
    scope_keys_df.unpersist(blocking=False)
    persisted_scoped_dfs.append(scoped_df)

    scope_method_parts = []
    if raw_serial_columns and normalized_serial_columns:
        scope_method_parts.append("EXACT_SERIAL")
    if raw_name_columns and normalized_name_columns:
        scope_method_parts.append("EXACT_DEVICE_NAME")
    scope_method = "+".join(scope_method_parts)

    scope_status_rows.append((
        source_name,
        str(raw_table),
        str(normalized_table),
        "|".join(normalized_serial_columns) if normalized_serial_columns else "NONE",
        "|".join(normalized_name_columns) if normalized_name_columns else "NONE",
        "|".join(raw_serial_columns) if raw_serial_columns else "NONE",
        "|".join(raw_name_columns) if raw_name_columns else "NONE",
        scope_method,
        int(scope_key_count),
        int(total_scoped_records),
        int(distinct_post_identities),
        "PASS",
    ))

    for field_metadata in metadata_by_source[source_name]:
        field_name = field_metadata["raw_field_name"]
        field_type = str(field_metadata["raw_field_type"]).lower()
        requested_method = field_metadata["recommended_profile_method"]

        if field_name not in scoped_df.columns:
            raise RuntimeError(
                f"Approved P1 field {source_name}.{field_name} is absent from "
                f"the current upstream schema {raw_table}."
            )

        if requested_method == "FRESHNESS_OR_RANGE_PROFILE" or re.search(
            r"timestamp|date", field_type
        ):
            frame = profile_temporal_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )
        elif requested_method == "SCHEMA_AND_BOUNDED_SHAPE_REVIEW" or re.search(
            r"array|map|struct", field_type
        ):
            frame = profile_complex_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )
        else:
            frame = profile_scalar_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )

        profile_frames.append(frame)
        profiled_field_count += 1


# ---------------------------------------------------------------------------
# 5. Publish read-only review outputs
# ---------------------------------------------------------------------------

scope_status_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), False),
    T.StructField("normalized_table", T.StringType(), False),
    T.StructField("normalized_serial_fields", T.StringType(), False),
    T.StructField("normalized_name_fields", T.StringType(), False),
    T.StructField("raw_serial_fields", T.StringType(), False),
    T.StructField("raw_name_fields", T.StringType(), False),
    T.StructField("scope_method", T.StringType(), False),
    T.StructField("scope_key_count", T.IntegerType(), False),
    T.StructField("scoped_raw_record_count", T.IntegerType(), False),
    T.StructField("distinct_post_identity_count", T.IntegerType(), False),
    T.StructField("scope_status", T.StringType(), False),
])

scope_status_df = spark.createDataFrame(scope_status_rows, schema=scope_status_schema)

p1_profile_df = union_all(profile_frames)
if p1_profile_df is None:
    raise RuntimeError("No P1 value profiles were produced.")

# Materialize the compact profile output once. The summary, detailed display,
# and certification count then reuse the cached result instead of replaying the
# per-field profile lineage.
p1_profile_df = p1_profile_df.persist(StorageLevel.MEMORY_AND_DISK)
profile_output_row_count = p1_profile_df.count()

p1_profile_summary_df = (
    p1_profile_df
    .groupBy(
        "source_name",
        "raw_field_name",
        "raw_field_type",
        "profile_method",
        "scope_method",
        "total_scoped_source_records",
    )
    .agg(
        F.count(F.lit(1)).cast("long").alias("profile_value_count_displayed"),
        F.sum("scoped_record_count").cast("long").alias("sum_profiled_record_counts"),
        F.max("distinct_post_identity_count").cast("long").alias("max_distinct_post_identity_count"),
    )
    .orderBy("source_name", "raw_field_name")
)

print("Approved P1 metadata fields included in the bounded profile")
display(
    candidate_review_df
    .filter(F.col("review_priority") == "P1")
    .orderBy("source_name", "raw_field_name")
)

print("Post-scoping identity bridge certification")
display(scope_status_df.orderBy("source_name"))

print("Bounded P1 profile summary")
display(p1_profile_summary_df)

print("Bounded P1 value distributions - top 100 values per field")
display(
    p1_profile_df.orderBy(
        "source_name",
        "raw_field_name",
        "value_rank",
        "profile_value",
    )
)

requested_p1_field_count = len(p1_metadata)
source_count = len(metadata_by_source)

if profiled_field_count != requested_p1_field_count:
    raise RuntimeError(
        "P1 field profile reconciliation failed. Requested="
        f"{requested_p1_field_count}; profiled={profiled_field_count}"
    )

# All actions using the persisted Post-scoped subsets and profile output are
# complete. Release them only now; never before the profile actions execute.
p1_profile_df.unpersist(blocking=False)
for persisted_df in persisted_scoped_dfs:
    persisted_df.unpersist(blocking=False)

print("=" * 100)
print("RCAIP DISCOVERY 003 BOUNDED P1 VALUE PROFILE CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Mode: READ_ONLY_POST_SCOPED_VALUE_PROFILE")
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("P1 candidate fields requested:", requested_p1_field_count)
print("P1 candidate fields profiled:", profiled_field_count)
print("Sources profiled:", source_count)
print("Profile output rows:", profile_output_row_count)
print("Raw enterprise rows collected to Python: 0")
print("Delta tables written: 0")
print("Normalizers changed: 0")
print("Rules changed: 0")
print("Operational effects: 0")
print(
    "Next gate: review P1 value semantics and scope-match quality, then approve "
    "the exact upstream fields to add to the reusable INTUNE_NORM and MDE_NORM contracts."
)
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 003 - Bounded P1 Intune/MDE Value Profile
# Version: 0.2.0-EFFICIENT
# Mode: READ_ONLY_POST_SCOPED_VALUE_PROFILE
#
# Purpose:
#   Profile only the P1 configuration/health fields identified by Discovery
#   003, using the current Post's normalized Intune/MDE identities as the
#   bounding population.
#
# Design rules:
#   - Runtime-driven; no Post names, table names, field names, or counts are
#     hard-coded.
#   - Uses candidate_review_df from Discovery 003 Cell 3.
#   - Reads only identity and P1 fields from resolved upstream sources.
#   - Uses one equality-key scan per source; no OR/nested-loop raw join.
#   - Persists the bounded Post subset until every profile action finishes.
#   - Bounds upstream rows through exact normalized device-name and/or serial
#     matches from the current Post/run normalized source products.
#   - Refuses to profile a source when no safe identity join can be resolved.
#   - Does not change normalizers, rules, the Asset Plane, or durable products.
#
# Prerequisites:
#   Cell 1: %run ./RCAIP_Config_001_Table_Registry
#   Cell 2: Discovery 003 control-evidence catalog
#   Cell 3: Discovery 003 candidate field review
# ============================================================================

from functools import reduce
import operator
import re

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# 1. Validate the in-memory Discovery 003 and runtime contracts
# ---------------------------------------------------------------------------

required_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "candidate_review_df",
    "catalog_df",
    "status_df",
]

missing_objects = [name for name in required_objects if name not in globals()]
if missing_objects:
    raise RuntimeError(
        "Discovery 003 bounded P1 profile is missing required objects: "
        + " | ".join(missing_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")


# ---------------------------------------------------------------------------
# 2. Small metadata collections only - no upstream enterprise rows collected
# ---------------------------------------------------------------------------

p1_metadata = [
    row.asDict(recursive=True)
    for row in (
        candidate_review_df
        .filter(F.col("review_priority") == "P1")
        .orderBy("source_name", "raw_field_name")
        .collect()
    )
]

if not p1_metadata:
    raise RuntimeError("Discovery 003 produced no P1 fields to profile.")

status_metadata = {
    row["source_name"]: row.asDict(recursive=True)
    for row in status_df.collect()
}


# ---------------------------------------------------------------------------
# 3. Reusable helpers
# ---------------------------------------------------------------------------

def quoted_col(column_name, alias=None):
    escaped = str(column_name).replace("`", "``")
    if alias:
        return F.col(f"{alias}.`{escaped}`")
    return F.col(f"`{escaped}`")


def normalized_text(column_expression):
    return F.upper(F.trim(column_expression.cast("string")))


def normalized_field_token(field_name):
    return re.sub(r"[^a-z0-9]+", "", str(field_name).lower())


def matching_columns(columns, exact_tokens=None, regex_tokens=None):
    exact_tokens = {
        normalized_field_token(value)
        for value in (exact_tokens or [])
    }
    regex_tokens = list(regex_tokens or [])

    matches = []
    for column_name in columns:
        token = normalized_field_token(column_name)
        if token in exact_tokens:
            matches.append(column_name)
            continue
        if any(re.search(pattern, token, flags=re.IGNORECASE) for pattern in regex_tokens):
            matches.append(column_name)

    # Preserve schema order while removing duplicates.
    return list(dict.fromkeys(matches))


def union_all(frames):
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right), frames)


def nonblank_string(column_expression):
    return (
        column_expression.isNotNull()
        & (F.length(F.trim(column_expression.cast("string"))) > 0)
    )


def make_scope_frame(normalized_df, source_name, column_name, join_type, confidence):
    host_key_expression = (
        quoted_col("host_key")
        if "host_key" in normalized_df.columns
        else F.lit(None).cast("string")
    )

    return (
        normalized_df
        .filter(nonblank_string(quoted_col(column_name)))
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(join_type).alias("join_type"),
            normalized_text(quoted_col(column_name)).alias("join_value"),
            F.coalesce(
                host_key_expression.cast("string"),
                quoted_col(column_name).cast("string"),
            ).alias("scope_host_key"),
            F.lit(column_name).alias("normalized_join_field"),
            F.lit(confidence).alias("scope_match_confidence"),
        )
        .dropDuplicates(["source_name", "join_type", "join_value", "scope_host_key"])
    )


def profile_scalar_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    value_expression = (
        F.when(quoted_col(field_name).isNull(), F.lit("<NULL>"))
        .when(
            F.length(F.trim(quoted_col(field_name).cast("string"))) == 0,
            F.lit("<BLANK>"),
        )
        .otherwise(quoted_col(field_name).cast("string"))
    )

    grouped = (
        scoped_df
        .withColumn("profile_value", value_expression)
        .groupBy("profile_value")
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("scoped_record_count"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identity_count"),
            F.max("scope_match_confidence").alias("highest_scope_match_confidence"),
        )
        .withColumn("source_name", F.lit(source_name))
        .withColumn("raw_source_table", F.lit(raw_table))
        .withColumn("raw_field_name", F.lit(field_name))
        .withColumn("raw_field_type", F.lit(field_type))
        .withColumn("profile_method", F.lit("BOUNDED_VALUE_DISTRIBUTION"))
        .withColumn("scope_method", F.lit(scope_method))
        .withColumn("total_scoped_source_records", F.lit(int(total_scoped_records)).cast("long"))
        .withColumn(
            "percent_of_scoped_records",
            F.round(
                F.col("scoped_record_count")
                / F.when(
                    F.col("total_scoped_source_records") > 0,
                    F.col("total_scoped_source_records"),
                ).otherwise(F.lit(None).cast("long"))
                * F.lit(100.0),
                2,
            ),
        )
    )

    ranking_window = Window.orderBy(
        F.desc("scoped_record_count"),
        F.asc("profile_value"),
    )

    return (
        grouped
        .withColumn("value_rank", F.row_number().over(ranking_window))
        .filter(F.col("value_rank") <= 100)
        .select(
            "source_name",
            "raw_source_table",
            "raw_field_name",
            "raw_field_type",
            "profile_method",
            "scope_method",
            "profile_value",
            "scoped_record_count",
            "distinct_post_identity_count",
            "highest_scope_match_confidence",
            "total_scoped_source_records",
            "percent_of_scoped_records",
            "value_rank",
        )
    )


def profile_temporal_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    escaped_timestamp_field = str(field_name).replace("`", "``")
    timestamp_expression = F.expr(
        f"try_cast(`{escaped_timestamp_field}` AS TIMESTAMP)"
    )
    age_days_expression = F.datediff(F.current_timestamp(), timestamp_expression)

    bucket_expression = (
        F.when(timestamp_expression.isNull(), "<NULL_OR_UNPARSEABLE>")
        .when(age_days_expression < 0, "FUTURE_TIMESTAMP")
        .when(age_days_expression <= 1, "AGE_LE_1_DAY")
        .when(age_days_expression <= 3, "AGE_LE_3_DAYS")
        .when(age_days_expression <= 7, "AGE_LE_7_DAYS")
        .when(age_days_expression <= 14, "AGE_LE_14_DAYS")
        .when(age_days_expression <= 30, "AGE_LE_30_DAYS")
        .otherwise("AGE_GT_30_DAYS")
    )

    grouped = (
        scoped_df
        .withColumn("profile_value", bucket_expression)
        .groupBy("profile_value")
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("scoped_record_count"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identity_count"),
            F.max("scope_match_confidence").alias("highest_scope_match_confidence"),
        )
        .withColumn("source_name", F.lit(source_name))
        .withColumn("raw_source_table", F.lit(raw_table))
        .withColumn("raw_field_name", F.lit(field_name))
        .withColumn("raw_field_type", F.lit(field_type))
        .withColumn("profile_method", F.lit("FRESHNESS_OR_RANGE_PROFILE"))
        .withColumn("scope_method", F.lit(scope_method))
        .withColumn("total_scoped_source_records", F.lit(int(total_scoped_records)).cast("long"))
        .withColumn(
            "percent_of_scoped_records",
            F.round(
                F.col("scoped_record_count")
                / F.when(
                    F.col("total_scoped_source_records") > 0,
                    F.col("total_scoped_source_records"),
                ).otherwise(F.lit(None).cast("long"))
                * F.lit(100.0),
                2,
            ),
        )
    )

    ranking_window = Window.orderBy(
        F.desc("scoped_record_count"),
        F.asc("profile_value"),
    )

    return (
        grouped
        .withColumn("value_rank", F.row_number().over(ranking_window))
        .select(
            "source_name",
            "raw_source_table",
            "raw_field_name",
            "raw_field_type",
            "profile_method",
            "scope_method",
            "profile_value",
            "scoped_record_count",
            "distinct_post_identity_count",
            "highest_scope_match_confidence",
            "total_scoped_source_records",
            "percent_of_scoped_records",
            "value_rank",
        )
    )


def profile_complex_field(
    scoped_df,
    source_name,
    raw_table,
    field_name,
    field_type,
    scope_method,
    total_scoped_records,
):
    profile_value_expression = F.when(
        quoted_col(field_name).isNull(),
        F.lit("<NULL>"),
    ).otherwise(F.lit("<NON_NULL_COMPLEX_VALUE>"))

    return profile_scalar_field(
        scoped_df.withColumn(field_name, profile_value_expression),
        source_name,
        raw_table,
        field_name,
        field_type,
        scope_method,
        total_scoped_records,
    ).withColumn("profile_method", F.lit("SCHEMA_AND_BOUNDED_SHAPE_REVIEW"))


# ---------------------------------------------------------------------------
# 4. Build a safe Post-scoped identity bridge for each source and profile P1
# ---------------------------------------------------------------------------

metadata_by_source = {}
for row in p1_metadata:
    metadata_by_source.setdefault(row["source_name"], []).append(row)

profile_frames = []
scope_status_rows = []
profiled_field_count = 0
persisted_scoped_dfs = []

for source_name in sorted(metadata_by_source.keys()):
    source_status = status_metadata.get(source_name)
    if not source_status:
        raise RuntimeError(f"No Discovery 003 source status exists for {source_name}.")

    raw_table = source_status.get("raw_source_table")
    normalized_table = source_status.get("normalized_table")

    if source_status.get("raw_source_status") != "AVAILABLE":
        raise RuntimeError(f"Upstream {source_name} source is not available: {raw_table}")
    if source_status.get("normalized_status") != "AVAILABLE":
        raise RuntimeError(
            f"Post-scoped normalized {source_name} source is not available: "
            f"{normalized_table}"
        )

    raw_df = spark.table(raw_table)
    normalized_df = spark.table(normalized_table)

    if "rcaip_run_id" in normalized_df.columns:
        normalized_df = normalized_df.filter(F.col("rcaip_run_id") == RCAIP_RUN_ID)

    normalized_name_exact = {
        "INTUNE": ["intune_device_name", "canonical_name"],
        "MDE": ["mde_device_name", "canonical_name"],
    }.get(source_name, ["canonical_name"])

    normalized_serial_exact = {
        "INTUNE": ["intune_serial_number", "canonical_serial"],
        "MDE": ["canonical_serial"],
    }.get(source_name, ["canonical_serial"])

    raw_name_exact = [
        "device_name", "devicename", "hostname", "computer_name",
        "computername", "machine_name", "machinename",
        "properties_device_name", "properties_devicename",
    ]
    raw_name_patterns = [
        r"devicename$", r"hostname$", r"computername$", r"machinename$",
    ]

    raw_serial_exact = [
        "serial_number", "serialnumber", "device_serial_number",
        "properties_serial_number", "properties_serialnumber",
    ]
    raw_serial_patterns = [r"serialnumber$", r"serial$"]

    normalized_name_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_name_exact,
    )
    normalized_serial_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_serial_exact,
    )
    raw_name_columns = matching_columns(
        raw_df.columns,
        exact_tokens=raw_name_exact,
        regex_tokens=raw_name_patterns,
    )
    raw_serial_columns = matching_columns(
        raw_df.columns,
        exact_tokens=raw_serial_exact,
        regex_tokens=raw_serial_patterns,
    )

    scope_frames = []
    for column_name in normalized_serial_columns:
        scope_frames.append(
            make_scope_frame(
                normalized_df,
                source_name,
                column_name,
                "SERIAL",
                "HIGH_EXACT_SERIAL",
            )
        )
    for column_name in normalized_name_columns:
        scope_frames.append(
            make_scope_frame(
                normalized_df,
                source_name,
                column_name,
                "DEVICE_NAME",
                "MODERATE_EXACT_DEVICE_NAME",
            )
        )

    scope_keys_df = union_all(scope_frames)
    if scope_keys_df is None:
        raise RuntimeError(
            f"No safe normalized identity fields were resolved for {source_name}."
        )

    scope_key_count = scope_keys_df.count()
    if scope_key_count == 0:
        raise RuntimeError(
            f"No current-run normalized identity values were available for {source_name}."
        )
    if scope_key_count > 50000:
        raise RuntimeError(
            f"The {source_name} Post scope unexpectedly contains {scope_key_count} "
            "identity keys; review the normalized source before profiling."
        )

    # Persist the small Post-scoped key set once. It is reused for the bounded
    # equality join and must never be recomputed against the normalized table
    # during each downstream profile.
    scope_keys_df = scope_keys_df.persist(StorageLevel.MEMORY_AND_DISK)

    # Build one compact equality-key representation of the raw source. The
    # prior OR-based join could force a broadcast nested-loop comparison across
    # the enterprise source. Exploding raw identity keys converts the operation
    # into a broadcast hash join on (_join_type, _join_value).
    raw_id_columns = matching_columns(
        raw_df.columns,
        exact_tokens=[
            "device_id", "deviceid", "managed_device_id", "manageddeviceid",
            "machine_id", "machineid", "aad_device_id", "aaddeviceid",
            "azure_ad_device_id", "azureaddeviceid",
        ],
        regex_tokens=[
            r"manageddeviceid$", r"machineid$", r"aaddeviceid$", r"azureaddeviceid$",
        ],
    )

    approved_profile_fields = [
        row["raw_field_name"]
        for row in metadata_by_source[source_name]
    ]

    # Read only fields needed for identity scoping, record identity, and the
    # approved P1 profiles. Do not carry every raw enterprise column through
    # the join or into the persisted Post-scoped subset.
    required_raw_columns = list(dict.fromkeys(
        raw_id_columns
        + raw_serial_columns
        + raw_name_columns
        + approved_profile_fields
    ))

    missing_required_raw_columns = [
        column_name
        for column_name in required_raw_columns
        if column_name not in raw_df.columns
    ]
    if missing_required_raw_columns:
        raise RuntimeError(
            f"Required upstream fields are absent from {raw_table}: "
            + " | ".join(missing_required_raw_columns)
        )

    raw_minimal_df = raw_df.select(
        *[
            quoted_col(column_name).alias(column_name)
            for column_name in required_raw_columns
        ]
    )

    key_candidates = []
    for column_name in raw_id_columns + raw_serial_columns + raw_name_columns:
        key_candidates.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.concat(
                    F.lit(f"{column_name}="),
                    normalized_text(quoted_col(column_name)),
                ),
            )
        )

    if key_candidates:
        raw_record_key_expression = F.coalesce(*key_candidates)
    else:
        raise RuntimeError(
            f"No stable upstream record-key candidates were resolved for {source_name}."
        )

    raw_join_key_expressions = []
    for column_name in raw_serial_columns:
        raw_join_key_expressions.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.struct(
                    F.lit("SERIAL").alias("join_type"),
                    normalized_text(quoted_col(column_name)).alias("join_value"),
                    F.lit(column_name).alias("raw_join_field"),
                ),
            )
        )
    for column_name in raw_name_columns:
        raw_join_key_expressions.append(
            F.when(
                nonblank_string(quoted_col(column_name)),
                F.struct(
                    F.lit("DEVICE_NAME").alias("join_type"),
                    normalized_text(quoted_col(column_name)).alias("join_value"),
                    F.lit(column_name).alias("raw_join_field"),
                ),
            )
        )

    if not raw_join_key_expressions:
        raise RuntimeError(
            f"No safe upstream identity join fields were resolved for {source_name}. "
            "Review the Discovery 003 identity candidates before profiling values."
        )

    raw_keyed_df = (
        raw_minimal_df
        .withColumn("_raw_record_key", raw_record_key_expression)
        .filter(F.col("_raw_record_key").isNotNull())
        .withColumn(
            "_join_key",
            F.explode(F.array(*raw_join_key_expressions)),
        )
        .filter(F.col("_join_key").isNotNull())
        .select(
            *[quoted_col(column_name) for column_name in required_raw_columns],
            "_raw_record_key",
            F.col("_join_key.join_type").alias("_join_type"),
            F.col("_join_key.join_value").alias("_join_value"),
            F.col("_join_key.raw_join_field").alias("raw_join_field"),
        )
    )

    scope_join_df = (
        scope_keys_df
        .select(
            F.col("join_type").alias("_join_type"),
            F.col("join_value").alias("_join_value"),
            "scope_host_key",
            "normalized_join_field",
            "scope_match_confidence",
        )
        .dropDuplicates([
            "_join_type",
            "_join_value",
            "scope_host_key",
        ])
    )

    scoped_df = (
        raw_keyed_df
        .join(
            F.broadcast(scope_join_df),
            ["_join_type", "_join_value"],
            "inner",
        )
        .select(
            *[quoted_col(column_name) for column_name in required_raw_columns],
            "_raw_record_key",
            "scope_host_key",
            F.col("_join_type").alias("scope_join_type"),
            "normalized_join_field",
            "scope_match_confidence",
            "raw_join_field",
        )
        .dropDuplicates(["_raw_record_key", "scope_host_key"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # One action materializes the expensive raw-source join and calculates both
    # metrics. Every field profile below reads only this compact persisted
    # Post-scoped subset.
    scope_metrics = (
        scoped_df
        .agg(
            F.countDistinct("_raw_record_key").cast("long").alias("total_scoped_records"),
            F.countDistinct("scope_host_key").cast("long").alias("distinct_post_identities"),
        )
        .first()
    )

    total_scoped_records = int(scope_metrics["total_scoped_records"] or 0)
    distinct_post_identities = int(scope_metrics["distinct_post_identities"] or 0)

    # The expensive join has now been materialized, so the small key DataFrame
    # can be released. Keep scoped_df persisted until all value profiles and
    # displays have completed.
    scope_keys_df.unpersist(blocking=False)
    persisted_scoped_dfs.append(scoped_df)

    scope_method_parts = []
    if raw_serial_columns and normalized_serial_columns:
        scope_method_parts.append("EXACT_SERIAL")
    if raw_name_columns and normalized_name_columns:
        scope_method_parts.append("EXACT_DEVICE_NAME")
    scope_method = "+".join(scope_method_parts)

    scope_status_rows.append((
        source_name,
        str(raw_table),
        str(normalized_table),
        "|".join(normalized_serial_columns) if normalized_serial_columns else "NONE",
        "|".join(normalized_name_columns) if normalized_name_columns else "NONE",
        "|".join(raw_serial_columns) if raw_serial_columns else "NONE",
        "|".join(raw_name_columns) if raw_name_columns else "NONE",
        scope_method,
        int(scope_key_count),
        int(total_scoped_records),
        int(distinct_post_identities),
        "PASS",
    ))

    for field_metadata in metadata_by_source[source_name]:
        field_name = field_metadata["raw_field_name"]
        field_type = str(field_metadata["raw_field_type"]).lower()
        requested_method = field_metadata["recommended_profile_method"]

        if field_name not in scoped_df.columns:
            raise RuntimeError(
                f"Approved P1 field {source_name}.{field_name} is absent from "
                f"the current upstream schema {raw_table}."
            )

        if requested_method == "FRESHNESS_OR_RANGE_PROFILE" or re.search(
            r"timestamp|date", field_type
        ):
            frame = profile_temporal_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )
        elif requested_method == "SCHEMA_AND_BOUNDED_SHAPE_REVIEW" or re.search(
            r"array|map|struct", field_type
        ):
            frame = profile_complex_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )
        else:
            frame = profile_scalar_field(
                scoped_df,
                source_name,
                raw_table,
                field_name,
                field_type,
                scope_method,
                total_scoped_records,
            )

        profile_frames.append(frame)
        profiled_field_count += 1


# ---------------------------------------------------------------------------
# 5. Publish read-only review outputs
# ---------------------------------------------------------------------------

scope_status_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), False),
    T.StructField("normalized_table", T.StringType(), False),
    T.StructField("normalized_serial_fields", T.StringType(), False),
    T.StructField("normalized_name_fields", T.StringType(), False),
    T.StructField("raw_serial_fields", T.StringType(), False),
    T.StructField("raw_name_fields", T.StringType(), False),
    T.StructField("scope_method", T.StringType(), False),
    T.StructField("scope_key_count", T.IntegerType(), False),
    T.StructField("scoped_raw_record_count", T.IntegerType(), False),
    T.StructField("distinct_post_identity_count", T.IntegerType(), False),
    T.StructField("scope_status", T.StringType(), False),
])

scope_status_df = spark.createDataFrame(scope_status_rows, schema=scope_status_schema)

p1_profile_df = union_all(profile_frames)
if p1_profile_df is None:
    raise RuntimeError("No P1 value profiles were produced.")

# Materialize the compact profile output once. The summary, detailed display,
# and certification count then reuse the cached result instead of replaying the
# per-field profile lineage.
p1_profile_df = p1_profile_df.persist(StorageLevel.MEMORY_AND_DISK)
profile_output_row_count = p1_profile_df.count()

p1_profile_summary_df = (
    p1_profile_df
    .groupBy(
        "source_name",
        "raw_field_name",
        "raw_field_type",
        "profile_method",
        "scope_method",
        "total_scoped_source_records",
    )
    .agg(
        F.count(F.lit(1)).cast("long").alias("profile_value_count_displayed"),
        F.sum("scoped_record_count").cast("long").alias("sum_profiled_record_counts"),
        F.max("distinct_post_identity_count").cast("long").alias("max_distinct_post_identity_count"),
    )
    .orderBy("source_name", "raw_field_name")
)

print("Approved P1 metadata fields included in the bounded profile")
display(
    candidate_review_df
    .filter(F.col("review_priority") == "P1")
    .orderBy("source_name", "raw_field_name")
)

print("Post-scoping identity bridge certification")
display(scope_status_df.orderBy("source_name"))

print("Bounded P1 profile summary")
display(p1_profile_summary_df)

print("Bounded P1 value distributions - top 100 values per field")
display(
    p1_profile_df.orderBy(
        "source_name",
        "raw_field_name",
        "value_rank",
        "profile_value",
    )
)

requested_p1_field_count = len(p1_metadata)
source_count = len(metadata_by_source)

if profiled_field_count != requested_p1_field_count:
    raise RuntimeError(
        "P1 field profile reconciliation failed. Requested="
        f"{requested_p1_field_count}; profiled={profiled_field_count}"
    )

# All actions using the persisted Post-scoped subsets and profile output are
# complete. Release them only now; never before the profile actions execute.
p1_profile_df.unpersist(blocking=False)
for persisted_df in persisted_scoped_dfs:
    persisted_df.unpersist(blocking=False)

print("=" * 100)
print("RCAIP DISCOVERY 003 BOUNDED P1 VALUE PROFILE CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Mode: READ_ONLY_POST_SCOPED_VALUE_PROFILE")
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("P1 candidate fields requested:", requested_p1_field_count)
print("P1 candidate fields profiled:", profiled_field_count)
print("Sources profiled:", source_count)
print("Profile output rows:", profile_output_row_count)
print("Raw enterprise rows collected to Python: 0")
print("Delta tables written: 0")
print("Normalizers changed: 0")
print("Rules changed: 0")
print("Operational effects: 0")
print(
    "Next gate: review P1 value semantics and scope-match quality, then approve "
    "the exact upstream fields to add to the reusable INTUNE_NORM and MDE_NORM contracts."
)
print("=" * 100)


# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 003 - Latest-Record Control-Evidence Validation
# Cell 5 - One current raw record per Post-scoped normalized asset
#
# Purpose:
#   Correct the bounded P1 profile's many-to-many identity and historical-row
#   limitations before approving reusable INTUNE_NORM or MDE_NORM fields.
#
# Prerequisites:
#   Cell 1: %run ./RCAIP_Config_001_Table_Registry
#   Cell 2: Intune/MDE Control-Evidence Catalog
#   Cell 3: Candidate Field Review
#
# Performance contract:
#   - Reads each enterprise source once with only required columns.
#   - Uses broadcast equality joins; no OR join and no nested-loop design.
#   - Filters to the bounded Post identity set before any window operation.
#   - Persists only compact Post-scoped candidates and latest-asset rows.
#   - Never loops through the enterprise source once per field.
#
# Analytical contract:
#   - Excludes ambiguous normalized join values.
#   - Excludes raw snapshots mapping to more than one Post asset.
#   - Prefers exact serial matches over exact device-name matches.
#   - Selects one latest raw snapshot per normalized asset_key.
#   - Requires every field distribution to reconcile to the latest-asset count.
#
# Safety:
#   - Read-only.
#   - No Delta writes.
#   - No normalizer or rule changes.
#   - No raw enterprise rows collected to Python.
# ============================================================================

from functools import reduce
import re

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate inherited objects
# ---------------------------------------------------------------------------

required_objects = [
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "candidate_review_df",
    "status_df",
]

missing_objects = [name for name in required_objects if name not in globals()]
if missing_objects:
    raise RuntimeError(
        "Discovery 003 latest-record validation is missing prerequisite objects: "
        + " | ".join(missing_objects)
    )

LATEST_PROFILE_VERSION = "0.1.0"
LATEST_PROFILE_MODE = "READ_ONLY_POST_SCOPED_LATEST_RECORD_VALIDATION"


def quoted_col(column_name):
    return F.col("`" + str(column_name).replace("`", "``") + "`")


def normalized_token(value):
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def matching_columns(columns, exact_tokens=None, regex_tokens=None):
    exact_tokens = exact_tokens or []
    regex_tokens = regex_tokens or []

    exact_normalized = {normalized_token(token) for token in exact_tokens}
    matches = []

    for column_name in columns:
        normalized_name = normalized_token(column_name)
        if normalized_name in exact_normalized:
            matches.append(column_name)
            continue

        if any(
            re.search(pattern, normalized_name, flags=re.IGNORECASE)
            for pattern in regex_tokens
        ):
            matches.append(column_name)

    return list(dict.fromkeys(matches))


def nonblank_string(expression):
    return (
        expression.isNotNull()
        & (F.length(F.trim(expression.cast("string"))) > 0)
    )


def normalized_text(expression):
    return F.lower(F.trim(expression.cast("string")))


def union_all(frames):
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right), frames)


def first_existing_column(columns, exact_tokens, regex_tokens=None):
    matches = matching_columns(
        columns,
        exact_tokens=exact_tokens,
        regex_tokens=regex_tokens or [],
    )
    return matches[0] if matches else None


def coalesced_timestamp(columns, candidate_names):
    matched = matching_columns(columns, exact_tokens=candidate_names)
    if not matched:
        return F.lit(None).cast("timestamp"), []

    safe_timestamp_expressions = []
    for name in matched:
        escaped_name = str(name).replace("`", "``")
        safe_timestamp_expressions.append(
            F.expr(f"try_cast(`{escaped_name}` AS TIMESTAMP)")
        )

    return F.coalesce(*safe_timestamp_expressions), matched


def scope_frame(normalized_df, source_name, field_name, join_type, confidence):
    return (
        normalized_df
        .filter(
            F.col("asset_key").isNotNull()
            & nonblank_string(quoted_col(field_name))
        )
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(join_type).alias("join_type"),
            normalized_text(quoted_col(field_name)).alias("join_value"),
            F.col("asset_key").cast("string").alias("scope_asset_key"),
            F.col("host_key").cast("string").alias("scope_host_key"),
            F.lit(field_name).alias("normalized_join_field"),
            F.lit(confidence).alias("scope_match_confidence"),
            F.lit(1 if join_type == "SERIAL" else 2).alias("scope_confidence_rank"),
        )
        .dropDuplicates([
            "source_name",
            "join_type",
            "join_value",
            "scope_asset_key",
        ])
    )


def value_expression(field_name):
    return (
        F.when(quoted_col(field_name).isNull(), F.lit("<NULL>"))
        .when(
            F.length(F.trim(quoted_col(field_name).cast("string"))) == 0,
            F.lit("<BLANK>"),
        )
        .otherwise(quoted_col(field_name).cast("string"))
    )


# ---------------------------------------------------------------------------
# 2. Resolve P1 metadata and source status
# ---------------------------------------------------------------------------

p1_metadata_rows = [
    row.asDict(recursive=True)
    for row in (
        candidate_review_df
        .filter(F.col("review_priority") == "P1")
        .orderBy("source_name", "raw_field_name")
        .collect()
    )
]

if not p1_metadata_rows:
    raise RuntimeError("Discovery 003 has no P1 fields for latest-record validation.")

status_rows = {
    row["source_name"]: row.asDict(recursive=True)
    for row in status_df.collect()
}

metadata_by_source = {}
for metadata_row in p1_metadata_rows:
    metadata_by_source.setdefault(metadata_row["source_name"], []).append(metadata_row)


# ---------------------------------------------------------------------------
# 3. Build one latest Post-scoped record per asset for each source
# ---------------------------------------------------------------------------

scope_quality_rows = []
profile_frames = []
concordance_frames = []
latest_frames_by_source = {}
persisted_latest_frames = []
profiled_field_count = 0

for source_name in sorted(metadata_by_source.keys()):
    source_status = status_rows.get(source_name)
    if not source_status:
        raise RuntimeError(f"No source status row exists for {source_name}.")

    raw_table = source_status.get("raw_source_table")
    normalized_table = source_status.get("normalized_table")

    if source_status.get("raw_source_status") != "AVAILABLE":
        raise RuntimeError(f"Upstream {source_name} table is unavailable: {raw_table}")
    if source_status.get("normalized_status") != "AVAILABLE":
        raise RuntimeError(
            f"Normalized {source_name} table is unavailable: {normalized_table}"
        )

    raw_df = spark.table(raw_table)
    normalized_df = spark.table(normalized_table)

    if "rcaip_run_id" in normalized_df.columns:
        normalized_df = normalized_df.filter(F.col("rcaip_run_id") == RCAIP_RUN_ID)

    required_normalized_columns = {"asset_key", "host_key"}
    missing_normalized = required_normalized_columns - set(normalized_df.columns)
    if missing_normalized:
        raise RuntimeError(
            f"Normalized {source_name} table lacks required identity fields: "
            + " | ".join(sorted(missing_normalized))
        )

    normalized_name_candidates = {
        "INTUNE": ["intune_device_name", "canonical_name"],
        "MDE": ["mde_device_name", "canonical_name"],
    }.get(source_name, ["canonical_name"])

    normalized_serial_candidates = {
        "INTUNE": ["intune_serial_number", "canonical_serial"],
        "MDE": ["canonical_serial"],
    }.get(source_name, ["canonical_serial"])

    normalized_name_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_name_candidates,
    )
    normalized_serial_columns = matching_columns(
        normalized_df.columns,
        exact_tokens=normalized_serial_candidates,
    )

    normalized_asset_count = normalized_df.select("asset_key").distinct().count()

    scope_frames = []
    for field_name in normalized_serial_columns:
        scope_frames.append(
            scope_frame(
                normalized_df,
                source_name,
                field_name,
                "SERIAL",
                "HIGH_EXACT_SERIAL",
            )
        )
    for field_name in normalized_name_columns:
        scope_frames.append(
            scope_frame(
                normalized_df,
                source_name,
                field_name,
                "DEVICE_NAME",
                "MODERATE_EXACT_DEVICE_NAME",
            )
        )

    all_scope_keys_df = union_all(scope_frames)
    if all_scope_keys_df is None:
        raise RuntimeError(f"No normalized identity keys resolved for {source_name}.")

    scope_value_quality_df = (
        all_scope_keys_df
        .groupBy("join_type", "join_value")
        .agg(
            F.countDistinct("scope_asset_key").cast("long").alias(
                "scope_asset_key_count"
            )
        )
    )

    ambiguous_scope_value_count = (
        scope_value_quality_df
        .filter(F.col("scope_asset_key_count") > 1)
        .count()
    )

    unambiguous_scope_keys_df = (
        all_scope_keys_df
        .join(
            scope_value_quality_df.filter(F.col("scope_asset_key_count") == 1),
            ["join_type", "join_value"],
            "inner",
        )
        .drop("scope_asset_key_count")
        .dropDuplicates([
            "join_type",
            "join_value",
            "scope_asset_key",
        ])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    unambiguous_scope_key_count = unambiguous_scope_keys_df.count()
    if unambiguous_scope_key_count == 0:
        raise RuntimeError(
            f"No unambiguous normalized identity keys remain for {source_name}."
        )
    if unambiguous_scope_key_count > 50000:
        raise RuntimeError(
            f"{source_name} unambiguous scope contains {unambiguous_scope_key_count} "
            "keys; review Post scoping before enterprise profiling."
        )

    raw_name_columns = matching_columns(
        raw_df.columns,
        exact_tokens=[
            "device_name", "devicename", "hostname", "computer_name",
            "computername", "machine_name", "machinename",
            "properties_device_name", "properties_devicename",
        ],
        regex_tokens=[
            r"devicename$", r"hostname$", r"computername$", r"machinename$",
        ],
    )

    raw_serial_columns = matching_columns(
        raw_df.columns,
        exact_tokens=[
            "serial_number", "serialnumber", "device_serial_number",
            "properties_serial_number", "properties_serialnumber",
        ],
        regex_tokens=[r"serialnumber$", r"serial$"],
    )

    raw_id_columns = matching_columns(
        raw_df.columns,
        exact_tokens=[
            "device_id", "deviceid", "managed_device_id", "manageddeviceid",
            "machine_id", "machineid", "aad_device_id", "aaddeviceid",
            "azure_ad_device_id", "azureaddeviceid",
        ],
        regex_tokens=[
            r"manageddeviceid$", r"machineid$", r"aaddeviceid$",
            r"azureaddeviceid$",
        ],
    )

    timestamp_candidates = {
        "INTUNE": [
            "properties_lastcontact", "properties_lastsyncdatetime",
            "last_contact", "last_sync_date_time", "last_check_in",
            "last_seen", "timestamp",
        ],
        "MDE": [
            "timestamp", "mde_last_seen", "last_seen", "lastseen",
        ],
    }.get(source_name, ["timestamp", "last_seen"])

    raw_timestamp_expression, raw_timestamp_columns = coalesced_timestamp(
        raw_df.columns,
        timestamp_candidates,
    )

    approved_fields = [
        row["raw_field_name"] for row in metadata_by_source[source_name]
    ]

    required_raw_columns = list(dict.fromkeys(
        raw_id_columns
        + raw_serial_columns
        + raw_name_columns
        + raw_timestamp_columns
        + approved_fields
    ))

    if not required_raw_columns:
        raise RuntimeError(f"No required upstream columns resolved for {source_name}.")

    missing_raw_fields = [
        field_name for field_name in approved_fields if field_name not in raw_df.columns
    ]
    if missing_raw_fields:
        raise RuntimeError(
            f"Approved fields are missing from {raw_table}: "
            + " | ".join(missing_raw_fields)
        )

    raw_minimal_df = raw_df.select(
        *[quoted_col(name).alias(name) for name in required_raw_columns]
    )

    stable_key_candidates = []
    for field_name in raw_id_columns + raw_serial_columns + raw_name_columns:
        stable_key_candidates.append(
            F.when(
                nonblank_string(quoted_col(field_name)),
                F.concat(
                    F.lit(f"{field_name}="),
                    normalized_text(quoted_col(field_name)),
                ),
            )
        )

    if not stable_key_candidates:
        raise RuntimeError(f"No stable upstream device key resolved for {source_name}.")

    raw_device_key_expression = F.coalesce(*stable_key_candidates)

    join_structs = []
    for field_name in raw_serial_columns:
        join_structs.append(
            F.when(
                nonblank_string(quoted_col(field_name)),
                F.struct(
                    F.lit("SERIAL").alias("join_type"),
                    normalized_text(quoted_col(field_name)).alias("join_value"),
                    F.lit(field_name).alias("raw_join_field"),
                    F.lit(1).alias("raw_confidence_rank"),
                ),
            )
        )
    for field_name in raw_name_columns:
        join_structs.append(
            F.when(
                nonblank_string(quoted_col(field_name)),
                F.struct(
                    F.lit("DEVICE_NAME").alias("join_type"),
                    normalized_text(quoted_col(field_name)).alias("join_value"),
                    F.lit(field_name).alias("raw_join_field"),
                    F.lit(2).alias("raw_confidence_rank"),
                ),
            )
        )

    if not join_structs:
        raise RuntimeError(f"No safe raw identity join keys resolved for {source_name}.")

    snapshot_hash_fields = [
        F.coalesce(quoted_col(name).cast("string"), F.lit("<NULL>"))
        for name in approved_fields
    ]

    raw_keyed_df = (
        raw_minimal_df
        .withColumn("_raw_device_key", raw_device_key_expression)
        .withColumn("_raw_event_ts", raw_timestamp_expression)
        .filter(F.col("_raw_device_key").isNotNull())
        .withColumn(
            "_raw_snapshot_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("_raw_device_key"),
                    F.coalesce(
                        F.date_format(F.col("_raw_event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                        F.lit("<NO_TIMESTAMP>"),
                    ),
                    *snapshot_hash_fields,
                ),
                256,
            ),
        )
        .withColumn("_join_struct", F.explode(F.array(*join_structs)))
        .filter(F.col("_join_struct").isNotNull())
        .select(
            *[quoted_col(name) for name in required_raw_columns],
            "_raw_device_key",
            "_raw_event_ts",
            "_raw_snapshot_key",
            F.col("_join_struct.join_type").alias("_join_type"),
            F.col("_join_struct.join_value").alias("_join_value"),
            F.col("_join_struct.raw_join_field").alias("raw_join_field"),
            F.col("_join_struct.raw_confidence_rank").alias("raw_confidence_rank"),
        )
    )

    scope_join_df = (
        unambiguous_scope_keys_df
        .select(
            F.col("join_type").alias("_join_type"),
            F.col("join_value").alias("_join_value"),
            "scope_asset_key",
            "scope_host_key",
            "normalized_join_field",
            "scope_match_confidence",
            "scope_confidence_rank",
        )
    )

    mapped_candidate_df = (
        raw_keyed_df
        .join(
            F.broadcast(scope_join_df),
            ["_join_type", "_join_value"],
            "inner",
        )
        .withColumn(
            "effective_confidence_rank",
            F.least("raw_confidence_rank", "scope_confidence_rank"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # This is the only action that materializes the raw enterprise scan for the
    # source. All later logic reads the compact persisted Post-scoped result.
    mapped_candidate_count = mapped_candidate_df.count()

    snapshot_asset_counts_df = (
        mapped_candidate_df
        .groupBy("_raw_snapshot_key")
        .agg(
            F.countDistinct("scope_asset_key").cast("long").alias(
                "mapped_asset_count"
            )
        )
    )

    ambiguous_snapshot_count = (
        snapshot_asset_counts_df
        .filter(F.col("mapped_asset_count") > 1)
        .count()
    )

    unambiguous_mapped_df = (
        mapped_candidate_df
        .join(
            snapshot_asset_counts_df.filter(F.col("mapped_asset_count") == 1),
            "_raw_snapshot_key",
            "inner",
        )
        .drop("mapped_asset_count")
    )

    mapping_choice_window = (
        Window
        .partitionBy("_raw_snapshot_key", "scope_asset_key")
        .orderBy(
            F.col("effective_confidence_rank").asc(),
            F.col("raw_join_field").asc(),
            F.col("normalized_join_field").asc(),
        )
    )

    one_mapping_per_snapshot_df = (
        unambiguous_mapped_df
        .withColumn(
            "_mapping_row_number",
            F.row_number().over(mapping_choice_window),
        )
        .filter(F.col("_mapping_row_number") == 1)
        .drop("_mapping_row_number")
    )

    latest_asset_window = (
        Window
        .partitionBy("scope_asset_key")
        .orderBy(
            F.col("_raw_event_ts").desc_nulls_last(),
            F.col("effective_confidence_rank").asc(),
            F.col("_raw_snapshot_key").asc(),
        )
    )

    latest_df = (
        one_mapping_per_snapshot_df
        .withColumn(
            "_latest_asset_row_number",
            F.row_number().over(latest_asset_window),
        )
        .filter(F.col("_latest_asset_row_number") == 1)
        .drop("_latest_asset_row_number")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    latest_metrics = (
        latest_df
        .agg(
            F.count(F.lit(1)).cast("long").alias("latest_asset_count"),
            F.countDistinct("scope_asset_key").cast("long").alias(
                "latest_distinct_asset_count"
            ),
            F.sum(
                F.when(F.col("_raw_event_ts").isNull(), 1).otherwise(0)
            ).cast("long").alias("latest_null_timestamp_count"),
            F.sum(
                F.when(F.col("effective_confidence_rank") == 1, 1).otherwise(0)
            ).cast("long").alias("latest_serial_match_count"),
            F.sum(
                F.when(F.col("effective_confidence_rank") == 2, 1).otherwise(0)
            ).cast("long").alias("latest_name_match_count"),
        )
        .first()
    )

    latest_asset_count = int(latest_metrics["latest_asset_count"] or 0)
    latest_distinct_asset_count = int(
        latest_metrics["latest_distinct_asset_count"] or 0
    )

    if latest_asset_count != latest_distinct_asset_count:
        raise RuntimeError(
            f"{source_name} latest-record output is not one row per asset. "
            f"Rows={latest_asset_count}; distinct assets={latest_distinct_asset_count}"
        )

    mapped_candidate_df.unpersist(blocking=False)
    unambiguous_scope_keys_df.unpersist(blocking=False)

    latest_frames_by_source[source_name] = latest_df
    persisted_latest_frames.append(latest_df)

    unmatched_normalized_asset_count = max(
        int(normalized_asset_count) - latest_asset_count,
        0,
    )

    scope_quality_rows.append((
        source_name,
        str(raw_table),
        str(normalized_table),
        int(normalized_asset_count),
        int(unambiguous_scope_key_count),
        int(ambiguous_scope_value_count),
        int(mapped_candidate_count),
        int(ambiguous_snapshot_count),
        int(latest_asset_count),
        int(unmatched_normalized_asset_count),
        int(latest_metrics["latest_null_timestamp_count"] or 0),
        int(latest_metrics["latest_serial_match_count"] or 0),
        int(latest_metrics["latest_name_match_count"] or 0),
        "|".join(raw_timestamp_columns) if raw_timestamp_columns else "NONE",
        "PASS",
    ))

    # One reconciled value distribution per approved field.
    for metadata_row in metadata_by_source[source_name]:
        field_name = metadata_row["raw_field_name"]
        field_type = metadata_row["raw_field_type"]

        distribution_df = (
            latest_df
            .withColumn("profile_value", value_expression(field_name))
            .groupBy("profile_value")
            .agg(
                F.count(F.lit(1)).cast("long").alias("latest_asset_count"),
            )
            .withColumn("source_name", F.lit(source_name))
            .withColumn("raw_source_table", F.lit(str(raw_table)))
            .withColumn("raw_field_name", F.lit(field_name))
            .withColumn("raw_field_type", F.lit(str(field_type)))
            .withColumn(
                "latest_scoped_asset_denominator",
                F.lit(latest_asset_count).cast("long"),
            )
            .withColumn(
                "percent_of_latest_scoped_assets",
                F.round(
                    F.col("latest_asset_count")
                    / F.when(
                        F.col("latest_scoped_asset_denominator") > 0,
                        F.col("latest_scoped_asset_denominator"),
                    ).otherwise(F.lit(None).cast("long"))
                    * F.lit(100.0),
                    2,
                ),
            )
        )

        ranking_window = Window.orderBy(
            F.desc("latest_asset_count"),
            F.asc("profile_value"),
        )

        distribution_df = (
            distribution_df
            .withColumn("value_rank", F.row_number().over(ranking_window))
            .select(
                "source_name",
                "raw_source_table",
                "raw_field_name",
                "raw_field_type",
                "profile_value",
                "latest_asset_count",
                "latest_scoped_asset_denominator",
                "percent_of_latest_scoped_assets",
                "value_rank",
            )
        )

        profile_frames.append(distribution_df)
        profiled_field_count += 1

    # Pairwise concordance is needed before choosing between duplicate or
    # localized Intune fields.
    if source_name == "INTUNE":
        concordance_pairs = [
            (
                "COMPLIANCE_CODE_TO_LOCALIZED_LABEL",
                "properties_ComplianceState",
                "properties_ComplianceState_loc",
            ),
            (
                "COMPLIANCE_CODE_TO_ALTERNATE_STATE",
                "properties_ComplianceState",
                "properties_CompliantState",
            ),
            (
                "HEALTH_CODE_TO_LOCALIZED_LABEL",
                "properties_DeviceHealthThreatLevel",
                "properties_DeviceHealthThreatLevel_loc",
            ),
        ]

        for pair_name, left_field, right_field in concordance_pairs:
            if left_field in latest_df.columns and right_field in latest_df.columns:
                concordance_frames.append(
                    latest_df
                    .withColumn("left_value", value_expression(left_field))
                    .withColumn("right_value", value_expression(right_field))
                    .groupBy("left_value", "right_value")
                    .agg(F.count(F.lit(1)).cast("long").alias("asset_count"))
                    .withColumn("source_name", F.lit(source_name))
                    .withColumn("comparison_name", F.lit(pair_name))
                    .withColumn("left_field", F.lit(left_field))
                    .withColumn("right_field", F.lit(right_field))
                    .withColumn(
                        "latest_scoped_asset_denominator",
                        F.lit(latest_asset_count).cast("long"),
                    )
                    .withColumn(
                        "percent_of_latest_scoped_assets",
                        F.round(
                            F.col("asset_count")
                            / F.col("latest_scoped_asset_denominator")
                            * F.lit(100.0),
                            2,
                        ),
                    )
                    .select(
                        "source_name",
                        "comparison_name",
                        "left_field",
                        "left_value",
                        "right_field",
                        "right_value",
                        "asset_count",
                        "latest_scoped_asset_denominator",
                        "percent_of_latest_scoped_assets",
                    )
                )


# ---------------------------------------------------------------------------
# 4. Reconcile and publish compact outputs
# ---------------------------------------------------------------------------

scope_quality_schema = T.StructType([
    T.StructField("source_name", T.StringType(), False),
    T.StructField("raw_source_table", T.StringType(), False),
    T.StructField("normalized_table", T.StringType(), False),
    T.StructField("normalized_asset_count", T.IntegerType(), False),
    T.StructField("unambiguous_scope_key_count", T.IntegerType(), False),
    T.StructField("ambiguous_scope_join_value_count", T.IntegerType(), False),
    T.StructField("mapped_candidate_row_count", T.IntegerType(), False),
    T.StructField("ambiguous_raw_snapshot_count", T.IntegerType(), False),
    T.StructField("latest_mapped_asset_count", T.IntegerType(), False),
    T.StructField("unmatched_normalized_asset_count", T.IntegerType(), False),
    T.StructField("latest_null_timestamp_count", T.IntegerType(), False),
    T.StructField("latest_serial_match_count", T.IntegerType(), False),
    T.StructField("latest_name_match_count", T.IntegerType(), False),
    T.StructField("raw_timestamp_fields", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
])

scope_quality_df = spark.createDataFrame(
    scope_quality_rows,
    schema=scope_quality_schema,
)

latest_profile_df = union_all(profile_frames)
if latest_profile_df is None:
    raise RuntimeError("No latest-record field profiles were produced.")

latest_profile_df = latest_profile_df.persist(StorageLevel.MEMORY_AND_DISK)
latest_profile_output_rows = latest_profile_df.count()

profile_reconciliation_df = (
    latest_profile_df
    .groupBy(
        "source_name",
        "raw_field_name",
        "latest_scoped_asset_denominator",
    )
    .agg(
        F.sum("latest_asset_count").cast("long").alias(
            "sum_value_asset_counts"
        ),
        F.count(F.lit(1)).cast("long").alias("distinct_profile_values"),
    )
    .withColumn(
        "reconciliation_status",
        F.when(
            F.col("sum_value_asset_counts")
            == F.col("latest_scoped_asset_denominator"),
            "PASS",
        ).otherwise("FAIL"),
    )
    .orderBy("source_name", "raw_field_name")
)

reconciliation_failures = (
    profile_reconciliation_df
    .filter(F.col("reconciliation_status") != "PASS")
    .count()
)

if reconciliation_failures:
    raise RuntimeError(
        f"Latest-record field profile reconciliation failed for "
        f"{reconciliation_failures} fields."
    )

latest_concordance_df = union_all(concordance_frames)

print("Latest-record Post-scope and identity-quality certification")
display(scope_quality_df.orderBy("source_name"))

print("Latest-record field-profile reconciliation")
display(profile_reconciliation_df)

print("Latest-record control-evidence value distributions")
display(
    latest_profile_df.orderBy(
        "source_name",
        "raw_field_name",
        "value_rank",
        "profile_value",
    )
)

if latest_concordance_df is not None:
    print("Intune paired-field concordance")
    display(
        latest_concordance_df.orderBy(
            "comparison_name",
            F.desc("asset_count"),
            "left_value",
            "right_value",
        )
    )

requested_field_count = len(p1_metadata_rows)
if profiled_field_count != requested_field_count:
    raise RuntimeError(
        "Latest-record profile field count mismatch. Requested="
        f"{requested_field_count}; profiled={profiled_field_count}"
    )

# All compact actions are complete. Release caches only now.
latest_profile_df.unpersist(blocking=False)
for latest_df in persisted_latest_frames:
    latest_df.unpersist(blocking=False)

print("=" * 100)
print("RCAIP DISCOVERY 003 LATEST-RECORD CONTROL-EVIDENCE VALIDATION CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Profile version:", LATEST_PROFILE_VERSION)
print("Mode:", LATEST_PROFILE_MODE)
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("P1 candidate fields requested:", requested_field_count)
print("P1 candidate fields profiled:", profiled_field_count)
print("Sources profiled:", len(metadata_by_source))
print("Latest-profile output rows:", latest_profile_output_rows)
print("Field reconciliation failures:", reconciliation_failures)
print("Raw enterprise rows collected to Python: 0")
print("Enterprise source scans per source: 1")
print("Delta tables written: 0")
print("Normalizers changed: 0")
print("Rules changed: 0")
print("Operational effects: 0")
print(
    "Next gate: use the reconciled latest-record distributions, paired-field "
    "concordance, and scope-quality counts to approve exact reusable fields "
    "for INTUNE_NORM and MDE_NORM."
)
print("=" * 100)
