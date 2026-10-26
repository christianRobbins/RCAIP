# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry
# MAGIC

# COMMAND ----------

# ============================================================================
# RCAIP IP-004 - IDENTITY AND ACTIVE DIRECTORY HYGIENE
# Cell 2 - Strict Post scope and one-hop AD context
#
# Purpose:
#   Establish a conservative Post-relevant identity boundary before any
#   hygiene analytics are performed.
#
# Scope model:
#   - Direct Post users: Post token evidence or bounded direct membership in
#     an explicitly Post-attributed group.
#   - Direct Post computers: Asset Plane hostname match or Post token evidence.
#   - Direct Post groups: the group's own attributes contain Post evidence.
#   - One-hop support groups: groups directly referenced by a direct Post
#     user, computer, or group through memberOf.
#
# Guardrails:
#   - No recursive group traversal.
#   - No expansion of one-hop support-group members.
#   - No user or computer attribution from broad direct groups.
#   - No enterprise-wide AD population is written to Post-scoped tables.
#   - No account, membership, group, finding, ticket, or remediation changes.
# ============================================================================

import re

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Runtime and table contracts
# ---------------------------------------------------------------------------

required_runtime = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_POST_TOKENS",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_RUN_MODE",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "get_rcaip_table"
]

missing_runtime = [
    name
    for name in required_runtime
    if name not in globals()
]

if missing_runtime:
    raise RuntimeError(
        "IP-004 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready runtime."
    )

if str(RCAIP_RUN_MODE).upper() not in {
    "DISCOVERY",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}:
    raise RuntimeError(
        "Unsupported IP-004 run mode: "
        + str(RCAIP_RUN_MODE)
    )

IP004_STALE_DAYS = 90
IP004_VERY_STALE_DAYS = 180

# A direct Post group larger than this remains visible as a Post group, but
# its complete member list is not used to attribute users to the Post.
IP004_MAX_DIRECT_GROUP_MEMBERS = int(
    globals().get(
        "RCAIP_IP004_MAX_DIRECT_GROUP_MEMBERS",
        500
    )
)

if IP004_MAX_DIRECT_GROUP_MEMBERS < 1:
    raise RuntimeError(
        "RCAIP_IP004_MAX_DIRECT_GROUP_MEMBERS must be at least 1."
    )

POST_PREFIX = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    f"{RCAIP_POST_SLUG}_"
).lower()


def resolve_table(
    logical_name,
    require_exists=True
):
    try:
        table_name = get_rcaip_table(
            logical_name,
            require_exists=require_exists
        )

    except TypeError:
        table_name = get_rcaip_table(
            logical_name
        )

        if (
            require_exists
            and
            not spark.catalog.tableExists(table_name)
        ):
            raise RuntimeError(
                "Missing required table: "
                f"{logical_name} -> {table_name}"
            )

    return table_name


def resolve_output(
    logical_name,
    fallback_stem
):
    try:
        table_name = resolve_table(
            logical_name,
            require_exists=False
        )

    except Exception:
        table_name = None

    if not table_name:
        table_name = (
            f"{RCAIP_RESULTS_NAMESPACE}."
            f"{RCAIP_POST_SLUG}_{fallback_stem}"
        )

    if not table_name.lower().startswith(
        POST_PREFIX
    ):
        raise RuntimeError(
            "Cross-Post output blocked: "
            + table_name
        )

    return table_name


INPUTS = {
    "users": resolve_table(
        "AD_USERS_SOURCE"
    ),

    "computers": resolve_table(
        "AD_COMPUTERS_SOURCE"
    ),

    "groups": resolve_table(
        "AD_GROUPS_SOURCE"
    ),

    "asset_plane": resolve_table(
        "ASSET_RECONCILIATION_SCORED"
    )
}


OUTPUTS = {
    "IP004_AD_USERS_NORM": resolve_output(
        "IP004_AD_USERS_NORM",
        "ip004_ad_users_norm"
    ),

    "IP004_AD_COMPUTERS_NORM": resolve_output(
        "IP004_AD_COMPUTERS_NORM",
        "ip004_ad_computers_norm"
    ),

    "IP004_AD_GROUPS_NORM": resolve_output(
        "IP004_AD_GROUPS_NORM",
        "ip004_ad_groups_norm"
    ),

    "IP004_AD_OBJECT_DIRECTORY": resolve_output(
        "IP004_AD_OBJECT_DIRECTORY",
        "ip004_ad_object_directory"
    ),

    "IP004_AD_MEMBEROF_BRIDGE": resolve_output(
        "IP004_AD_MEMBEROF_BRIDGE",
        "ip004_ad_memberof_bridge"
    ),

    "IP004_AD_GROUP_MEMBER_BRIDGE": resolve_output(
        "IP004_AD_GROUP_MEMBER_BRIDGE",
        "ip004_ad_group_member_bridge"
    ),

    "IP004_AD_MANAGEDBY_BRIDGE": resolve_output(
        "IP004_AD_MANAGEDBY_BRIDGE",
        "ip004_ad_managedby_bridge"
    ),

    "IP004_COMPUTER_ASSET_MAP": resolve_output(
        "IP004_COMPUTER_ASSET_MAP",
        "ip004_computer_asset_map"
    ),

    "IP004_POST_SCOPE_OBJECTS": resolve_output(
        "IP004_POST_SCOPE_OBJECTS",
        "ip004_post_scope_objects"
    ),

    "IP004_PHASE1_SUMMARY": resolve_output(
        "IP004_PHASE1_SUMMARY",
        "ip004_phase1_summary"
    ),

    "IP004_RELATIONSHIP_SUMMARY": resolve_output(
        "IP004_RELATIONSHIP_SUMMARY",
        "ip004_relationship_summary"
    ),

    "IP004_COMPUTER_MAPPING_SUMMARY": resolve_output(
        "IP004_COMPUTER_MAPPING_SUMMARY",
        "ip004_computer_mapping_summary"
    ),

    "IP004_POST_ATTRIBUTION_SUMMARY": resolve_output(
        "IP004_POST_ATTRIBUTION_SUMMARY",
        "ip004_post_attribution_summary"
    )
}


WRITTEN_TABLES = []


def with_runtime(
    dataframe
):
    runtime_values = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "rcaip_run_id": RCAIP_RUN_ID,
        "runtime_country_name": RCAIP_COUNTRY_NAME,
        "runtime_run_mode": RCAIP_RUN_MODE
    }

    output = dataframe

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


def save_output(
    dataframe,
    logical_name
):
    table_name = OUTPUTS[
        logical_name
    ]

    (
        with_runtime(dataframe)
        .write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    WRITTEN_TABLES.append(
        table_name
    )

    print(
        "Saved:",
        table_name
    )


# ---------------------------------------------------------------------------
# 2. Generic AD helpers
# ---------------------------------------------------------------------------


def resolve_col(
    dataframe,
    *names
):
    lookup = {
        str(column_name).lower(): column_name
        for column_name in dataframe.columns
    }

    for name in names:
        resolved = lookup.get(
            str(name).lower()
        )

        if resolved:
            return resolved

    return None


def opt_col(
    dataframe,
    data_type,
    *names
):
    resolved = resolve_col(
        dataframe,
        *names
    )

    if resolved is None:
        return F.lit(None).cast(
            data_type
        )

    escaped = resolved.replace(
        "`",
        "``"
    )

    return F.col(
        f"`{escaped}`"
    ).cast(
        data_type
    )


def opt_string(
    dataframe,
    *names
):
    return opt_col(
        dataframe,
        "string",
        *names
    )


def opt_long(
    dataframe,
    *names
):
    return opt_col(
        dataframe,
        "long",
        *names
    )


def opt_array(
    dataframe,
    *names
):
    return opt_col(
        dataframe,
        "array<string>",
        *names
    )


def as_col(
    expression
):
    if isinstance(
        expression,
        str
    ):
        return F.col(
            expression
        )

    return expression


def normalize_guid(
    expression
):
    expression = as_col(
        expression
    )

    value = F.lower(
        F.trim(
            expression.cast("string")
        )
    )

    return F.when(
        value.isNull()
        |
        (value == ""),
        None
    ).otherwise(
        value
    )


def normalize_dn(
    expression
):
    expression = as_col(
        expression
    )

    value = F.lower(
        F.trim(
            expression.cast("string")
        )
    )

    value = F.regexp_replace(
        value,
        r"\s*,\s*",
        ","
    )

    value = F.regexp_replace(
        value,
        r"\s*=\s*",
        "="
    )

    return F.when(
        value.isNull()
        |
        (value == ""),
        None
    ).otherwise(
        value
    )


def normalize_host(
    expression
):
    expression = as_col(
        expression
    )

    value = F.lower(
        F.trim(
            expression.cast("string")
        )
    )

    value = F.regexp_replace(
        value,
        r"\$$",
        ""
    )

    value = F.regexp_extract(
        value,
        r"^([^.]+)",
        1
    )

    return F.when(
        value.isNull()
        |
        (value == ""),
        None
    ).otherwise(
        value
    )


def parse_ad_timestamp(
    expression
):
    expression = as_col(
        expression
    )

    text = F.trim(
        expression.cast("string")
    )

    filetime = F.when(
        text.rlike(
            r"^[0-9]{15,20}$"
        )
        &
        (
            text.cast("decimal(20,0)")
            >
            0
        ),
        F.to_timestamp(
            F.from_unixtime(
                (
                    text.cast("decimal(20,0)")
                    /
                    F.lit(10000000)
                    -
                    F.lit(11644473600)
                ).cast("long")
            )
        )
    )

    return F.coalesce(
        filetime,
        F.to_timestamp(text),
        F.to_timestamp(
            text,
            "yyyy-MM-dd'T'HH:mm:ss.SSSX"
        ),
        F.to_timestamp(
            text,
            "yyyy-MM-dd'T'HH:mm:ssX"
        ),
        F.to_timestamp(
            text,
            "yyyy-MM-dd HH:mm:ss"
        ),
        F.to_timestamp(
            text,
            "yyyyMMddHHmmss.S'Z'"
        ),
        F.to_timestamp(
            text,
            "yyyyMMddHHmmss'Z'"
        )
    )


def uac_flag(
    expression,
    value
):
    return (
        expression.isNotNull()
        &
        (
            expression.bitwiseAND(
                F.lit(value)
            )
            !=
            0
        )
    )


def contains_token(
    expression,
    tokens
):
    expression = as_col(
        expression
    )

    text = F.lower(
        F.coalesce(
            expression.cast("string"),
            F.lit("")
        )
    )

    condition = F.lit(False)

    for token in tokens:
        condition = condition | text.contains(
            token
        )

    return condition


def dedup(
    dataframe,
    key="ad_object_key"
):
    hash_columns = [
        column_name
        for column_name in [
            key,
            "object_sid",
            "distinguished_name_key",
            "sam_account_name",
            "user_principal_name",
            "dns_host_name",
            "name",
            "when_changed_raw",
            "usn_changed",
            "last_logon_timestamp_raw",
            "user_account_control"
        ]
        if column_name in dataframe.columns
    ]

    tie_hash = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column_name).cast("string"),
                    F.lit("")
                )
                for column_name in hash_columns
            ]
        ),
        256
    )

    window_spec = (
        Window
        .partitionBy(key)
        .orderBy(
            F.col(
                "when_changed_timestamp"
            ).desc_nulls_last(),
            F.col(
                "usn_changed"
            ).desc_nulls_last(),
            F.col(
                "_tie_hash"
            ).desc()
        )
    )

    return (
        dataframe
        .withColumn(
            "_tie_hash",
            tie_hash
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                window_spec
            )
        )
        .filter(
            F.col("_rn") == 1
        )
        .drop(
            "_rn"
        )
    )


def require_columns(
    dataframe,
    logical_name,
    names
):
    missing = [
        name
        for name in names
        if resolve_col(
            dataframe,
            name
        ) is None
    ]

    if missing:
        raise RuntimeError(
            logical_name
            +
            " is missing required fields: "
            +
            " | ".join(missing)
        )


# ---------------------------------------------------------------------------
# 3. Post tokens and source contracts
# ---------------------------------------------------------------------------

post_tokens = []

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

if not post_tokens:
    raise RuntimeError(
        "No usable Post identity tokens were configured."
    )


privileged_regex = (
    r"(^|[^a-z0-9])"
    r"(admin|admins|administrator|administrators|domain admins|"
    r"enterprise admins|schema admins|account operators|server operators|"
    r"backup operators|print operators|dnsadmins|key admins|"
    r"enterprise key admins|group policy creator owners|remote desktop users|"
    r"network configuration operators|cryptographic operators|"
    r"hyper-v administrators|protected users)"
    r"([^a-z0-9]|$)"
)


broad_group_regex = (
    r"(^|[^a-z0-9])"
    r"(all users|all employees|all staff|everyone|authenticated users|"
    r"domain users|global users|enterprise users|department-wide|"
    r"department wide|worldwide|organization-wide|organization wide|"
    r"company-wide|company wide)"
    r"([^a-z0-9]|$)"
)


ad_users_raw = spark.table(
    INPUTS["users"]
)

ad_computers_raw = spark.table(
    INPUTS["computers"]
)

ad_groups_raw = spark.table(
    INPUTS["groups"]
)

asset_plane = (
    spark.table(
        INPUTS["asset_plane"]
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


required_ad = [
    "objectGUID",
    "distinguishedName",
    "objectClass",
    "objectCategory",
    "whenChanged",
    "uSNChanged",
    "memberOf",
    "managedBy"
]

require_columns(
    ad_users_raw,
    "AD_USERS_SOURCE",
    required_ad
)

require_columns(
    ad_computers_raw,
    "AD_COMPUTERS_SOURCE",
    required_ad
)

require_columns(
    ad_groups_raw,
    "AD_GROUPS_SOURCE",
    required_ad + [
        "member"
    ]
)


required_asset = {
    "post_name",
    "post_slug",
    "rcaip_run_id",
    "asset_key",
    "host_key",
    "cmdb_name",
    "cmdb_fqdn",
    "observed_names",
    "cmdb_present",
    "lifecycle_reconciliation_status"
}

missing_asset = sorted(
    required_asset
    -
    set(
        asset_plane.columns
    )
)

if missing_asset:
    raise RuntimeError(
        "ASSET_RECONCILIATION_SCORED is missing: "
        +
        " | ".join(
            missing_asset
        )
    )


asset_metrics = (
    asset_plane
    .agg(
        F.count("*").alias(
            "rows"
        ),
        F.countDistinct(
            "asset_key"
        ).alias(
            "keys"
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
            "post_mismatch"
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
            "run_mismatch"
        )
    )
    .first()
)

asset_plane_count = int(
    asset_metrics["rows"]
    or
    0
)

if asset_plane_count == 0:
    raise RuntimeError(
        "The scored Asset Plane is empty."
    )

if int(
    asset_metrics["keys"]
    or
    0
) != asset_plane_count:
    raise RuntimeError(
        "The scored Asset Plane is not one row per asset_key."
    )

if int(
    asset_metrics["post_mismatch"]
    or
    0
) > 0:
    raise RuntimeError(
        "The scored Asset Plane contains cross-Post rows."
    )

if int(
    asset_metrics["run_mismatch"]
    or
    0
) > 0:
    raise RuntimeError(
        "The scored Asset Plane belongs to another run."
    )


# ---------------------------------------------------------------------------
# 4. Asset hostname keys
# ---------------------------------------------------------------------------

asset_names = (
    asset_plane
    .select(
        "asset_key",
        "cmdb_present",
        "lifecycle_reconciliation_status",
        F.explode_outer(
            F.concat(
                F.array(
                    "host_key",
                    "cmdb_name",
                    "cmdb_fqdn"
                ),
                F.split(
                    F.coalesce(
                        F.col("observed_names"),
                        F.lit("")
                    ),
                    r"\s*,\s*"
                )
            )
        ).alias(
            "candidate_name"
        )
    )
    .withColumn(
        "asset_host_key",
        normalize_host(
            "candidate_name"
        )
    )
    .filter(
        F.col("asset_host_key").isNotNull()
    )
    .dropDuplicates([
        "asset_key",
        "asset_host_key"
    ])
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)

asset_host_keys = (
    asset_names
    .select(
        "asset_host_key"
    )
    .distinct()
)


# ---------------------------------------------------------------------------
# 5. Normalize groups and identify direct Post groups
# ---------------------------------------------------------------------------

group_base = (
    ad_groups_raw
    .select(
        normalize_guid(
            opt_string(
                ad_groups_raw,
                "objectGUID"
            )
        ).alias(
            "ad_object_key"
        ),
        normalize_guid(
            opt_string(
                ad_groups_raw,
                "objectSid"
            )
        ).alias(
            "object_sid"
        ),
        opt_string(
            ad_groups_raw,
            "distinguishedName"
        ).alias(
            "distinguished_name"
        ),
        opt_string(
            ad_groups_raw,
            "sAMAccountName"
        ).alias(
            "sam_account_name"
        ),
        opt_string(
            ad_groups_raw,
            "name"
        ).alias(
            "name"
        ),
        opt_string(
            ad_groups_raw,
            "description"
        ).alias(
            "description"
        ),
        opt_string(
            ad_groups_raw,
            "groupType"
        ).alias(
            "group_type"
        ),
        opt_long(
            ad_groups_raw,
            "adminCount"
        ).alias(
            "admin_count"
        ),
        opt_string(
            ad_groups_raw,
            "location"
        ).alias(
            "location"
        ),
        opt_string(
            ad_groups_raw,
            "managedBy"
        ).alias(
            "managed_by_dn_raw"
        ),
        opt_array(
            ad_groups_raw,
            "member"
        ).alias(
            "member_dn_array"
        ),
        opt_array(
            ad_groups_raw,
            "memberOf"
        ).alias(
            "member_of_dn_array"
        ),
        opt_string(
            ad_groups_raw,
            "whenCreated"
        ).alias(
            "when_created_raw"
        ),
        opt_string(
            ad_groups_raw,
            "whenChanged"
        ).alias(
            "when_changed_raw"
        ),
        opt_long(
            ad_groups_raw,
            "uSNChanged"
        ).alias(
            "usn_changed"
        )
    )
    .withColumn(
        "distinguished_name_key",
        normalize_dn(
            "distinguished_name"
        )
    )
    .withColumn(
        "managed_by_dn_key",
        normalize_dn(
            "managed_by_dn_raw"
        )
    )
    .withColumn(
        "when_created_timestamp",
        parse_ad_timestamp(
            "when_created_raw"
        )
    )
    .withColumn(
        "when_changed_timestamp",
        parse_ad_timestamp(
            "when_changed_raw"
        )
    )
    .withColumn(
        "search_text",
        F.concat_ws(
            " ",
            "distinguished_name",
            "sam_account_name",
            "name",
            "description",
            "location"
        )
    )
    .withColumn(
        "direct_post_token_match",
        F.when(
            contains_token(
                "search_text",
                post_tokens
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "privileged_name_indicator",
        F.when(
            F.lower(
                F.col("search_text")
            ).rlike(
                privileged_regex
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "admin_protected_indicator",
        F.when(
            F.col("admin_count") == 1,
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "member_count",
        F.coalesce(
            F.size(
                F.col("member_dn_array")
            ),
            F.lit(0)
        )
    )
    .withColumn(
        "memberof_count",
        F.coalesce(
            F.size(
                F.col("member_of_dn_array")
            ),
            F.lit(0)
        )
    )
    .withColumn(
        "broad_group_name_indicator",
        F.when(
            F.lower(
                F.col("search_text")
            ).rlike(
                broad_group_regex
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .filter(
        F.col("ad_object_key").isNotNull()
    )
    .filter(
        F.col("distinguished_name_key").isNotNull()
    )
)


direct_groups = (
    dedup(
        group_base
        .filter(
            F.col("direct_post_token_match") == "Y"
        )
    )
    .withColumn(
        "member_expansion_allowed",
        F.when(
            (
                F.col("member_count")
                <=
                F.lit(
                    IP004_MAX_DIRECT_GROUP_MEMBERS
                )
            )
            &
            (
                F.col("broad_group_name_indicator") == "N"
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "member_expansion_reason",
        F.when(
            F.col("broad_group_name_indicator") == "Y",
            "Broad or enterprise-style group name"
        )
        .when(
            F.col("member_count")
            >
            F.lit(
                IP004_MAX_DIRECT_GROUP_MEMBERS
            ),
            F.concat(
                F.lit(
                    "Member count exceeds bounded attribution limit of "
                ),
                F.lit(
                    str(
                        IP004_MAX_DIRECT_GROUP_MEMBERS
                    )
                )
            )
        )
        .otherwise(
            "Bounded direct Post group"
        )
    )
    .withColumn(
        "group_scope_role",
        F.lit(
            "DIRECT_POST_GROUP"
        )
    )
    .withColumn(
        "object_type",
        F.lit(
            "Group"
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


direct_group_dns = (
    direct_groups
    .select(
        F.col(
            "distinguished_name_key"
        ).alias(
            "group_dn_key"
        )
    )
    .distinct()
)


# Only bounded direct Post groups may contribute member-based user scope.
# One-hop support groups never contribute member-based Post attribution.
allowed_direct_member_dns = (
    direct_groups
    .filter(
        F.col("member_expansion_allowed") == "Y"
    )
    .select(
        F.col("ad_object_key").alias(
            "attribution_group_key"
        ),
        F.col("name").alias(
            "attribution_group_name"
        ),
        F.explode_outer(
            F.col("member_dn_array")
        ).alias(
            "member_dn"
        )
    )
    .filter(
        F.col("member_dn").isNotNull()
    )
    .withColumn(
        "member_dn_key",
        normalize_dn(
            "member_dn"
        )
    )
    .filter(
        F.col("member_dn_key").isNotNull()
    )
    .select(
        "attribution_group_key",
        "attribution_group_name",
        "member_dn_key"
    )
    .dropDuplicates([
        "attribution_group_key",
        "member_dn_key"
    ])
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 6. Direct Post computers
#
# Group membership is retained as context but cannot independently place a
# computer into the Post boundary. Computers require Asset Plane or direct
# Post-attribute evidence.
# ---------------------------------------------------------------------------

computer_base = (
    ad_computers_raw
    .select(
        normalize_guid(
            opt_string(
                ad_computers_raw,
                "objectGUID"
            )
        ).alias(
            "ad_object_key"
        ),
        normalize_guid(
            opt_string(
                ad_computers_raw,
                "objectSid"
            )
        ).alias(
            "object_sid"
        ),
        opt_string(
            ad_computers_raw,
            "distinguishedName"
        ).alias(
            "distinguished_name"
        ),
        opt_string(
            ad_computers_raw,
            "sAMAccountName"
        ).alias(
            "sam_account_name"
        ),
        opt_string(
            ad_computers_raw,
            "name"
        ).alias(
            "name"
        ),
        opt_string(
            ad_computers_raw,
            "dNSHostName",
            "dnsHostName"
        ).alias(
            "dns_host_name"
        ),
        opt_string(
            ad_computers_raw,
            "operatingSystem"
        ).alias(
            "operating_system"
        ),
        opt_string(
            ad_computers_raw,
            "operatingSystemVersion"
        ).alias(
            "operating_system_version"
        ),
        opt_string(
            ad_computers_raw,
            "location"
        ).alias(
            "location"
        ),
        opt_string(
            ad_computers_raw,
            "physicalDeliveryOfficeName"
        ).alias(
            "office"
        ),
        opt_string(
            ad_computers_raw,
            "managedBy"
        ).alias(
            "managed_by_dn_raw"
        ),
        opt_array(
            ad_computers_raw,
            "memberOf"
        ).alias(
            "member_of_dn_array"
        ),
        opt_string(
            ad_computers_raw,
            "whenCreated"
        ).alias(
            "when_created_raw"
        ),
        opt_string(
            ad_computers_raw,
            "whenChanged"
        ).alias(
            "when_changed_raw"
        ),
        opt_long(
            ad_computers_raw,
            "uSNChanged"
        ).alias(
            "usn_changed"
        ),
        opt_string(
            ad_computers_raw,
            "lastLogon"
        ).alias(
            "last_logon_raw"
        ),
        opt_string(
            ad_computers_raw,
            "lastLogonTimestamp"
        ).alias(
            "last_logon_timestamp_raw"
        ),
        opt_string(
            ad_computers_raw,
            "pwdLastSet"
        ).alias(
            "pwd_last_set_raw"
        ),
        opt_long(
            ad_computers_raw,
            "userAccountControl"
        ).alias(
            "user_account_control"
        ),
        opt_long(
            ad_computers_raw,
            "adminCount"
        ).alias(
            "admin_count"
        )
    )
    .withColumn(
        "distinguished_name_key",
        normalize_dn(
            "distinguished_name"
        )
    )
    .withColumn(
        "managed_by_dn_key",
        normalize_dn(
            "managed_by_dn_raw"
        )
    )
    .withColumn(
        "host_key",
        F.coalesce(
            normalize_host(
                "dns_host_name"
            ),
            normalize_host(
                "sam_account_name"
            ),
            normalize_host(
                "name"
            )
        )
    )
    .withColumn(
        "when_created_timestamp",
        parse_ad_timestamp(
            "when_created_raw"
        )
    )
    .withColumn(
        "when_changed_timestamp",
        parse_ad_timestamp(
            "when_changed_raw"
        )
    )
    .withColumn(
        "last_logon_timestamp",
        F.coalesce(
            parse_ad_timestamp(
                "last_logon_timestamp_raw"
            ),
            parse_ad_timestamp(
                "last_logon_raw"
            )
        )
    )
    .withColumn(
        "pwd_last_set_timestamp",
        parse_ad_timestamp(
            "pwd_last_set_raw"
        )
    )
    .withColumn(
        "search_text",
        F.concat_ws(
            " ",
            "distinguished_name",
            "office",
            "location",
            "dns_host_name",
            "sam_account_name",
            "name"
        )
    )
    .withColumn(
        "direct_post_token_match",
        F.when(
            contains_token(
                "search_text",
                post_tokens
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .filter(
        F.col("ad_object_key").isNotNull()
    )
    .filter(
        F.col("distinguished_name_key").isNotNull()
    )
)


computer_group_context = (
    computer_base
    .select(
        "ad_object_key",
        F.explode_outer(
            F.col("member_of_dn_array")
        ).alias(
            "group_dn"
        )
    )
    .filter(
        F.col("group_dn").isNotNull()
    )
    .withColumn(
        "group_dn_key",
        normalize_dn(
            "group_dn"
        )
    )
    .join(
        F.broadcast(
            direct_group_dns
        ),
        "group_dn_key",
        "inner"
    )
    .select(
        "ad_object_key"
    )
    .distinct()
    .withColumn(
        "direct_post_group_member_indicator",
        F.lit("Y")
    )
)


computer_scoped = (
    computer_base.alias("c")
    .join(
        F.broadcast(
            asset_host_keys
        ).alias("a"),
        F.col("c.host_key")
        ==
        F.col("a.asset_host_key"),
        "left"
    )
    .join(
        computer_group_context.alias("g"),
        F.col("c.ad_object_key")
        ==
        F.col("g.ad_object_key"),
        "left"
    )
    .filter(
        (
            F.col(
                "c.direct_post_token_match"
            )
            ==
            "Y"
        )
        |
        F.col(
            "a.asset_host_key"
        ).isNotNull()
    )
    .select(
        "c.*",
        F.when(
            F.col("a.asset_host_key").isNotNull(),
            "Y"
        )
        .otherwise(
            "N"
        ).alias(
            "asset_host_key_match_indicator"
        ),
        F.coalesce(
            F.col(
                "g.direct_post_group_member_indicator"
            ),
            F.lit("N")
        ).alias(
            "direct_post_group_member_indicator"
        )
    )
    .dropDuplicates([
        "ad_object_key",
        "host_key"
    ])
)


ad_computers_norm = (
    dedup(
        computer_scoped
    )
    .withColumn(
        "account_enabled",
        F.when(
            uac_flag(
                F.col("user_account_control"),
                2
            ),
            "N"
        )
        .otherwise(
            "Y"
        )
    )
    .withColumn(
        "days_since_last_logon",
        F.when(
            F.col(
                "last_logon_timestamp"
            ).isNotNull(),
            F.greatest(
                F.lit(0),
                F.datediff(
                    F.current_date(),
                    F.to_date(
                        F.col(
                            "last_logon_timestamp"
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
        "computer_lifecycle_status",
        F.when(
            F.col("account_enabled") == "N",
            "Disabled"
        )
        .when(
            F.col(
                "last_logon_timestamp"
            ).isNull(),
            "Enabled / No Logon Timestamp"
        )
        .when(
            F.col("days_since_last_logon")
            >
            IP004_VERY_STALE_DAYS,
            "Enabled / Over 180 Days"
        )
        .when(
            F.col("days_since_last_logon")
            >
            IP004_STALE_DAYS,
            "Enabled / 91-180 Days"
        )
        .otherwise(
            "Enabled / Active Within 90 Days"
        )
    )
    .withColumn(
        "scope_role",
        F.when(
            F.col(
                "asset_host_key_match_indicator"
            )
            ==
            "Y",
            "DIRECT_POST_COMPUTER_ASSET_MATCH"
        )
        .otherwise(
            "DIRECT_POST_COMPUTER_TOKEN_MATCH"
        )
    )
    .withColumn(
        "object_type",
        F.lit(
            "Computer"
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 7. Direct Post users
#
# A user may enter scope through its own Post attributes or as a direct member
# of a bounded, explicitly Post-attributed group. Membership in a broad group
# or any one-hop support group cannot place a user into the Post boundary.
# ---------------------------------------------------------------------------

user_base = (
    ad_users_raw
    .select(
        normalize_guid(
            opt_string(
                ad_users_raw,
                "objectGUID"
            )
        ).alias(
            "ad_object_key"
        ),
        normalize_guid(
            opt_string(
                ad_users_raw,
                "objectSid"
            )
        ).alias(
            "object_sid"
        ),
        opt_string(
            ad_users_raw,
            "distinguishedName"
        ).alias(
            "distinguished_name"
        ),
        opt_string(
            ad_users_raw,
            "objectClass"
        ).alias(
            "object_class_text"
        ),
        opt_string(
            ad_users_raw,
            "objectCategory"
        ).alias(
            "object_category"
        ),
        opt_string(
            ad_users_raw,
            "sAMAccountType"
        ).alias(
            "sam_account_type"
        ),
        opt_string(
            ad_users_raw,
            "sAMAccountName"
        ).alias(
            "sam_account_name"
        ),
        opt_string(
            ad_users_raw,
            "userPrincipalName"
        ).alias(
            "user_principal_name"
        ),
        opt_string(
            ad_users_raw,
            "name"
        ).alias(
            "name"
        ),
        opt_string(
            ad_users_raw,
            "displayName"
        ).alias(
            "display_name"
        ),
        opt_string(
            ad_users_raw,
            "employeeID"
        ).alias(
            "employee_id"
        ),
        opt_string(
            ad_users_raw,
            "employeeType"
        ).alias(
            "employee_type"
        ),
        opt_string(
            ad_users_raw,
            "mail"
        ).alias(
            "mail"
        ),
        opt_string(
            ad_users_raw,
            "department"
        ).alias(
            "department"
        ),
        opt_string(
            ad_users_raw,
            "company"
        ).alias(
            "company"
        ),
        opt_string(
            ad_users_raw,
            "title"
        ).alias(
            "title"
        ),
        opt_string(
            ad_users_raw,
            "physicalDeliveryOfficeName"
        ).alias(
            "office"
        ),
        opt_string(
            ad_users_raw,
            "location"
        ).alias(
            "location"
        ),
        opt_string(
            ad_users_raw,
            "manager"
        ).alias(
            "manager_dn_raw"
        ),
        opt_string(
            ad_users_raw,
            "managedBy"
        ).alias(
            "managed_by_dn_raw"
        ),
        opt_array(
            ad_users_raw,
            "memberOf"
        ).alias(
            "member_of_dn_array"
        ),
        opt_string(
            ad_users_raw,
            "whenCreated"
        ).alias(
            "when_created_raw"
        ),
        opt_string(
            ad_users_raw,
            "whenChanged"
        ).alias(
            "when_changed_raw"
        ),
        opt_long(
            ad_users_raw,
            "uSNChanged"
        ).alias(
            "usn_changed"
        ),
        opt_string(
            ad_users_raw,
            "lastLogon"
        ).alias(
            "last_logon_raw"
        ),
        opt_string(
            ad_users_raw,
            "lastLogonTimestamp"
        ).alias(
            "last_logon_timestamp_raw"
        ),
        opt_string(
            ad_users_raw,
            "pwdLastSet"
        ).alias(
            "pwd_last_set_raw"
        ),
        opt_long(
            ad_users_raw,
            "userAccountControl"
        ).alias(
            "user_account_control"
        ),
        opt_long(
            ad_users_raw,
            "adminCount"
        ).alias(
            "admin_count"
        )
    )
    .withColumn(
        "distinguished_name_key",
        normalize_dn(
            "distinguished_name"
        )
    )
    .withColumn(
        "manager_dn_key",
        normalize_dn(
            "manager_dn_raw"
        )
    )
    .withColumn(
        "managed_by_dn_key",
        normalize_dn(
            "managed_by_dn_raw"
        )
    )
    .withColumn(
        "type_text",
        F.lower(
            F.concat_ws(
                " ",
                "object_class_text",
                "object_category",
                "sam_account_type"
            )
        )
    )
    .withColumn(
        "is_computer",
        F.col("type_text").contains(
            "computer"
        )
        |
        F.lower(
            F.coalesce(
                F.col("sam_account_name"),
                F.lit("")
            )
        ).endswith(
            "$"
        )
    )
    .withColumn(
        "is_user_person",
        (
            F.col("type_text").contains(
                "user"
            )
            |
            F.col("type_text").contains(
                "person"
            )
        )
        &
        (~F.col("is_computer"))
    )
    .withColumn(
        "when_created_timestamp",
        parse_ad_timestamp(
            "when_created_raw"
        )
    )
    .withColumn(
        "when_changed_timestamp",
        parse_ad_timestamp(
            "when_changed_raw"
        )
    )
    .withColumn(
        "last_logon_timestamp",
        F.coalesce(
            parse_ad_timestamp(
                "last_logon_timestamp_raw"
            ),
            parse_ad_timestamp(
                "last_logon_raw"
            )
        )
    )
    .withColumn(
        "pwd_last_set_timestamp",
        parse_ad_timestamp(
            "pwd_last_set_raw"
        )
    )
    .withColumn(
        "search_text",
        F.concat_ws(
            " ",
            "distinguished_name",
            "office",
            "location",
            "department",
            "company",
            "title",
            "user_principal_name",
            "mail",
            "sam_account_name",
            "name",
            "display_name"
        )
    )
    .withColumn(
        "direct_post_token_match",
        F.when(
            contains_token(
                "search_text",
                post_tokens
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .filter(
        F.col("ad_object_key").isNotNull()
    )
    .filter(
        F.col("distinguished_name_key").isNotNull()
    )
    .filter(
        F.col("is_user_person")
    )
)


user_group_attribution = (
    user_base.alias("u")
    .join(
        F.broadcast(
            allowed_direct_member_dns
        ).alias("m"),
        F.col(
            "u.distinguished_name_key"
        )
        ==
        F.col(
            "m.member_dn_key"
        ),
        "left"
    )
    .groupBy(
        "u.ad_object_key"
    )
    .agg(
        F.max(
            F.when(
                F.col(
                    "m.member_dn_key"
                ).isNotNull(),
                1
            )
            .otherwise(0)
        ).alias(
            "direct_post_group_member_flag"
        ),
        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    F.col(
                        "m.attribution_group_name"
                    )
                )
            )
        ).alias(
            "attribution_group_names"
        )
    )
)


user_scoped = (
    user_base.alias("u")
    .join(
        user_group_attribution.alias("g"),
        F.col("u.ad_object_key")
        ==
        F.col("g.ad_object_key"),
        "left"
    )
    .filter(
        (
            F.col(
                "u.direct_post_token_match"
            )
            ==
            "Y"
        )
        |
        (
            F.coalesce(
                F.col(
                    "g.direct_post_group_member_flag"
                ),
                F.lit(0)
            )
            ==
            1
        )
    )
    .select(
        "u.*",
        F.when(
            F.coalesce(
                F.col(
                    "g.direct_post_group_member_flag"
                ),
                F.lit(0)
            )
            ==
            1,
            "Y"
        )
        .otherwise(
            "N"
        ).alias(
            "direct_post_group_member_indicator"
        ),
        F.coalesce(
            F.col(
                "g.attribution_group_names"
            ),
            F.lit("")
        ).alias(
            "attribution_group_names"
        )
    )
)


ad_users_norm = (
    dedup(
        user_scoped
    )
    .withColumn(
        "account_enabled",
        F.when(
            uac_flag(
                F.col("user_account_control"),
                2
            ),
            "N"
        )
        .otherwise(
            "Y"
        )
    )
    .withColumn(
        "password_never_expires",
        F.when(
            uac_flag(
                F.col("user_account_control"),
                65536
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "smartcard_required",
        F.when(
            uac_flag(
                F.col("user_account_control"),
                262144
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "does_not_require_pre_auth",
        F.when(
            uac_flag(
                F.col("user_account_control"),
                4194304
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "days_since_last_logon",
        F.when(
            F.col(
                "last_logon_timestamp"
            ).isNotNull(),
            F.greatest(
                F.lit(0),
                F.datediff(
                    F.current_date(),
                    F.to_date(
                        F.col(
                            "last_logon_timestamp"
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
        "account_lifecycle_status",
        F.when(
            F.col("account_enabled") == "N",
            "Disabled"
        )
        .when(
            F.col(
                "last_logon_timestamp"
            ).isNull(),
            "Enabled / No Logon Timestamp"
        )
        .when(
            F.col("days_since_last_logon")
            >
            IP004_VERY_STALE_DAYS,
            "Enabled / Over 180 Days"
        )
        .when(
            F.col("days_since_last_logon")
            >
            IP004_STALE_DAYS,
            "Enabled / 91-180 Days"
        )
        .otherwise(
            "Enabled / Active Within 90 Days"
        )
    )
    .withColumn(
        "scope_role",
        F.when(
            F.col("direct_post_token_match") == "Y",
            "DIRECT_POST_USER_TOKEN_MATCH"
        )
        .otherwise(
            "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP"
        )
    )
    .withColumn(
        "object_type",
        F.lit(
            "User"
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 8. One-hop support groups
#
# A one-hop support group is referenced directly through memberOf by a direct
# Post user, computer, or direct Post group. Its members and parent groups are
# not traversed or imported.
# ---------------------------------------------------------------------------

one_hop_group_dns = (
    ad_users_norm
    .select(
        F.explode_outer(
            F.col("member_of_dn_array")
        ).alias(
            "group_dn"
        )
    )
    .unionByName(
        ad_computers_norm
        .select(
            F.explode_outer(
                F.col("member_of_dn_array")
            ).alias(
                "group_dn"
            )
        )
    )
    .unionByName(
        direct_groups
        .select(
            F.explode_outer(
                F.col("member_of_dn_array")
            ).alias(
                "group_dn"
            )
        )
    )
    .filter(
        F.col("group_dn").isNotNull()
    )
    .withColumn(
        "group_dn_key",
        normalize_dn(
            "group_dn"
        )
    )
    .filter(
        F.col("group_dn_key").isNotNull()
    )
    .select(
        "group_dn_key"
    )
    .distinct()
    .join(
        F.broadcast(
            direct_group_dns
        ),
        "group_dn_key",
        "left_anti"
    )
)


one_hop_support_groups = (
    group_base.alias("g")
    .join(
        F.broadcast(
            one_hop_group_dns
        ).alias("r"),
        F.col(
            "g.distinguished_name_key"
        )
        ==
        F.col(
            "r.group_dn_key"
        ),
        "inner"
    )
    .select(
        "g.*"
    )
    .transform(
        dedup
    )
    .withColumn(
        "member_expansion_allowed",
        F.lit("N")
    )
    .withColumn(
        "member_expansion_reason",
        F.lit(
            "One-hop context only; member expansion prohibited"
        )
    )
    .withColumn(
        "group_scope_role",
        F.lit(
            "ONE_HOP_SUPPORT_GROUP"
        )
    )
    .withColumn(
        "object_type",
        F.lit(
            "Group"
        )
    )
)


ad_groups_norm = (
    direct_groups
    .unionByName(
        one_hop_support_groups,
        allowMissingColumns=True
    )
    .dropDuplicates([
        "ad_object_key"
    ])
    .withColumn(
        "has_managed_by",
        F.when(
            F.col("managed_by_dn_key").isNotNull(),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .withColumn(
        "privileged_indicator",
        F.when(
            (
                F.col(
                    "admin_protected_indicator"
                )
                ==
                "Y"
            )
            |
            (
                F.col(
                    "privileged_name_indicator"
                )
                ==
                "Y"
            ),
            "Y"
        )
        .otherwise(
            "N"
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 9. Cell 2 readiness checks
# ---------------------------------------------------------------------------

scope_metrics = (
    ad_users_norm
    .agg(
        F.count("*").alias(
            "direct_user_count"
        ),
        F.sum(
            F.when(
                F.col(
                    "direct_post_token_match"
                )
                ==
                "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "token_user_count"
        ),
        F.sum(
            F.when(
                F.col(
                    "scope_role"
                )
                ==
                "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP",
                1
            )
            .otherwise(0)
        ).alias(
            "group_derived_user_count"
        )
    )
    .first()
)

computer_count = ad_computers_norm.count()

group_metrics = (
    ad_groups_norm
    .agg(
        F.count("*").alias(
            "scoped_group_count"
        ),
        F.sum(
            F.when(
                F.col("group_scope_role")
                ==
                "DIRECT_POST_GROUP",
                1
            )
            .otherwise(0)
        ).alias(
            "direct_group_count"
        ),
        F.sum(
            F.when(
                F.col("group_scope_role")
                ==
                "ONE_HOP_SUPPORT_GROUP",
                1
            )
            .otherwise(0)
        ).alias(
            "support_group_count"
        ),
        F.sum(
            F.when(
                (
                    F.col("group_scope_role")
                    ==
                    "DIRECT_POST_GROUP"
                )
                &
                (
                    F.col(
                        "member_expansion_allowed"
                    )
                    ==
                    "Y"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "bounded_direct_group_count"
        ),
        F.sum(
            F.when(
                (
                    F.col("group_scope_role")
                    ==
                    "DIRECT_POST_GROUP"
                )
                &
                (
                    F.col(
                        "member_expansion_allowed"
                    )
                    ==
                    "N"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "nonexpanded_direct_group_count"
        )
    )
    .first()
)

print("=" * 100)
print("RCAIP IP-004 - STRICT POST SCOPE")
print("=" * 100)
print("Status: READY")
print("Runtime Post:", RCAIP_POST_NAME)
print("Runtime Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Scored Asset Plane rows:", asset_plane_count)
print(
    "Direct Post users:",
    int(
        scope_metrics["direct_user_count"]
        or
        0
    )
)
print(
    "  User attribute-token matches:",
    int(
        scope_metrics["token_user_count"]
        or
        0
    )
)
print(
    "  Bounded direct-group-derived users:",
    int(
        scope_metrics["group_derived_user_count"]
        or
        0
    )
)
print("Direct Post computers:", computer_count)
print(
    "Direct Post groups:",
    int(
        group_metrics["direct_group_count"]
        or
        0
    )
)
print(
    "  Direct groups allowed for bounded member attribution:",
    int(
        group_metrics["bounded_direct_group_count"]
        or
        0
    )
)
print(
    "  Direct groups retained without member expansion:",
    int(
        group_metrics["nonexpanded_direct_group_count"]
        or
        0
    )
)
print(
    "One-hop support groups:",
    int(
        group_metrics["support_group_count"]
        or
        0
    )
)
print(
    "Maximum direct-group member attribution threshold:",
    IP004_MAX_DIRECT_GROUP_MEMBERS
)
print("Recursive group traversal performed: 0")
print("One-hop support-group members imported: 0")
print("Automatic changes or findings created: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-004 - IDENTITY AND ACTIVE DIRECTORY HYGIENE
# Cell 3 - Strict identity graph, durable foundation, and certification
#
# Requires Cell 2 objects in the same notebook session.
#
# Relationship model:
#   - memberOf edges originate only from direct Post users, direct Post
#     computers, and direct Post groups.
#   - Targets are limited to direct Post or one-hop support groups.
#   - Group-member edges originate only from bounded direct Post groups.
#   - Group-member targets must already be direct Post identities.
#   - One-hop support-group members and parents are never expanded.
# ============================================================================


# ---------------------------------------------------------------------------
# 10. Scope-role directory
# ---------------------------------------------------------------------------

ad_object_directory = (
    ad_users_norm
    .select(
        "ad_object_key",
        "object_sid",
        "distinguished_name",
        "distinguished_name_key",
        "object_type",
        "scope_role",
        F.coalesce(
            F.col("display_name"),
            F.col("name"),
            F.col("sam_account_name")
        ).alias(
            "object_name"
        ),
        F.col(
            "sam_account_name"
        ).alias(
            "account_or_host_name"
        ),
        "direct_post_token_match"
    )
    .unionByName(
        ad_computers_norm
        .select(
            "ad_object_key",
            "object_sid",
            "distinguished_name",
            "distinguished_name_key",
            "object_type",
            "scope_role",
            F.coalesce(
                F.col("dns_host_name"),
                F.col("name"),
                F.col("sam_account_name")
            ).alias(
                "object_name"
            ),
            F.col(
                "host_key"
            ).alias(
                "account_or_host_name"
            ),
            "direct_post_token_match"
        )
    )
    .unionByName(
        ad_groups_norm
        .select(
            "ad_object_key",
            "object_sid",
            "distinguished_name",
            "distinguished_name_key",
            "object_type",
            F.col(
                "group_scope_role"
            ).alias(
                "scope_role"
            ),
            F.coalesce(
                F.col("name"),
                F.col("sam_account_name")
            ).alias(
                "object_name"
            ),
            F.col(
                "sam_account_name"
            ).alias(
                "account_or_host_name"
            ),
            "direct_post_token_match"
        )
    )
    .dropDuplicates([
        "ad_object_key"
    ])
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


object_dn_window = (
    Window
    .partitionBy(
        "distinguished_name_key"
    )
    .orderBy(
        F.when(
            F.col("object_type") == "User",
            1
        )
        .when(
            F.col("object_type") == "Computer",
            2
        )
        .when(
            F.col("scope_role") == "DIRECT_POST_GROUP",
            3
        )
        .when(
            F.col("scope_role") == "ONE_HOP_SUPPORT_GROUP",
            4
        )
        .otherwise(5),
        "ad_object_key"
    )
)


object_by_dn = (
    ad_object_directory
    .withColumn(
        "_rn",
        F.row_number().over(
            object_dn_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .drop(
        "_rn"
    )
)


direct_object_lookup = (
    object_by_dn
    .filter(
        F.col("scope_role")
        !=
        "ONE_HOP_SUPPORT_GROUP"
    )
)


# ---------------------------------------------------------------------------
# 11. memberOf bridge - direct sources to one-hop group targets only
# ---------------------------------------------------------------------------

memberof_sources = (
    ad_users_norm
    .select(
        "ad_object_key",
        "object_type",
        "scope_role",
        "distinguished_name_key",
        F.explode_outer(
            F.col("member_of_dn_array")
        ).alias(
            "group_dn_raw"
        )
    )
    .unionByName(
        ad_computers_norm
        .select(
            "ad_object_key",
            "object_type",
            "scope_role",
            "distinguished_name_key",
            F.explode_outer(
                F.col("member_of_dn_array")
            ).alias(
                "group_dn_raw"
            )
        )
    )
    .unionByName(
        direct_groups
        .select(
            "ad_object_key",
            "object_type",
            F.col(
                "group_scope_role"
            ).alias(
                "scope_role"
            ),
            "distinguished_name_key",
            F.explode_outer(
                F.col("member_of_dn_array")
            ).alias(
                "group_dn_raw"
            )
        )
    )
    .filter(
        F.col("group_dn_raw").isNotNull()
    )
    .withColumn(
        "group_dn_key",
        normalize_dn(
            "group_dn_raw"
        )
    )
    .filter(
        F.col("group_dn_key").isNotNull()
    )
    .dropDuplicates([
        "ad_object_key",
        "group_dn_key"
    ])
)


group_lookup = (
    ad_groups_norm
    .select(
        F.col(
            "ad_object_key"
        ).alias(
            "resolved_group_key"
        ),
        F.col(
            "distinguished_name_key"
        ).alias(
            "resolved_group_dn_key"
        ),
        F.col("name").alias(
            "resolved_group_name"
        ),
        F.col(
            "sam_account_name"
        ).alias(
            "resolved_group_account_name"
        ),
        F.col("admin_count").alias(
            "resolved_group_admin_count"
        ),
        F.col(
            "privileged_indicator"
        ).alias(
            "resolved_group_privileged_indicator"
        ),
        F.col(
            "direct_post_token_match"
        ).alias(
            "resolved_group_post_token_match"
        ),
        F.col(
            "group_scope_role"
        ).alias(
            "resolved_group_scope_role"
        )
    )
)


ad_memberof_bridge = (
    memberof_sources.alias("m")
    .join(
        F.broadcast(
            group_lookup
        ).alias("g"),
        F.col("m.group_dn_key")
        ==
        F.col(
            "g.resolved_group_dn_key"
        ),
        "inner"
    )
    .withColumn(
        "relationship_scope",
        F.when(
            F.col(
                "resolved_group_scope_role"
            )
            ==
            "DIRECT_POST_GROUP",
            "DIRECT_IDENTITY_TO_DIRECT_POST_GROUP"
        )
        .otherwise(
            "DIRECT_IDENTITY_TO_ONE_HOP_SUPPORT_GROUP"
        )
    )
    .withColumn(
        "relationship_resolution_status",
        F.lit(
            "Resolved Within Strict Scope"
        )
    )
    .select(
        "m.ad_object_key",
        "m.object_type",
        "m.scope_role",
        "m.distinguished_name_key",
        "m.group_dn_raw",
        "m.group_dn_key",
        "resolved_group_key",
        "resolved_group_name",
        "resolved_group_account_name",
        "resolved_group_admin_count",
        "resolved_group_privileged_indicator",
        "resolved_group_post_token_match",
        "resolved_group_scope_role",
        "relationship_scope",
        "relationship_resolution_status"
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 12. Group-member bridge - bounded direct groups to direct identities only
# ---------------------------------------------------------------------------

bounded_direct_group_members = (
    direct_groups
    .filter(
        F.col("member_expansion_allowed") == "Y"
    )
    .select(
        F.col("ad_object_key").alias(
            "group_key"
        ),
        F.col(
            "distinguished_name_key"
        ).alias(
            "group_dn_key"
        ),
        F.col("name").alias(
            "group_name"
        ),
        F.col(
            "sam_account_name"
        ).alias(
            "group_account_name"
        ),
        F.col(
            "group_scope_role"
        ),
        F.explode_outer(
            F.col("member_dn_array")
        ).alias(
            "member_dn_raw"
        )
    )
    .filter(
        F.col("member_dn_raw").isNotNull()
    )
    .withColumn(
        "member_dn_key",
        normalize_dn(
            "member_dn_raw"
        )
    )
    .filter(
        F.col("member_dn_key").isNotNull()
    )
    .dropDuplicates([
        "group_key",
        "member_dn_key"
    ])
)


direct_member_lookup = (
    direct_object_lookup
    .select(
        F.col(
            "distinguished_name_key"
        ).alias(
            "resolved_object_dn_key"
        ),
        F.col(
            "ad_object_key"
        ).alias(
            "resolved_member_key"
        ),
        F.col(
            "object_type"
        ).alias(
            "resolved_member_type"
        ),
        F.col(
            "object_name"
        ).alias(
            "resolved_member_name"
        ),
        F.col(
            "scope_role"
        ).alias(
            "resolved_member_scope_role"
        ),
        F.col(
            "direct_post_token_match"
        ).alias(
            "resolved_member_post_token_match"
        )
    )
)


ad_group_member_bridge = (
    bounded_direct_group_members.alias("gm")
    .join(
        F.broadcast(
            direct_member_lookup
        ).alias("o"),
        F.col("gm.member_dn_key")
        ==
        F.col(
            "o.resolved_object_dn_key"
        ),
        "inner"
    )
    .withColumn(
        "relationship_scope",
        F.lit(
            "BOUNDED_DIRECT_POST_GROUP_TO_DIRECT_POST_IDENTITY"
        )
    )
    .withColumn(
        "relationship_resolution_status",
        F.lit(
            "Resolved Direct Identity"
        )
    )
    .select(
        "gm.group_key",
        "gm.group_dn_key",
        "gm.group_name",
        "gm.group_account_name",
        "gm.group_scope_role",
        "gm.member_dn_raw",
        "gm.member_dn_key",
        "resolved_member_key",
        "resolved_member_type",
        "resolved_member_name",
        "resolved_member_scope_role",
        "resolved_member_post_token_match",
        "relationship_scope",
        "relationship_resolution_status"
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 13. managedBy bridge - retain ownership context without scope expansion
# ---------------------------------------------------------------------------

managedby_sources = (
    ad_users_norm
    .select(
        "ad_object_key",
        "object_type",
        "scope_role",
        "distinguished_name_key",
        "managed_by_dn_raw",
        "managed_by_dn_key"
    )
    .unionByName(
        ad_computers_norm
        .select(
            "ad_object_key",
            "object_type",
            "scope_role",
            "distinguished_name_key",
            "managed_by_dn_raw",
            "managed_by_dn_key"
        )
    )
    .unionByName(
        ad_groups_norm
        .select(
            "ad_object_key",
            "object_type",
            F.col(
                "group_scope_role"
            ).alias(
                "scope_role"
            ),
            "distinguished_name_key",
            "managed_by_dn_raw",
            "managed_by_dn_key"
        )
    )
    .filter(
        F.col("managed_by_dn_key").isNotNull()
    )
    .dropDuplicates([
        "ad_object_key",
        "managed_by_dn_key"
    ])
)


manager_lookup = (
    object_by_dn
    .select(
        F.col(
            "distinguished_name_key"
        ).alias(
            "resolved_manager_dn_key"
        ),
        F.col(
            "ad_object_key"
        ).alias(
            "resolved_manager_key"
        ),
        F.col(
            "object_type"
        ).alias(
            "resolved_manager_type"
        ),
        F.col(
            "object_name"
        ).alias(
            "resolved_manager_name"
        ),
        F.col(
            "scope_role"
        ).alias(
            "resolved_manager_scope_role"
        )
    )
)


ad_managedby_bridge = (
    managedby_sources.alias("m")
    .join(
        F.broadcast(
            manager_lookup
        ).alias("r"),
        F.col("m.managed_by_dn_key")
        ==
        F.col(
            "r.resolved_manager_dn_key"
        ),
        "left"
    )
    .withColumn(
        "relationship_resolution_status",
        F.when(
            F.col(
                "resolved_manager_key"
            ).isNotNull(),
            "Resolved Within Strict Scope"
        )
        .otherwise(
            "External or Unresolved ManagedBy DN"
        )
    )
    .select(
        "m.ad_object_key",
        "m.object_type",
        "m.scope_role",
        "m.distinguished_name_key",
        "m.managed_by_dn_raw",
        "m.managed_by_dn_key",
        "resolved_manager_key",
        "resolved_manager_type",
        "resolved_manager_name",
        "resolved_manager_scope_role",
        "relationship_resolution_status"
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 14. Computer-to-Asset-Plane mapping
# ---------------------------------------------------------------------------

computer_matches = (
    ad_computers_norm.alias("c")
    .join(
        F.broadcast(
            asset_names
        ).alias("a"),
        F.col("c.host_key")
        ==
        F.col("a.asset_host_key"),
        "left"
    )
    .groupBy(
        "c.ad_object_key"
    )
    .agg(
        F.countDistinct(
            "a.asset_key"
        ).alias(
            "asset_match_count"
        ),
        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "a.asset_key"
                )
            )
        ).alias(
            "matched_asset_keys"
        ),
        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "a.lifecycle_reconciliation_status"
                )
            )
        ).alias(
            "matched_asset_lifecycle_statuses"
        ),
        F.max(
            F.when(
                F.col("a.cmdb_present") == "Y",
                1
            )
            .otherwise(0)
        ).alias(
            "matched_cmdb_indicator"
        )
    )
)


computer_asset_map = (
    ad_computers_norm.alias("c")
    .join(
        computer_matches.alias("m"),
        F.col("c.ad_object_key")
        ==
        F.col("m.ad_object_key"),
        "left"
    )
    .select(
        "c.*",
        F.coalesce(
            F.col("m.asset_match_count"),
            F.lit(0)
        ).alias(
            "asset_match_count"
        ),
        F.coalesce(
            F.col("m.matched_asset_keys"),
            F.lit("")
        ).alias(
            "matched_asset_keys"
        ),
        F.coalesce(
            F.col(
                "m.matched_asset_lifecycle_statuses"
            ),
            F.lit("")
        ).alias(
            "matched_asset_lifecycle_statuses"
        ),
        F.coalesce(
            F.col(
                "m.matched_cmdb_indicator"
            ),
            F.lit(0)
        ).alias(
            "matched_cmdb_indicator"
        )
    )
    .withColumn(
        "asset_match_status",
        F.when(
            F.col("asset_match_count") == 1,
            "Unique Asset Match"
        )
        .when(
            F.col("asset_match_count") > 1,
            "Multiple Asset Matches"
        )
        .otherwise(
            "No Asset Match"
        )
    )
    .withColumn(
        "post_attribution_confidence",
        F.when(
            F.col("asset_match_count") == 1,
            "High"
        )
        .when(
            F.col("asset_match_count") > 1,
            "Review"
        )
        .when(
            F.col("direct_post_token_match") == "Y",
            "Medium"
        )
        .otherwise(
            "Unattributed"
        )
    )
    .withColumn(
        "post_attribution_basis",
        F.when(
            F.col("asset_match_count") == 1,
            "Matched to the Post Asset Plane"
        )
        .when(
            F.col("asset_match_count") > 1,
            "Hostname matched multiple Asset Plane identities"
        )
        .when(
            F.col("direct_post_token_match") == "Y",
            "Post token in AD computer attributes"
        )
        .otherwise(
            "No direct Post evidence identified"
        )
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 15. Direct Post identity boundary
#
# One-hop support groups remain in the directory for relationship context but
# are intentionally excluded from the Post identity-boundary population.
# ---------------------------------------------------------------------------

post_users = (
    ad_users_norm
    .withColumn(
        "post_attribution_confidence",
        F.when(
            F.col("direct_post_token_match") == "Y",
            "Medium"
        )
        .otherwise(
            "Review"
        )
    )
    .withColumn(
        "post_attribution_basis",
        F.when(
            F.col("direct_post_token_match") == "Y",
            "Post token in AD user attributes"
        )
        .otherwise(
            F.concat(
                F.lit(
                    "Direct member of bounded Post group: "
                ),
                F.col(
                    "attribution_group_names"
                )
            )
        )
    )
    .select(
        "ad_object_key",
        "object_type",
        "scope_role",
        "distinguished_name_key",
        F.coalesce(
            F.col("display_name"),
            F.col("name"),
            F.col("sam_account_name")
        ).alias(
            "object_name"
        ),
        "post_attribution_confidence",
        "post_attribution_basis",
        F.when(
            F.col(
                "direct_post_group_member_indicator"
            )
            ==
            "Y",
            1
        )
        .otherwise(0)
        .cast("long")
        .alias(
            "scope_relationship_count"
        ),
        F.col(
            "attribution_group_names"
        ).alias(
            "scope_relationship_names"
        )
    )
)


post_computers = (
    computer_asset_map
    .filter(
        F.col(
            "post_attribution_confidence"
        ).isin(
            "High",
            "Medium",
            "Review"
        )
    )
    .select(
        "ad_object_key",
        "object_type",
        "scope_role",
        "distinguished_name_key",
        F.coalesce(
            F.col("dns_host_name"),
            F.col("name"),
            F.col("sam_account_name")
        ).alias(
            "object_name"
        ),
        "post_attribution_confidence",
        "post_attribution_basis",
        F.col(
            "asset_match_count"
        ).cast("long").alias(
            "scope_relationship_count"
        ),
        F.col(
            "matched_asset_keys"
        ).alias(
            "scope_relationship_names"
        )
    )
)


post_groups = (
    ad_groups_norm
    .filter(
        F.col("group_scope_role")
        ==
        "DIRECT_POST_GROUP"
    )
    .select(
        "ad_object_key",
        "object_type",
        F.col(
            "group_scope_role"
        ).alias(
            "scope_role"
        ),
        "distinguished_name_key",
        F.coalesce(
            F.col("name"),
            F.col("sam_account_name")
        ).alias(
            "object_name"
        ),
        F.lit("Medium").alias(
            "post_attribution_confidence"
        ),
        F.lit(
            "Post token in AD group attributes"
        ).alias(
            "post_attribution_basis"
        ),
        F.col("member_count")
        .cast("long")
        .alias(
            "scope_relationship_count"
        ),
        F.col(
            "member_expansion_reason"
        ).alias(
            "scope_relationship_names"
        )
    )
)


post_scope_objects = (
    post_users
    .unionByName(
        post_computers
    )
    .unionByName(
        post_groups
    )
    .dropDuplicates([
        "ad_object_key",
        "object_type"
    ])
    .withColumn(
        "automatic_assignment_allowed",
        F.lit(False)
    )
    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------------------------
# 16. Durable strict-scope foundation
# ---------------------------------------------------------------------------

save_output(
    ad_users_norm,
    "IP004_AD_USERS_NORM"
)

save_output(
    ad_computers_norm,
    "IP004_AD_COMPUTERS_NORM"
)

save_output(
    ad_groups_norm,
    "IP004_AD_GROUPS_NORM"
)

save_output(
    ad_object_directory,
    "IP004_AD_OBJECT_DIRECTORY"
)

save_output(
    ad_memberof_bridge,
    "IP004_AD_MEMBEROF_BRIDGE"
)

save_output(
    ad_group_member_bridge,
    "IP004_AD_GROUP_MEMBER_BRIDGE"
)

save_output(
    ad_managedby_bridge,
    "IP004_AD_MANAGEDBY_BRIDGE"
)

save_output(
    computer_asset_map,
    "IP004_COMPUTER_ASSET_MAP"
)

save_output(
    post_scope_objects,
    "IP004_POST_SCOPE_OBJECTS"
)


saved = {
    logical_name: spark.table(
        table_name
    )
    for logical_name, table_name in OUTPUTS.items()
    if logical_name in {
        "IP004_AD_USERS_NORM",
        "IP004_AD_COMPUTERS_NORM",
        "IP004_AD_GROUPS_NORM",
        "IP004_AD_OBJECT_DIRECTORY",
        "IP004_AD_MEMBEROF_BRIDGE",
        "IP004_AD_GROUP_MEMBER_BRIDGE",
        "IP004_AD_MANAGEDBY_BRIDGE",
        "IP004_COMPUTER_ASSET_MAP",
        "IP004_POST_SCOPE_OBJECTS"
    }
}


# ---------------------------------------------------------------------------
# 17. Consolidated counts
# ---------------------------------------------------------------------------

user_metrics = (
    saved["IP004_AD_USERS_NORM"]
    .agg(
        F.count("*").alias(
            "user_count"
        ),
        F.sum(
            F.when(
                F.col("scope_role")
                ==
                "DIRECT_POST_USER_TOKEN_MATCH",
                1
            )
            .otherwise(0)
        ).alias(
            "token_user_count"
        ),
        F.sum(
            F.when(
                F.col("scope_role")
                ==
                "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP",
                1
            )
            .otherwise(0)
        ).alias(
            "bounded_group_user_count"
        )
    )
    .first()
)


computer_metrics = (
    saved["IP004_COMPUTER_ASSET_MAP"]
    .agg(
        F.count("*").alias(
            "computer_count"
        ),
        F.sum(
            F.when(
                F.col("asset_match_count") > 0,
                1
            )
            .otherwise(0)
        ).alias(
            "asset_matched_computer_count"
        ),
        F.sum(
            F.when(
                (
                    F.col("asset_match_count") == 0
                )
                &
                (
                    F.col("direct_post_token_match") == "Y"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "token_only_computer_count"
        )
    )
    .first()
)


group_metrics = (
    saved["IP004_AD_GROUPS_NORM"]
    .agg(
        F.count("*").alias(
            "group_count"
        ),
        F.sum(
            F.when(
                F.col("group_scope_role")
                ==
                "DIRECT_POST_GROUP",
                1
            )
            .otherwise(0)
        ).alias(
            "direct_group_count"
        ),
        F.sum(
            F.when(
                F.col("group_scope_role")
                ==
                "ONE_HOP_SUPPORT_GROUP",
                1
            )
            .otherwise(0)
        ).alias(
            "support_group_count"
        ),
        F.sum(
            F.when(
                (
                    F.col("group_scope_role")
                    ==
                    "DIRECT_POST_GROUP"
                )
                &
                (
                    F.col("member_expansion_allowed") == "Y"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "bounded_direct_group_count"
        ),
        F.sum(
            F.when(
                (
                    F.col("group_scope_role")
                    ==
                    "DIRECT_POST_GROUP"
                )
                &
                (
                    F.col("member_expansion_allowed") == "N"
                ),
                1
            )
            .otherwise(0)
        ).alias(
            "nonexpanded_direct_group_count"
        )
    )
    .first()
)


directory_count = saved[
    "IP004_AD_OBJECT_DIRECTORY"
].count()

memberof_count = saved[
    "IP004_AD_MEMBEROF_BRIDGE"
].count()

group_member_count = saved[
    "IP004_AD_GROUP_MEMBER_BRIDGE"
].count()

managedby_count = saved[
    "IP004_AD_MANAGEDBY_BRIDGE"
].count()

post_scope_count = saved[
    "IP004_POST_SCOPE_OBJECTS"
].count()


user_count = int(
    user_metrics["user_count"]
    or
    0
)

token_user_count = int(
    user_metrics["token_user_count"]
    or
    0
)

bounded_group_user_count = int(
    user_metrics["bounded_group_user_count"]
    or
    0
)

computer_count = int(
    computer_metrics["computer_count"]
    or
    0
)

asset_matched_computer_count = int(
    computer_metrics["asset_matched_computer_count"]
    or
    0
)

token_only_computer_count = int(
    computer_metrics["token_only_computer_count"]
    or
    0
)

group_count = int(
    group_metrics["group_count"]
    or
    0
)

direct_group_count = int(
    group_metrics["direct_group_count"]
    or
    0
)

support_group_count = int(
    group_metrics["support_group_count"]
    or
    0
)

bounded_direct_group_count = int(
    group_metrics["bounded_direct_group_count"]
    or
    0
)

nonexpanded_direct_group_count = int(
    group_metrics["nonexpanded_direct_group_count"]
    or
    0
)


# ---------------------------------------------------------------------------
# 18. Summary products
# ---------------------------------------------------------------------------

summary_schema = T.StructType([
    T.StructField(
        "metric_category",
        T.StringType(),
        False
    ),
    T.StructField(
        "metric_name",
        T.StringType(),
        False
    ),
    T.StructField(
        "metric_value",
        T.LongType(),
        False
    ),
    T.StructField(
        "metric_interpretation",
        T.StringType(),
        False
    )
])


summary_rows = [
    (
        "Direct Post Identity Population",
        "Direct Post user objects",
        user_count,
        "Post attribute evidence or bounded direct Post-group membership"
    ),
    (
        "Direct Post Identity Population",
        "User attribute-token matches",
        token_user_count,
        "User attributes contain configured Post evidence"
    ),
    (
        "Direct Post Identity Population",
        "Bounded direct-group-derived users",
        bounded_group_user_count,
        "Direct members of bounded explicitly Post-attributed groups"
    ),
    (
        "Direct Post Identity Population",
        "Direct Post computer objects",
        computer_count,
        "Asset Plane hostname match or Post attribute evidence"
    ),
    (
        "Direct Post Identity Population",
        "Asset-matched computer objects",
        asset_matched_computer_count,
        "AD hostname matched one or more Post Asset Plane identities"
    ),
    (
        "Direct Post Identity Population",
        "Token-only computer objects",
        token_only_computer_count,
        "Post attribute evidence without an Asset Plane hostname match"
    ),
    (
        "Group Context",
        "Direct Post groups",
        direct_group_count,
        "The group's own attributes contain configured Post evidence"
    ),
    (
        "Group Context",
        "Bounded direct groups",
        bounded_direct_group_count,
        "Direct Post groups allowed to contribute bounded member attribution"
    ),
    (
        "Group Context",
        "Nonexpanded direct groups",
        nonexpanded_direct_group_count,
        "Direct Post groups retained without member-based attribution"
    ),
    (
        "Group Context",
        "One-hop support groups",
        support_group_count,
        "Directly referenced groups retained as context only"
    ),
    (
        "Strict Identity Graph",
        "Scoped directory objects",
        directory_count,
        "Direct identities plus direct and one-hop group context"
    ),
    (
        "Strict Identity Graph",
        "memberOf relationships",
        memberof_count,
        "Direct objects to direct or one-hop group targets"
    ),
    (
        "Strict Identity Graph",
        "Bounded group-member relationships",
        group_member_count,
        "Bounded direct groups to already-scoped direct identities only"
    ),
    (
        "Strict Identity Graph",
        "managedBy relationships",
        managedby_count,
        "Ownership references without manager-based scope expansion"
    ),
    (
        "Post Attribution",
        "Direct Post identity-boundary objects",
        post_scope_count,
        "Direct users, direct computers, and direct groups only"
    )
]


phase1_summary = spark.createDataFrame(
    summary_rows,
    summary_schema
)


relationship_summary = (
    saved["IP004_AD_MEMBEROF_BRIDGE"]
    .select(
        F.lit("memberOf").alias(
            "relationship_type"
        ),
        "relationship_scope",
        "relationship_resolution_status"
    )
    .unionByName(
        saved["IP004_AD_GROUP_MEMBER_BRIDGE"]
        .select(
            F.lit("Group Member").alias(
                "relationship_type"
            ),
            "relationship_scope",
            "relationship_resolution_status"
        )
    )
    .unionByName(
        saved["IP004_AD_MANAGEDBY_BRIDGE"]
        .select(
            F.lit("managedBy").alias(
                "relationship_type"
            ),
            F.lit(
                "OWNERSHIP_CONTEXT_WITHOUT_SCOPE_EXPANSION"
            ).alias(
                "relationship_scope"
            ),
            "relationship_resolution_status"
        )
    )
    .groupBy(
        "relationship_type",
        "relationship_scope",
        "relationship_resolution_status"
    )
    .agg(
        F.count("*").alias(
            "relationship_count"
        )
    )
)


computer_mapping_summary = (
    saved["IP004_COMPUTER_ASSET_MAP"]
    .groupBy(
        "asset_match_status",
        "post_attribution_confidence",
        "scope_role"
    )
    .agg(
        F.count("*").alias(
            "computer_count"
        )
    )
)


post_attribution_summary = (
    saved["IP004_POST_SCOPE_OBJECTS"]
    .groupBy(
        "object_type",
        "scope_role",
        "post_attribution_confidence",
        "post_attribution_basis"
    )
    .agg(
        F.count("*").alias(
            "object_count"
        )
    )
)


save_output(
    phase1_summary,
    "IP004_PHASE1_SUMMARY"
)

save_output(
    relationship_summary,
    "IP004_RELATIONSHIP_SUMMARY"
)

save_output(
    computer_mapping_summary,
    "IP004_COMPUTER_MAPPING_SUMMARY"
)

save_output(
    post_attribution_summary,
    "IP004_POST_ATTRIBUTION_SUMMARY"
)


# ---------------------------------------------------------------------------
# 19. Semantic scope certification
# ---------------------------------------------------------------------------

expected_directory_count = (
    user_count
    +
    computer_count
    +
    group_count
)

expected_post_scope_count = (
    user_count
    +
    computer_count
    +
    direct_group_count
)


invalid_user_scope_count = (
    saved["IP004_AD_USERS_NORM"]
    .filter(
        ~(
            F.col("scope_role").isin(
                "DIRECT_POST_USER_TOKEN_MATCH",
                "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP"
            )
        )
    )
    .count()
)


invalid_computer_scope_count = (
    saved["IP004_AD_COMPUTERS_NORM"]
    .filter(
        ~(
            F.col("scope_role").isin(
                "DIRECT_POST_COMPUTER_ASSET_MATCH",
                "DIRECT_POST_COMPUTER_TOKEN_MATCH"
            )
        )
    )
    .count()
)


invalid_group_scope_count = (
    saved["IP004_AD_GROUPS_NORM"]
    .filter(
        ~(
            F.col("group_scope_role").isin(
                "DIRECT_POST_GROUP",
                "ONE_HOP_SUPPORT_GROUP"
            )
        )
    )
    .count()
)


support_groups_in_post_boundary = (
    saved["IP004_POST_SCOPE_OBJECTS"]
    .filter(
        F.col("scope_role")
        ==
        "ONE_HOP_SUPPORT_GROUP"
    )
    .count()
)


support_group_member_edges = (
    saved["IP004_AD_GROUP_MEMBER_BRIDGE"]
    .filter(
        (
            F.col("group_scope_role")
            !=
            "DIRECT_POST_GROUP"
        )
        |
        (
            F.col("resolved_member_scope_role")
            ==
            "ONE_HOP_SUPPORT_GROUP"
        )
    )
    .count()
)


support_group_memberof_sources = (
    saved["IP004_AD_MEMBEROF_BRIDGE"]
    .filter(
        F.col("scope_role")
        ==
        "ONE_HOP_SUPPORT_GROUP"
    )
    .count()
)


unexpected_memberof_targets = (
    saved["IP004_AD_MEMBEROF_BRIDGE"]
    .filter(
        ~(
            F.col(
                "resolved_group_scope_role"
            ).isin(
                "DIRECT_POST_GROUP",
                "ONE_HOP_SUPPORT_GROUP"
            )
        )
    )
    .count()
)


missing_outputs = [
    table_name
    for table_name in OUTPUTS.values()
    if not spark.catalog.tableExists(
        table_name
    )
]


cross_post_outputs = [
    table_name
    for table_name in OUTPUTS.values()
    if not table_name.lower().startswith(
        POST_PREFIX
    )
]


certification_checks = [
    (
        "Directory population accounting",
        expected_directory_count,
        directory_count,
        expected_directory_count == directory_count
    ),
    (
        "Direct Post boundary accounting",
        expected_post_scope_count,
        post_scope_count,
        expected_post_scope_count == post_scope_count
    ),
    (
        "Invalid user scope roles",
        0,
        invalid_user_scope_count,
        invalid_user_scope_count == 0
    ),
    (
        "Invalid computer scope roles",
        0,
        invalid_computer_scope_count,
        invalid_computer_scope_count == 0
    ),
    (
        "Invalid group scope roles",
        0,
        invalid_group_scope_count,
        invalid_group_scope_count == 0
    ),
    (
        "One-hop support groups in Post identity boundary",
        0,
        support_groups_in_post_boundary,
        support_groups_in_post_boundary == 0
    ),
    (
        "One-hop support-group member expansion",
        0,
        support_group_member_edges,
        support_group_member_edges == 0
    ),
    (
        "One-hop support groups used as memberOf sources",
        0,
        support_group_memberof_sources,
        support_group_memberof_sources == 0
    ),
    (
        "memberOf targets outside strict group scope",
        0,
        unexpected_memberof_targets,
        unexpected_memberof_targets == 0
    ),
    (
        "Missing durable outputs",
        0,
        len(missing_outputs),
        len(missing_outputs) == 0
    ),
    (
        "Cross-Post output bindings",
        0,
        len(cross_post_outputs),
        len(cross_post_outputs) == 0
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
        T.LongType(),
        False
    ),
    T.StructField(
        "actual_value",
        T.LongType(),
        False
    ),
    T.StructField(
        "check_passed",
        T.BooleanType(),
        False
    )
])


ip004_scope_certificate_df = spark.createDataFrame(
    certification_checks,
    certificate_schema
)


failed_checks = [
    check
    for check in certification_checks
    if not check[3]
]


RCAIP_IP004_FOUNDATION_CERTIFIED = (
    len(failed_checks) == 0
)

RCAIP_IP004_READY_FOR_HYGIENE = (
    RCAIP_IP004_FOUNDATION_CERTIFIED
)


# ---------------------------------------------------------------------------
# 20. Final runtime contract
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP IP-004 - STRICT SCOPED AD FOUNDATION")
print("=" * 100)
print(
    "Status:",
    "PASS"
    if RCAIP_IP004_FOUNDATION_CERTIFIED
    else
    "FAIL"
)
print(
    "Ready for hygiene analytics:",
    RCAIP_IP004_READY_FOR_HYGIENE
)
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
print("Scored Asset Plane rows:", asset_plane_count)
print("Direct Post users:", user_count)
print("  User attribute-token matches:", token_user_count)
print(
    "  Bounded direct-group-derived users:",
    bounded_group_user_count
)
print("Direct Post computers:", computer_count)
print("  Asset-matched computers:", asset_matched_computer_count)
print("  Token-only computers:", token_only_computer_count)
print("Direct Post groups:", direct_group_count)
print("  Bounded direct groups:", bounded_direct_group_count)
print(
    "  Direct groups retained without member expansion:",
    nonexpanded_direct_group_count
)
print("One-hop support groups:", support_group_count)
print("Scoped directory objects:", directory_count)
print("Direct Post identity-boundary objects:", post_scope_count)
print("Scoped memberOf relationships:", memberof_count)
print(
    "Bounded direct group-member relationships:",
    group_member_count
)
print("Scoped managedBy relationships:", managedby_count)
print(
    "Maximum direct-group member attribution threshold:",
    IP004_MAX_DIRECT_GROUP_MEMBERS
)
print("Recursive group traversal performed: 0")
print("One-hop support-group members imported: 0")
print("One-hop support groups assigned to Post boundary: 0")
print("Durable foundation tables written:", len(WRITTEN_TABLES))
print("Missing foundation outputs:", len(missing_outputs))
print("Cross-Post output bindings:", len(cross_post_outputs))
print("Automatic account or group changes performed: 0")
print("Automatic findings, tickets, or remediation created: 0")
print("=" * 100)


display(
    ip004_scope_certificate_df
    .orderBy(
        F.col("check_passed").asc(),
        "certification_check"
    )
)


display(
    post_attribution_summary
    .orderBy(
        "object_type",
        F.desc("object_count")
    )
)


display(
    computer_mapping_summary
    .orderBy(
        "asset_match_status",
        "post_attribution_confidence"
    )
)


display(
    relationship_summary
    .orderBy(
        "relationship_type",
        "relationship_scope",
        "relationship_resolution_status"
    )
)


if failed_checks:
    raise RuntimeError(
        "IP-004 strict scope certification failed: "
        +
        " | ".join(
            check[0]
            for check in failed_checks
        )
    )

# COMMAND ----------

# ============================================================================
# RCAIP IP-004 - IDENTITY AND ACTIVE DIRECTORY HYGIENE
# Cell 4 - Security-relevant hygiene analytics and AD-bloat prioritization
#
# Requires:
#   Cell 1: RCAIP_Config_001_Table_Registry
#   Certified strict-scope outputs from IP-004 Cells 2 and 3
#
# Principles:
#   - Direct Post identities are the operational denominator.
#   - One-hop support groups remain enterprise context only.
#   - Directory imperfection is not automatically a security finding.
#   - Priority is driven by privilege, exposure, lifecycle, ownership,
#     attribution confidence, and effect on direct Post identities.
#   - No account, group, membership, finding, ticket, or remediation changes.
# ============================================================================

from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import functions as F


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
    "RCAIP_ASSESSMENT_END_DATE_TEXT"
]

missing_runtime_objects = [
    name
    for name in required_runtime_objects
    if name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-004 hygiene analytics did not receive the complete runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

post_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()


def post_table(stem, require_exists=False):
    table_name = (
        f"{RCAIP_RESULTS_NAMESPACE}."
        f"{RCAIP_POST_SLUG}_{stem}"
    )

    if not table_name.lower().startswith(post_prefix):
        raise RuntimeError(
            "Cross-Post table protection blocked: " + table_name
        )

    if require_exists and not spark.catalog.tableExists(table_name):
        raise RuntimeError("Missing required table: " + table_name)

    return table_name


INPUTS = {
    "users": post_table(
        "ip004_ad_users_norm",
        require_exists=True
    ),
    "computers": post_table(
        "ip004_computer_asset_map",
        require_exists=True
    ),
    "groups": post_table(
        "ip004_ad_groups_norm",
        require_exists=True
    ),
    "memberof": post_table(
        "ip004_ad_memberof_bridge",
        require_exists=True
    ),
    "group_member": post_table(
        "ip004_ad_group_member_bridge",
        require_exists=True
    ),
    "managedby": post_table(
        "ip004_ad_managedby_bridge",
        require_exists=True
    ),
    "post_scope": post_table(
        "ip004_post_scope_objects",
        require_exists=True
    )
}


OUTPUTS = {
    "computer_review": post_table(
        "ip004_computer_hygiene_review"
    ),
    "user_review": post_table(
        "ip004_user_hygiene_review"
    ),
    "group_review": post_table(
        "ip004_group_hygiene_review"
    ),
    "membership_review": post_table(
        "ip004_membership_hygiene_review"
    ),
    "privileged_review": post_table(
        "ip004_privileged_access_review"
    ),
    "bloat_priority": post_table(
        "ip004_ad_bloat_priority"
    ),
    "action_queue": post_table(
        "ip004_hygiene_action_queue"
    ),
    "summary": post_table(
        "ip004_hygiene_summary"
    )
}


WRITTEN_TABLES = []


def add_runtime(dataframe):
    runtime_columns = {
        "post_name": RCAIP_POST_NAME,
        "post_slug": RCAIP_POST_SLUG,
        "rcaip_run_id": RCAIP_RUN_ID,
        "runtime_country_name": RCAIP_COUNTRY_NAME,
        "runtime_run_mode": RCAIP_RUN_MODE
    }

    output = dataframe

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


def save_output(dataframe, output_key):
    table_name = OUTPUTS[output_key]
    output = add_runtime(dataframe)

    (
        output.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    WRITTEN_TABLES.append(table_name)
    print("Saved:", table_name)


# ---------------------------------------------------------------------------
# 2. Load strict-scope foundation
# ---------------------------------------------------------------------------

users = spark.table(INPUTS["users"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

computers = spark.table(INPUTS["computers"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

groups = spark.table(INPUTS["groups"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

memberof = spark.table(INPUTS["memberof"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

group_member = spark.table(INPUTS["group_member"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

managedby = spark.table(INPUTS["managedby"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

post_scope = spark.table(INPUTS["post_scope"]).persist(
    StorageLevel.MEMORY_AND_DISK
)


required_columns = {
    "users": {
        "ad_object_key",
        "sam_account_name",
        "display_name",
        "user_principal_name",
        "employee_id",
        "employee_type",
        "title",
        "office",
        "location",
        "manager_dn_key",
        "managed_by_dn_key",
        "last_logon_timestamp",
        "pwd_last_set_timestamp",
        "admin_count",
        "account_enabled",
        "password_never_expires",
        "smartcard_required",
        "does_not_require_pre_auth",
        "days_since_last_logon",
        "account_lifecycle_status",
        "scope_role",
        "direct_post_token_match",
        "direct_post_group_member_indicator",
        "attribution_group_names"
    },
    "computers": {
        "ad_object_key",
        "host_key",
        "sam_account_name",
        "dns_host_name",
        "operating_system",
        "last_logon_timestamp",
        "pwd_last_set_timestamp",
        "managed_by_dn_key",
        "admin_count",
        "account_enabled",
        "days_since_last_logon",
        "computer_lifecycle_status",
        "scope_role",
        "asset_match_count",
        "asset_match_status",
        "matched_asset_keys",
        "matched_asset_lifecycle_statuses",
        "matched_cmdb_indicator",
        "post_attribution_confidence",
        "post_attribution_basis"
    },
    "groups": {
        "ad_object_key",
        "sam_account_name",
        "name",
        "description",
        "admin_count",
        "managed_by_dn_key",
        "when_changed_timestamp",
        "member_count",
        "member_expansion_allowed",
        "member_expansion_reason",
        "group_scope_role",
        "has_managed_by",
        "privileged_indicator"
    },
    "memberof": {
        "ad_object_key",
        "object_type",
        "scope_role",
        "resolved_group_key",
        "resolved_group_name",
        "resolved_group_admin_count",
        "resolved_group_privileged_indicator",
        "resolved_group_scope_role",
        "relationship_scope"
    },
    "group_member": {
        "group_key",
        "group_name",
        "resolved_member_key",
        "resolved_member_type",
        "resolved_member_name",
        "resolved_member_scope_role"
    }
}

loaded_frames = {
    "users": users,
    "computers": computers,
    "groups": groups,
    "memberof": memberof,
    "group_member": group_member
}

for frame_name, required in required_columns.items():
    missing = sorted(
        required - set(loaded_frames[frame_name].columns)
    )

    if missing:
        raise RuntimeError(
            f"{frame_name} does not satisfy the hygiene contract. "
            "Missing: " + " | ".join(missing)
        )


# ---------------------------------------------------------------------------
# 3. Relationship metrics used by group and bloat analysis
# ---------------------------------------------------------------------------

group_relationship_metrics = (
    memberof
    .groupBy("resolved_group_key")
    .agg(
        F.countDistinct("ad_object_key").alias(
            "direct_identity_reference_count"
        ),
        F.sum(
            F.when(F.col("object_type") == "User", 1)
            .otherwise(0)
        ).alias("referencing_user_count"),
        F.sum(
            F.when(F.col("object_type") == "Computer", 1)
            .otherwise(0)
        ).alias("referencing_computer_count"),
        F.sum(
            F.when(F.col("object_type") == "Group", 1)
            .otherwise(0)
        ).alias("referencing_group_count")
    )
)

bounded_member_metrics = (
    group_member
    .groupBy("group_key")
    .agg(
        F.countDistinct("resolved_member_key").alias(
            "bounded_direct_member_count"
        ),
        F.sum(
            F.when(
                F.col("resolved_member_type") == "User",
                1
            ).otherwise(0)
        ).alias("bounded_user_member_count"),
        F.sum(
            F.when(
                F.col("resolved_member_type") == "Computer",
                1
            ).otherwise(0)
        ).alias("bounded_computer_member_count"),
        F.sum(
            F.when(
                F.col("resolved_member_type") == "Group",
                1
            ).otherwise(0)
        ).alias("bounded_group_member_count")
    )
)


# ---------------------------------------------------------------------------
# 4. Computer hygiene review
# ---------------------------------------------------------------------------

computer_hygiene_review = (
    computers
    .withColumn(
        "computer_review_condition",
        F.concat_ws(
            "; ",
            F.when(
                (F.col("account_enabled") == "Y")
                & F.col("last_logon_timestamp").isNull(),
                F.lit("Enabled account has no usable logon timestamp")
            ),
            F.when(
                (F.col("account_enabled") == "Y")
                & (F.col("days_since_last_logon") > 180),
                F.lit("Enabled computer account inactive over 180 days")
            ),
            F.when(
                (F.col("account_enabled") == "Y")
                & (F.col("days_since_last_logon") > 90)
                & (F.col("days_since_last_logon") <= 180),
                F.lit("Enabled computer account inactive 91-180 days")
            ),
            F.when(
                F.col("asset_match_count") == 0,
                F.lit("No Asset Plane identity match")
            ),
            F.when(
                F.col("asset_match_count") > 1,
                F.lit("Hostname maps to multiple Asset Plane identities")
            ),
            F.when(
                F.col("admin_count") == 1,
                F.lit("Admin-protected computer object")
            ),
            F.when(
                F.col("managed_by_dn_key").isNull(),
                F.lit("No managedBy ownership attribute")
            )
        )
    )
    .withColumn(
        "computer_security_priority",
        F.when(
            (F.col("account_enabled") == "Y")
            & (F.col("admin_count") == 1),
            "High"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (F.col("asset_match_count") == 0),
            "High"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (
                F.col("last_logon_timestamp").isNull()
                | (F.col("days_since_last_logon") > 180)
            ),
            "Medium"
        )
        .when(
            F.col("asset_match_count") > 1,
            "Medium"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (F.col("days_since_last_logon") > 90),
            "Review"
        )
        .when(
            F.col("managed_by_dn_key").isNull(),
            "Low"
        )
        .otherwise("Pass")
    )
    .withColumn(
        "computer_security_interpretation",
        F.when(
            F.col("computer_security_priority") == "High",
            "Validate operational ownership and identity before concluding "
            "that the AD or Asset Plane record is incorrect."
        )
        .when(
            F.col("computer_security_priority").isin("Medium", "Review"),
            "Lifecycle or identity evidence warrants validation but is not "
            "an automatic security finding."
        )
        .otherwise(
            "No material computer-account hygiene condition identified by "
            "the current evidence model."
        )
    )
    .withColumn(
        "validation_responsibility",
        F.when(
            F.col("asset_match_count") == 0,
            "Post corroboration with Enterprise AD and Asset Management"
        )
        .when(
            F.col("asset_match_count") > 1,
            "Enterprise identity correlation with Post corroboration"
        )
        .otherwise(
            "Post AD administration with Enterprise policy support"
        )
    )
    .withColumn("automatic_disable_allowed", F.lit(False))
    .withColumn("automatic_delete_allowed", F.lit(False))
    .withColumn("automatic_finding_allowed", F.lit(False))
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# ---------------------------------------------------------------------------
# 5. User hygiene review
# ---------------------------------------------------------------------------

user_hygiene_review = (
    users
    .withColumn(
        "user_review_condition",
        F.concat_ws(
            "; ",
            F.when(
                F.col("does_not_require_pre_auth") == "Y",
                F.lit("Kerberos pre-authentication not required")
            ),
            F.when(
                F.col("password_never_expires") == "Y",
                F.lit("Password configured to never expire")
            ),
            F.when(
                (F.col("account_enabled") == "Y")
                & F.col("last_logon_timestamp").isNull(),
                F.lit("Enabled user has no usable logon timestamp")
            ),
            F.when(
                (F.col("account_enabled") == "Y")
                & (F.col("days_since_last_logon") > 180),
                F.lit("Enabled user inactive over 180 days")
            ),
            F.when(
                (F.col("account_enabled") == "Y")
                & (F.col("days_since_last_logon") > 90)
                & (F.col("days_since_last_logon") <= 180),
                F.lit("Enabled user inactive 91-180 days")
            ),
            F.when(
                F.col("admin_count") == 1,
                F.lit("Admin-protected user object")
            ),
            F.when(
                F.col("manager_dn_key").isNull(),
                F.lit("No manager attribute")
            ),
            F.when(
                F.col("employee_id").isNull(),
                F.lit("No employee identifier")
            ),
            F.when(
                F.col("scope_role")
                == "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP",
                F.lit("Post attribution is based on bounded group membership")
            )
        )
    )
    .withColumn(
        "user_security_priority",
        F.when(
            (F.col("account_enabled") == "Y")
            & (F.col("does_not_require_pre_auth") == "Y"),
            "High"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (F.col("admin_count") == 1)
            & (
                F.col("last_logon_timestamp").isNull()
                | (F.col("days_since_last_logon") > 180)
            ),
            "High"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (F.col("password_never_expires") == "Y"),
            "Medium"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (
                F.col("last_logon_timestamp").isNull()
                | (F.col("days_since_last_logon") > 180)
            ),
            "Medium"
        )
        .when(
            (F.col("account_enabled") == "Y")
            & (F.col("days_since_last_logon") > 90),
            "Review"
        )
        .when(
            F.col("scope_role")
            == "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP",
            "Review"
        )
        .when(
            F.col("manager_dn_key").isNull()
            | F.col("employee_id").isNull(),
            "Low"
        )
        .otherwise("Pass")
    )
    .withColumn(
        "user_security_interpretation",
        F.when(
            F.col("user_security_priority") == "High",
            "Security-significant account configuration or privileged "
            "lifecycle condition requires validation."
        )
        .when(
            F.col("user_security_priority").isin("Medium", "Review"),
            "Validate account purpose, owner, activity, and access need; the "
            "condition is not an automatic finding or disable decision."
        )
        .otherwise(
            "No material user-account security condition identified by the "
            "current evidence model."
        )
    )
    .withColumn(
        "validation_responsibility",
        F.when(
            F.col("scope_role")
            == "DIRECT_POST_USER_BOUNDED_GROUP_MEMBERSHIP",
            "Post group owner with Enterprise Identity Governance"
        )
        .when(
            F.col("admin_count") == 1,
            "Enterprise privileged-access owner with Post corroboration"
        )
        .otherwise(
            "Post account sponsor and AD administration"
        )
    )
    .withColumn("automatic_disable_allowed", F.lit(False))
    .withColumn("automatic_delete_allowed", F.lit(False))
    .withColumn("automatic_finding_allowed", F.lit(False))
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# ---------------------------------------------------------------------------
# 6. Group hygiene and AD-bloat prioritization
# ---------------------------------------------------------------------------

group_hygiene_review = (
    groups.alias("g")
    .join(
        group_relationship_metrics.alias("r"),
        F.col("g.ad_object_key")
        == F.col("r.resolved_group_key"),
        "left"
    )
    .join(
        bounded_member_metrics.alias("b"),
        F.col("g.ad_object_key")
        == F.col("b.group_key"),
        "left"
    )
    .select(
        "g.*",
        F.coalesce(
            F.col("r.direct_identity_reference_count"),
            F.lit(0)
        ).alias("direct_identity_reference_count"),
        F.coalesce(
            F.col("r.referencing_user_count"),
            F.lit(0)
        ).alias("referencing_user_count"),
        F.coalesce(
            F.col("r.referencing_computer_count"),
            F.lit(0)
        ).alias("referencing_computer_count"),
        F.coalesce(
            F.col("r.referencing_group_count"),
            F.lit(0)
        ).alias("referencing_group_count"),
        F.coalesce(
            F.col("b.bounded_direct_member_count"),
            F.lit(0)
        ).alias("bounded_direct_member_count"),
        F.coalesce(
            F.col("b.bounded_user_member_count"),
            F.lit(0)
        ).alias("bounded_user_member_count"),
        F.coalesce(
            F.col("b.bounded_computer_member_count"),
            F.lit(0)
        ).alias("bounded_computer_member_count"),
        F.coalesce(
            F.col("b.bounded_group_member_count"),
            F.lit(0)
        ).alias("bounded_group_member_count")
    )
    .withColumn(
        "days_since_group_change",
        F.when(
            F.col("when_changed_timestamp").isNotNull(),
            F.greatest(
                F.lit(0),
                F.datediff(
                    F.current_date(),
                    F.to_date(F.col("when_changed_timestamp"))
                )
            )
        ).otherwise(F.lit(None).cast("integer"))
    )
    .withColumn(
        "group_review_condition",
        F.concat_ws(
            "; ",
            F.when(
                F.col("privileged_indicator") == "Y",
                F.lit("Privileged or admin-protected group")
            ),
            F.when(
                F.col("has_managed_by") == "N",
                F.lit("No managedBy ownership attribute")
            ),
            F.when(
                F.col("member_count") == 0,
                F.lit("Group has no listed members")
            ),
            F.when(
                F.col("member_count") > 500,
                F.lit("Large group retained without member expansion")
            ),
            F.when(
                F.col("referencing_group_count") > 0,
                F.lit("Nested group relationship affects Post scope")
            ),
            F.when(
                F.col("direct_identity_reference_count") > 0,
                F.lit("Group directly affects Post identities")
            ),
            F.when(
                F.col("group_scope_role") == "ONE_HOP_SUPPORT_GROUP",
                F.lit("Enterprise support-group context only")
            )
        )
    )
    .withColumn(
        "group_security_priority",
        F.when(
            (F.col("group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("privileged_indicator") == "Y")
            & (F.col("has_managed_by") == "N"),
            "High"
        )
        .when(
            (F.col("privileged_indicator") == "Y")
            & (F.col("direct_identity_reference_count") > 0),
            "High"
        )
        .when(
            (F.col("group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("has_managed_by") == "N")
            & (F.col("member_count") > 0),
            "Medium"
        )
        .when(
            (F.col("group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("member_count") > 500),
            "Medium"
        )
        .when(
            (F.col("group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("referencing_group_count") > 0),
            "Review"
        )
        .when(
            (F.col("group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("member_count") == 0),
            "Low"
        )
        .otherwise("Context")
    )
    .withColumn(
        "ad_bloat_classification",
        F.when(
            F.col("group_security_priority") == "High",
            "Security-Relevant Bloat"
        )
        .when(
            F.col("group_security_priority").isin("Medium", "Review"),
            "Operationally Important Bloat"
        )
        .when(
            F.col("group_security_priority") == "Low",
            "Low-Priority Hygiene"
        )
        .otherwise(
            "Enterprise Context / Tolerable Unless Policy Changes"
        )
    )
    .withColumn(
        "group_security_interpretation",
        F.when(
            F.col("group_security_priority") == "High",
            "The group materially affects privileged or security-sensitive "
            "access for Post identities and should be validated first."
        )
        .when(
            F.col("group_security_priority").isin("Medium", "Review"),
            "The group affects Post operations or identity governance, but "
            "cleanup priority depends on ownership and business purpose."
        )
        .when(
            F.col("group_security_priority") == "Low",
            "The group appears untidy or unused but does not currently show "
            "a strong security-impact signal."
        )
        .otherwise(
            "The group is enterprise context. Its existence alone is not a "
            "Post finding or evidence that Post owns the group."
        )
    )
    .withColumn(
        "validation_responsibility",
        F.when(
            F.col("group_scope_role") == "ONE_HOP_SUPPORT_GROUP",
            "Enterprise group owner; Post validates local access need only"
        )
        .when(
            F.col("privileged_indicator") == "Y",
            "Enterprise privileged-access owner with Post corroboration"
        )
        .otherwise(
            "Post group owner with Enterprise Identity Governance"
        )
    )
    .withColumn("automatic_group_removal_allowed", F.lit(False))
    .withColumn("automatic_member_removal_allowed", F.lit(False))
    .withColumn("automatic_finding_allowed", F.lit(False))
    .persist(StorageLevel.MEMORY_AND_DISK)
)


ad_bloat_priority = (
    group_hygiene_review
    .select(
        "ad_object_key",
        "sam_account_name",
        "name",
        "description",
        "group_scope_role",
        "privileged_indicator",
        "has_managed_by",
        "member_count",
        "direct_identity_reference_count",
        "referencing_user_count",
        "referencing_computer_count",
        "referencing_group_count",
        "bounded_direct_member_count",
        "days_since_group_change",
        "member_expansion_allowed",
        "group_security_priority",
        "ad_bloat_classification",
        "group_review_condition",
        "group_security_interpretation",
        "validation_responsibility"
    )
    .withColumn(
        "recommended_bloat_disposition",
        F.when(
            F.col("ad_bloat_classification")
            == "Security-Relevant Bloat",
            "Validate owner, business purpose, privileged effect, nested "
            "access, and current Post member need before changing the group."
        )
        .when(
            F.col("ad_bloat_classification")
            == "Operationally Important Bloat",
            "Prioritize after privileged conditions; document purpose and "
            "reduce unnecessary access where the owner confirms it."
        )
        .when(
            F.col("ad_bloat_classification")
            == "Low-Priority Hygiene",
            "Retain for periodic cleanup unless stronger access or ownership "
            "evidence emerges."
        )
        .otherwise(
            "Treat as enterprise context; do not assign to Post solely because "
            "a Post identity references the group."
        )
    )
)


# ---------------------------------------------------------------------------
# 7. Membership and privileged-access review
# ---------------------------------------------------------------------------

user_membership_status = (
    users
    .select(
        "ad_object_key",
        F.lit("User").alias("source_object_type"),
        F.coalesce(
            F.col("display_name"),
            F.col("sam_account_name")
        ).alias("source_object_name"),
        "account_enabled",
        "days_since_last_logon",
        "admin_count",
        "password_never_expires",
        "does_not_require_pre_auth"
    )
)

computer_membership_status = (
    computers
    .select(
        "ad_object_key",
        F.lit("Computer").alias("source_object_type"),
        F.coalesce(
            F.col("dns_host_name"),
            F.col("sam_account_name"),
            F.col("host_key")
        ).alias("source_object_name"),
        "account_enabled",
        "days_since_last_logon",
        "admin_count",
        F.lit("N").alias("password_never_expires"),
        F.lit("N").alias("does_not_require_pre_auth")
    )
)

group_membership_status = (
    groups
    .filter(F.col("group_scope_role") == "DIRECT_POST_GROUP")
    .select(
        "ad_object_key",
        F.lit("Group").alias("source_object_type"),
        F.coalesce(
            F.col("name"),
            F.col("sam_account_name")
        ).alias("source_object_name"),
        F.lit("Y").alias("account_enabled"),
        F.lit(None).cast("integer").alias("days_since_last_logon"),
        "admin_count",
        F.lit("N").alias("password_never_expires"),
        F.lit("N").alias("does_not_require_pre_auth")
    )
)

membership_status = (
    user_membership_status
    .unionByName(computer_membership_status)
    .unionByName(group_membership_status)
)

membership_hygiene_review = (
    memberof.alias("m")
    .join(
        membership_status.alias("s"),
        F.col("m.ad_object_key")
        == F.col("s.ad_object_key"),
        "left"
    )
    .select(
        "m.*",
        "s.source_object_type",
        "s.source_object_name",
        "s.account_enabled",
        "s.days_since_last_logon",
        "s.admin_count",
        "s.password_never_expires",
        "s.does_not_require_pre_auth"
    )
    .withColumn(
        "membership_review_condition",
        F.concat_ws(
            "; ",
            F.when(
                F.col("resolved_group_privileged_indicator") == "Y",
                F.lit("Membership affects a privileged group")
            ),
            F.when(
                F.col("source_object_type") == "Group",
                F.lit("Nested group relationship")
            ),
            F.when(
                F.col("account_enabled") == "N",
                F.lit("Disabled identity remains a group member")
            ),
            F.when(
                F.col("days_since_last_logon") > 180,
                F.lit("Member identity inactive over 180 days")
            ),
            F.when(
                F.col("password_never_expires") == "Y",
                F.lit("Member password never expires")
            ),
            F.when(
                F.col("does_not_require_pre_auth") == "Y",
                F.lit("Member does not require Kerberos pre-authentication")
            ),
            F.when(
                F.col("resolved_group_scope_role")
                == "ONE_HOP_SUPPORT_GROUP",
                F.lit("Enterprise support-group relationship")
            )
        )
    )
    .withColumn(
        "membership_security_priority",
        F.when(
            (F.col("resolved_group_privileged_indicator") == "Y")
            & (F.col("does_not_require_pre_auth") == "Y"),
            "High"
        )
        .when(
            (F.col("resolved_group_privileged_indicator") == "Y")
            & (F.col("source_object_type") == "Group"),
            "High"
        )
        .when(
            (F.col("resolved_group_privileged_indicator") == "Y")
            & (
                (F.col("account_enabled") == "N")
                | (F.col("days_since_last_logon") > 180)
                | (F.col("password_never_expires") == "Y")
            ),
            "High"
        )
        .when(
            F.col("resolved_group_privileged_indicator") == "Y",
            "Medium"
        )
        .when(
            (F.col("resolved_group_scope_role") == "DIRECT_POST_GROUP")
            & (F.col("source_object_type") == "Group"),
            "Review"
        )
        .when(
            (F.col("resolved_group_scope_role") == "DIRECT_POST_GROUP")
            & (
                (F.col("account_enabled") == "N")
                | (F.col("days_since_last_logon") > 180)
            ),
            "Review"
        )
        .otherwise("Context")
    )
    .withColumn(
        "membership_security_interpretation",
        F.when(
            F.col("membership_security_priority") == "High",
            "Validate effective access, nesting, owner approval, and current "
            "operational need before changing membership."
        )
        .when(
            F.col("membership_security_priority").isin("Medium", "Review"),
            "Membership affects a relevant group and warrants owner review, "
            "but is not automatically inappropriate."
        )
        .otherwise(
            "Relationship retained as context; no strong security-priority "
            "signal is established by the current evidence."
        )
    )
    .withColumn(
        "validation_responsibility",
        F.when(
            F.col("resolved_group_scope_role")
            == "ONE_HOP_SUPPORT_GROUP",
            "Enterprise group owner; Post validates local access need"
        )
        .when(
            F.col("resolved_group_privileged_indicator") == "Y",
            "Enterprise privileged-access owner with Post corroboration"
        )
        .otherwise(
            "Post group owner with Enterprise Identity Governance"
        )
    )
    .withColumn("automatic_member_removal_allowed", F.lit(False))
    .withColumn("automatic_finding_allowed", F.lit(False))
    .persist(StorageLevel.MEMORY_AND_DISK)
)


privileged_access_review = (
    membership_hygiene_review
    .filter(
        F.col("resolved_group_privileged_indicator") == "Y"
    )
    .withColumn(
        "privileged_access_validation_state",
        F.when(
            F.col("membership_security_priority") == "High",
            "Priority Validation"
        )
        .otherwise("Owner Validation")
    )
)


# ---------------------------------------------------------------------------
# 8. Consolidated non-duplicative action queue
# ---------------------------------------------------------------------------

computer_actions = (
    computer_hygiene_review
    .filter(
        F.col("computer_security_priority").isin(
            "High", "Medium", "Review"
        )
    )
    .select(
        F.concat(
            F.lit("AD-COMPUTER-"),
            F.col("ad_object_key")
        ).alias("action_id"),
        F.lit("AD Computer").alias("action_domain"),
        F.col("ad_object_key").alias("subject_key"),
        F.coalesce(
            F.col("dns_host_name"),
            F.col("sam_account_name"),
            F.col("host_key")
        ).alias("subject_name"),
        F.col("computer_security_priority").alias("priority"),
        F.col("computer_review_condition").alias("condition"),
        F.col("computer_security_interpretation").alias("interpretation"),
        "validation_responsibility"
    )
)

user_actions = (
    user_hygiene_review
    .filter(
        F.col("user_security_priority").isin(
            "High", "Medium", "Review"
        )
    )
    .select(
        F.concat(
            F.lit("AD-USER-"),
            F.col("ad_object_key")
        ).alias("action_id"),
        F.lit("AD User").alias("action_domain"),
        F.col("ad_object_key").alias("subject_key"),
        F.coalesce(
            F.col("display_name"),
            F.col("sam_account_name"),
            F.col("user_principal_name")
        ).alias("subject_name"),
        F.col("user_security_priority").alias("priority"),
        F.col("user_review_condition").alias("condition"),
        F.col("user_security_interpretation").alias("interpretation"),
        "validation_responsibility"
    )
)

group_actions = (
    group_hygiene_review
    .filter(
        F.col("group_security_priority").isin(
            "High", "Medium", "Review"
        )
    )
    .select(
        F.concat(
            F.lit("AD-GROUP-"),
            F.col("ad_object_key")
        ).alias("action_id"),
        F.lit("AD Group").alias("action_domain"),
        F.col("ad_object_key").alias("subject_key"),
        F.coalesce(
            F.col("name"),
            F.col("sam_account_name")
        ).alias("subject_name"),
        F.col("group_security_priority").alias("priority"),
        F.col("group_review_condition").alias("condition"),
        F.col("group_security_interpretation").alias("interpretation"),
        "validation_responsibility"
    )
)

membership_actions = (
    membership_hygiene_review
    .filter(
        F.col("membership_security_priority").isin(
            "High", "Medium", "Review"
        )
    )
    .select(
        F.concat(
            F.lit("AD-MEMBERSHIP-"),
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("ad_object_key"),
                    F.col("resolved_group_key")
                ),
                256
            )
        ).alias("action_id"),
        F.lit("AD Membership").alias("action_domain"),
        F.col("ad_object_key").alias("subject_key"),
        F.concat_ws(
            " -> ",
            F.col("source_object_name"),
            F.col("resolved_group_name")
        ).alias("subject_name"),
        F.col("membership_security_priority").alias("priority"),
        F.col("membership_review_condition").alias("condition"),
        F.col("membership_security_interpretation").alias("interpretation"),
        "validation_responsibility"
    )
)

hygiene_action_queue = (
    computer_actions
    .unionByName(user_actions)
    .unionByName(group_actions)
    .unionByName(membership_actions)
    .dropDuplicates(["action_id"])
    .withColumn(
        "action_status",
        F.lit("Requires Validation")
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
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# ---------------------------------------------------------------------------
# 9. Compact hygiene summary
# ---------------------------------------------------------------------------

def priority_summary(dataframe, domain, priority_column):
    return (
        dataframe
        .groupBy(F.col(priority_column).alias("priority"))
        .agg(F.count("*").alias("record_count"))
        .withColumn("summary_domain", F.lit(domain))
        .select(
            "summary_domain",
            "priority",
            "record_count"
        )
    )


hygiene_summary = reduce(
    lambda left, right: left.unionByName(right),
    [
        priority_summary(
            computer_hygiene_review,
            "AD Computer",
            "computer_security_priority"
        ),
        priority_summary(
            user_hygiene_review,
            "AD User",
            "user_security_priority"
        ),
        priority_summary(
            group_hygiene_review,
            "AD Group",
            "group_security_priority"
        ),
        priority_summary(
            membership_hygiene_review,
            "AD Membership",
            "membership_security_priority"
        ),
        (
            ad_bloat_priority
            .groupBy(
                F.col("ad_bloat_classification").alias("priority")
            )
            .agg(F.count("*").alias("record_count"))
            .withColumn("summary_domain", F.lit("AD Bloat"))
            .select(
                "summary_domain",
                "priority",
                "record_count"
            )
        )
    ]
)


# ---------------------------------------------------------------------------
# 10. Durable hygiene products
# ---------------------------------------------------------------------------

save_output(computer_hygiene_review, "computer_review")
save_output(user_hygiene_review, "user_review")
save_output(group_hygiene_review, "group_review")
save_output(membership_hygiene_review, "membership_review")
save_output(privileged_access_review, "privileged_review")
save_output(ad_bloat_priority, "bloat_priority")
save_output(hygiene_action_queue, "action_queue")
save_output(hygiene_summary, "summary")


print("=" * 100)
print("RCAIP IP-004 - HYGIENE ANALYTICS FOUNDATION")
print("=" * 100)
print("Status: PASS")
print("Runtime Post:", RCAIP_POST_NAME)
print("Direct Post identities analyzed:", post_scope.count())
print("Computer review rows:", computer_hygiene_review.count())
print("User review rows:", user_hygiene_review.count())
print("Group context/review rows:", group_hygiene_review.count())
print("Membership context/review rows:", membership_hygiene_review.count())
print("Privileged-access relationship rows:", privileged_access_review.count())
print("Validation action rows:", hygiene_action_queue.count())
print("Durable hygiene tables written:", len(WRITTEN_TABLES))
print("Automatic account or group changes performed: 0")
print("Automatic member removals performed: 0")
print("Automatic findings, tickets, or remediation created: 0")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP IP-004 - IDENTITY AND ACTIVE DIRECTORY HYGIENE
# Cell 5 - Final hygiene certification and runtime contract
#
# Reads durable strict-scope and hygiene products. This cell is restartable
# after Cell 1 and does not require Cell 4 DataFrame objects in memory.
# ============================================================================

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# 1. Runtime and table contract
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
    "RCAIP_ASSESSMENT_END_DATE_TEXT"
]

missing_runtime_objects = [
    name
    for name in required_runtime_objects
    if name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "IP-004 certification did not receive the complete runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

post_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()


def post_table(stem):
    table_name = (
        f"{RCAIP_RESULTS_NAMESPACE}."
        f"{RCAIP_POST_SLUG}_{stem}"
    )

    if not table_name.lower().startswith(post_prefix):
        raise RuntimeError(
            "Cross-Post table protection blocked: " + table_name
        )

    return table_name


TABLES = {
    "users": post_table("ip004_ad_users_norm"),
    "computers": post_table("ip004_computer_asset_map"),
    "groups": post_table("ip004_ad_groups_norm"),
    "post_scope": post_table("ip004_post_scope_objects"),
    "computer_review": post_table("ip004_computer_hygiene_review"),
    "user_review": post_table("ip004_user_hygiene_review"),
    "group_review": post_table("ip004_group_hygiene_review"),
    "membership_review": post_table("ip004_membership_hygiene_review"),
    "privileged_review": post_table("ip004_privileged_access_review"),
    "bloat_priority": post_table("ip004_ad_bloat_priority"),
    "action_queue": post_table("ip004_hygiene_action_queue"),
    "summary": post_table("ip004_hygiene_summary")
}

missing_tables = [
    table_name
    for table_name in TABLES.values()
    if not spark.catalog.tableExists(table_name)
]

if missing_tables:
    raise RuntimeError(
        "Missing IP-004 durable outputs: "
        + " | ".join(missing_tables)
    )

frames = {
    name: spark.table(table_name)
    for name, table_name in TABLES.items()
}


# ---------------------------------------------------------------------------
# 2. Population and output-isolation checks
# ---------------------------------------------------------------------------

population_metrics = (
    frames["post_scope"]
    .agg(
        F.count("*").alias("post_scope_count"),
        F.countDistinct("ad_object_key").alias(
            "post_scope_distinct_keys"
        ),
        F.sum(
            F.when(F.col("object_type") == "User", 1)
            .otherwise(0)
        ).alias("direct_user_count"),
        F.sum(
            F.when(F.col("object_type") == "Computer", 1)
            .otherwise(0)
        ).alias("direct_computer_count"),
        F.sum(
            F.when(F.col("object_type") == "Group", 1)
            .otherwise(0)
        ).alias("direct_group_count")
    )
    .first()
)

post_scope_count = int(population_metrics["post_scope_count"] or 0)
post_scope_distinct_keys = int(
    population_metrics["post_scope_distinct_keys"] or 0
)
direct_user_count = int(population_metrics["direct_user_count"] or 0)
direct_computer_count = int(
    population_metrics["direct_computer_count"] or 0
)
direct_group_count = int(population_metrics["direct_group_count"] or 0)

if post_scope_count != post_scope_distinct_keys:
    raise RuntimeError(
        "Direct Post identity boundary is not unique by ad_object_key."
    )

if frames["computer_review"].count() != direct_computer_count:
    raise RuntimeError(
        "Computer hygiene review did not preserve the direct Post "
        "computer population."
    )

if frames["user_review"].count() != direct_user_count:
    raise RuntimeError(
        "User hygiene review did not preserve the direct Post user population."
    )

review_direct_group_count = (
    frames["group_review"]
    .filter(F.col("group_scope_role") == "DIRECT_POST_GROUP")
    .count()
)

if review_direct_group_count != direct_group_count:
    raise RuntimeError(
        "Group hygiene review did not preserve the direct Post group "
        "population."
    )

support_groups_in_post_boundary = (
    frames["post_scope"]
    .filter(F.col("scope_role") == "ONE_HOP_SUPPORT_GROUP")
    .count()
)

if support_groups_in_post_boundary != 0:
    raise RuntimeError(
        "One-hop support groups leaked into the direct Post boundary."
    )

cross_post_output_bindings = sum(
    1
    for table_name in TABLES.values()
    if not table_name.lower().startswith(post_prefix)
)

if cross_post_output_bindings != 0:
    raise RuntimeError(
        "Cross-Post output binding detected."
    )


# ---------------------------------------------------------------------------
# 3. Consolidated metrics
# ---------------------------------------------------------------------------

computer_priority = (
    frames["computer_review"]
    .groupBy("computer_security_priority")
    .count()
    .collect()
)

computer_counts = {
    row["computer_security_priority"]: int(row["count"])
    for row in computer_priority
}

user_priority = (
    frames["user_review"]
    .groupBy("user_security_priority")
    .count()
    .collect()
)

user_counts = {
    row["user_security_priority"]: int(row["count"])
    for row in user_priority
}

group_priority = (
    frames["group_review"]
    .groupBy("group_security_priority")
    .count()
    .collect()
)

group_counts = {
    row["group_security_priority"]: int(row["count"])
    for row in group_priority
}

membership_priority = (
    frames["membership_review"]
    .groupBy("membership_security_priority")
    .count()
    .collect()
)

membership_counts = {
    row["membership_security_priority"]: int(row["count"])
    for row in membership_priority
}

bloat_classification = (
    frames["bloat_priority"]
    .groupBy("ad_bloat_classification")
    .count()
    .collect()
)

bloat_counts = {
    row["ad_bloat_classification"]: int(row["count"])
    for row in bloat_classification
}

privileged_relationship_count = frames["privileged_review"].count()
action_queue_count = frames["action_queue"].count()

action_metrics = (
    frames["action_queue"]
    .agg(
        F.count("*").alias("action_count"),
        F.countDistinct("action_id").alias(
            "distinct_action_count"
        ),
        F.sum(
            F.when(F.col("priority") == "High", 1)
            .otherwise(0)
        ).alias("high_action_count"),
        F.sum(
            F.when(F.col("priority") == "Medium", 1)
            .otherwise(0)
        ).alias("medium_action_count"),
        F.sum(
            F.when(F.col("priority") == "Review", 1)
            .otherwise(0)
        ).alias("review_action_count")
    )
    .first()
)

if int(action_metrics["action_count"] or 0) != int(
    action_metrics["distinct_action_count"] or 0
):
    raise RuntimeError("The hygiene action queue contains duplicate action IDs.")

high_action_count = int(action_metrics["high_action_count"] or 0)
medium_action_count = int(action_metrics["medium_action_count"] or 0)
review_action_count = int(action_metrics["review_action_count"] or 0)


# ---------------------------------------------------------------------------
# 4. Final runtime contract
# ---------------------------------------------------------------------------

IP004_HYGIENE_READY = True

print("=" * 100)
print("RCAIP IP-004 - FINAL HYGIENE RUNTIME CONTRACT")
print("=" * 100)
print("Status: PASS")
print("Ready:", IP004_HYGIENE_READY)
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
print("Direct Post identity boundary:", post_scope_count)
print("  Direct users:", direct_user_count)
print("  Direct computers:", direct_computer_count)
print("  Direct groups:", direct_group_count)
print("Computer hygiene review rows:", frames["computer_review"].count())
print("  High:", computer_counts.get("High", 0))
print("  Medium:", computer_counts.get("Medium", 0))
print("  Review:", computer_counts.get("Review", 0))
print("  Low:", computer_counts.get("Low", 0))
print("  Pass:", computer_counts.get("Pass", 0))
print("User hygiene review rows:", frames["user_review"].count())
print("  High:", user_counts.get("High", 0))
print("  Medium:", user_counts.get("Medium", 0))
print("  Review:", user_counts.get("Review", 0))
print("  Low:", user_counts.get("Low", 0))
print("  Pass:", user_counts.get("Pass", 0))
print("Group context/review rows:", frames["group_review"].count())
print("  High:", group_counts.get("High", 0))
print("  Medium:", group_counts.get("Medium", 0))
print("  Review:", group_counts.get("Review", 0))
print("  Low:", group_counts.get("Low", 0))
print("  Context:", group_counts.get("Context", 0))
print("Membership context/review rows:", frames["membership_review"].count())
print("  High:", membership_counts.get("High", 0))
print("  Medium:", membership_counts.get("Medium", 0))
print("  Review:", membership_counts.get("Review", 0))
print("  Context:", membership_counts.get("Context", 0))
print("Privileged-access relationship rows:", privileged_relationship_count)
print("AD-bloat prioritization:")
print(
    "  Security-Relevant Bloat:",
    bloat_counts.get("Security-Relevant Bloat", 0)
)
print(
    "  Operationally Important Bloat:",
    bloat_counts.get("Operationally Important Bloat", 0)
)
print(
    "  Low-Priority Hygiene:",
    bloat_counts.get("Low-Priority Hygiene", 0)
)
print(
    "  Enterprise Context / Tolerable Unless Policy Changes:",
    bloat_counts.get(
        "Enterprise Context / Tolerable Unless Policy Changes",
        0
    )
)
print("Validation action rows:", action_queue_count)
print("  High:", high_action_count)
print("  Medium:", medium_action_count)
print("  Review:", review_action_count)
print("Durable hygiene outputs:", 8)
print("Missing durable outputs:", len(missing_tables))
print("Cross-Post output bindings:", cross_post_output_bindings)
print("One-hop support groups assigned to Post boundary:", support_groups_in_post_boundary)
print("Automatic account or group changes performed: 0")
print("Automatic member removals performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)


display(
    frames["summary"]
    .orderBy(
        "summary_domain",
        F.when(F.col("priority") == "High", 1)
        .when(F.col("priority") == "Medium", 2)
        .when(F.col("priority") == "Review", 3)
        .when(F.col("priority") == "Low", 4)
        .when(F.col("priority") == "Pass", 5)
        .otherwise(6)
    )
)


display(
    frames["action_queue"]
    .orderBy(
        F.when(F.col("priority") == "High", 1)
        .when(F.col("priority") == "Medium", 2)
        .otherwise(3),
        "action_domain",
        "subject_name"
    )
)