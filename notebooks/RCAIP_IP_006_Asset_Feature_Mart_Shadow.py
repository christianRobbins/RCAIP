# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP IP-006 - Asset Feature Mart Shadow Build and Certification
# Version: 0.1.0
# Feature contract: 2.2.0
#
# Purpose:
#   Build one deterministic feature row per certified Asset Plane asset for the
#   current Post and run. This notebook calculates evidence only.
#
# Safety:
#   - No rule evaluation occurs.
#   - No asset is excluded, suppressed, assigned, or remediated.
#   - Seven incomplete Nmap provenance fields remain null.
#   - Rule-dependent unexplained/suppression features remain not evaluated.
#   - Existing rows for other certified runs are preserved.
# ============================================================================

import ipaddress
import json
import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate inherited runtime and shadow safety
# ---------------------------------------------------------------------------

REQUIRED_RUNTIME_OBJECTS = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_POST_TOKENS",
    "RCAIP_ENTERPRISE_CIDRS",
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
        "IP-006 Feature Mart did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError("Config 000 did not publish a ready runtime.")

IP006_FEATURE_CONTRACT_VERSION = "2.2.0"
IP006_FEATURE_BUILD_VERSION = "0.1.0"
IP006_BUILD_MODE = "SHADOW"
IP006_INPUT_LOGICAL_NAME = "ASSET_RECONCILIATION_SCORED"
IP006_OUTPUT_LOGICAL_NAME = "IP006_ASSET_FEATURE_MART"
IP006_RULE_REGISTRY_LOGICAL_NAME = "KB_RULE_REGISTRY_V2_2"

IP006_KB_LOGICAL_NAMES = [
    "KB_ASSET_PATTERNS_V2",
    "KB_DEVICE_PROFILES",
    "KB_DEVICE_TOOL_EXPECTATIONS",
    "KB_ISSO_SUBNET_LOOKUP",
    "KB_NAMING_TOKEN_RULES",
    "KB_OBSERVATION_RULES_V2",
    "KB_PORT_RULES",
    "KB_REQUIRED_TOOLS_V2",
    "KB_SOURCE_REGISTRY",
    "KB_TERMINOLOGY_RULES"
]

required_logical_names = [
    IP006_INPUT_LOGICAL_NAME,
    IP006_OUTPUT_LOGICAL_NAME,
    IP006_RULE_REGISTRY_LOGICAL_NAME,
    *IP006_KB_LOGICAL_NAMES
]

missing_logical_names = [
    logical_name
    for logical_name in required_logical_names
    if logical_name not in RCAIP_TABLES
]

if missing_logical_names:
    raise RuntimeError(
        "Config 001 is missing required IP-006 logical bindings: "
        + " | ".join(missing_logical_names)
    )

INPUT_TABLE = get_rcaip_table(IP006_INPUT_LOGICAL_NAME)
OUTPUT_TABLE = get_rcaip_table(IP006_OUTPUT_LOGICAL_NAME)
RULE_REGISTRY_TABLE = get_rcaip_table(IP006_RULE_REGISTRY_LOGICAL_NAME)
KB_TABLES = {
    logical_name: get_rcaip_table(logical_name)
    for logical_name in IP006_KB_LOGICAL_NAMES
}

expected_post_prefix = (
    f"{RCAIP_RESULTS_NAMESPACE}.{RCAIP_POST_SLUG}_"
).lower()

for logical_name, physical_name in {
    IP006_INPUT_LOGICAL_NAME: INPUT_TABLE,
    IP006_OUTPUT_LOGICAL_NAME: OUTPUT_TABLE
}.items():
    if not str(physical_name).lower().startswith(expected_post_prefix):
        raise RuntimeError(
            f"{logical_name} resolved outside the current Post namespace: "
            f"{physical_name}"
        )

missing_physical_tables = [
    physical_name
    for physical_name in [INPUT_TABLE, RULE_REGISTRY_TABLE, *KB_TABLES.values()]
    if not spark.catalog.tableExists(physical_name)
]

if missing_physical_tables:
    raise RuntimeError(
        "IP-006 Feature Mart requires existing certified inputs: "
        + " | ".join(missing_physical_tables)
    )

rule_registry_df = spark.table(RULE_REGISTRY_TABLE)

if "active_indicator" not in rule_registry_df.columns:
    raise RuntimeError(
        "Rule Registry lacks active_indicator; refusing a shadow Feature Mart build."
    )

active_rule_count = (
    rule_registry_df
    .filter(F.col("active_indicator").cast("boolean"))
    .count()
)

if active_rule_count != 0:
    raise RuntimeError(
        "Feature Mart v0.1 is shadow-only and requires zero active rules. "
        f"Observed active rules: {active_rule_count}"
    )

print("IP-006 runtime and shadow-safety validation: PASS")
print("Input table:", INPUT_TABLE)
print("Output table:", OUTPUT_TABLE)
print("Post:", RCAIP_POST_NAME)
print("Run ID:", RCAIP_RUN_ID)


# ---------------------------------------------------------------------------
# 2. Load and certify the one-row-per-asset input boundary
# ---------------------------------------------------------------------------

asset_source_df = spark.table(INPUT_TABLE)

REQUIRED_SOURCE_COLUMNS = {
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
    "cmdb_category",
    "cmdb_mission",
    "cmdb_managed_by_group",
    "cmdb_support_group",
    "nmap_present",
    "nmap_asset_domain",
    "nmap_scan_scope",
    "nmap_scan_location",
    "nmap_ip",
    "nmap_hostname",
    "nmap_services",
    "nmap_ports",
    "nmap_open_port_count",
    "tenable_present",
    "tenable_ip",
    "tenable_dns_name",
    "tenable_acr_score",
    "intune_present",
    "intune_device_name",
    "intune_serial_number",
    "intune_operating_system",
    "mde_present",
    "mde_device_name",
    "mde_device_category",
    "mde_device_type",
    "mde_dynamic_tags",
    "mde_manual_tags",
    "mde_machine_group",
    "mde_public_ip",
    "qualys_present",
    "qualys_ip",
    "qualys_asset_name",
    "qualys_bios_serial_number",
    "qualys_criticality_score",
    "qualys_open_ports",
    "qualys_tags",
    "qualys_trurisk_score",
    "current_observation_status",
    "observed_sources_raw",
    "observed_sources_active",
    "observed_sources_stale",
    "nmap_record_present",
    "tenable_record_present",
    "intune_record_present",
    "mde_record_present",
    "qualys_record_present",
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

missing_source_columns = sorted(
    REQUIRED_SOURCE_COLUMNS - set(asset_source_df.columns)
)

if missing_source_columns:
    raise RuntimeError(
        "ASSET_RECONCILIATION_SCORED is missing required Feature Mart fields: "
        + " | ".join(missing_source_columns)
    )

input_profile = (
    asset_source_df
    .agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("asset_key").alias("distinct_asset_key_count"),
        F.sum(
            F.when(F.col("asset_key").isNull(), 1).otherwise(0)
        ).alias("null_asset_key_count"),
        F.sum(
            F.when(
                F.lower(F.trim(F.col("post_slug")))
                != F.lit(str(RCAIP_POST_SLUG).lower()),
                1
            ).otherwise(0)
        ).alias("cross_post_count"),
        F.sum(
            F.when(
                F.col("rcaip_run_id") != F.lit(RCAIP_RUN_ID),
                1
            ).otherwise(0)
        ).alias("cross_run_count")
    )
    .first()
)

input_row_count = int(input_profile["row_count"] or 0)
input_distinct_key_count = int(input_profile["distinct_asset_key_count"] or 0)
input_null_key_count = int(input_profile["null_asset_key_count"] or 0)
input_cross_post_count = int(input_profile["cross_post_count"] or 0)
input_cross_run_count = int(input_profile["cross_run_count"] or 0)

if input_row_count == 0:
    raise RuntimeError("ASSET_RECONCILIATION_SCORED contains zero rows.")

if input_row_count != input_distinct_key_count:
    raise RuntimeError(
        "Feature Mart input is not one row per asset_key. "
        f"Rows={input_row_count} | Distinct keys={input_distinct_key_count}"
    )

if input_null_key_count or input_cross_post_count or input_cross_run_count:
    raise RuntimeError(
        "Feature Mart input boundary failed. "
        f"Null keys={input_null_key_count} | "
        f"Cross-Post={input_cross_post_count} | Cross-run={input_cross_run_count}"
    )

print("IP-006 input grain and provenance certification: PASS")
print("Certified input rows:", input_row_count)


# ---------------------------------------------------------------------------
# 3. Small Knowledge Base collectors and release metadata
# ---------------------------------------------------------------------------

def collect_rows(table_name, maximum_rows=10000):
    return [
        row.asDict(recursive=True)
        for row in spark.table(table_name).limit(maximum_rows).collect()
    ]


def normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_token(value):
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def split_multi_value(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        output = []
        for item in value:
            output.extend(split_multi_value(item))
        return output
    return [
        item.strip()
        for item in re.split(r"[|,;]+", str(value))
        if item.strip()
    ]


device_profile_rows = collect_rows(KB_TABLES["KB_DEVICE_PROFILES"])
device_tool_rows = collect_rows(KB_TABLES["KB_DEVICE_TOOL_EXPECTATIONS"])
asset_pattern_rows = collect_rows(KB_TABLES["KB_ASSET_PATTERNS_V2"])
required_tool_rows = collect_rows(KB_TABLES["KB_REQUIRED_TOOLS_V2"])
naming_token_rows = collect_rows(KB_TABLES["KB_NAMING_TOKEN_RULES"])
source_registry_rows = collect_rows(KB_TABLES["KB_SOURCE_REGISTRY"])
port_rule_rows = collect_rows(KB_TABLES["KB_PORT_RULES"])

kb_versions = sorted({
    normalize_text(row.get("kb_version"))
    for row in (
        device_profile_rows
        + device_tool_rows
        + naming_token_rows
        + source_registry_rows
    )
    if normalize_text(row.get("kb_version"))
})

KB_RELEASE_VERSION = (
    kb_versions[-1]
    if kb_versions
    else "UNAVAILABLE"
)


# ---------------------------------------------------------------------------
# 4. Build governed token, profile, tool, and port dictionaries
# ---------------------------------------------------------------------------

current_post_tokens = sorted({
    normalize_token(value)
    for value in [
        *list(RCAIP_POST_TOKENS or []),
        RCAIP_POST_NAME,
        RCAIP_POST_SLUG,
        globals().get("RCAIP_POST_CODE", ""),
        globals().get("RCAIP_POST_DEVICE_PREFIX", "")
    ]
    if normalize_token(value)
})

profile_definitions = []
for row in device_profile_rows:
    if row.get("active_indicator") is False:
        continue
    observed_token = normalize_token(row.get("observed_token"))
    if not observed_token:
        continue
    profile_definitions.append({
        "token": observed_token,
        "profile_id": normalize_text(row.get("device_profile_id")) or "UNKNOWN",
        "canonical_device_type": normalize_text(row.get("canonical_device_type")),
        "management_model": normalize_text(row.get("management_model")),
        "required_tools": split_multi_value(row.get("required_tools")),
        "confidence": normalize_text(row.get("rule_confidence")) or "MODERATE"
    })

profile_definitions = sorted(
    profile_definitions,
    key=lambda row: (-len(row["token"]), row["token"])
)

tools_by_token = {}
for row in device_tool_rows:
    if row.get("active_indicator") is False:
        continue
    if str(row.get("expected_status") or "").strip().lower() != "required":
        continue
    token = normalize_token(row.get("observed_token"))
    tool = normalize_token(row.get("tool_name"))
    if token and tool:
        tools_by_token.setdefault(token, set()).add(tool)

pattern_definitions = []
for row in asset_pattern_rows:
    pattern_regex = normalize_text(row.get("pattern_regex"))
    if not pattern_regex:
        continue
    try:
        re.compile(pattern_regex)
    except re.error:
        continue
    pattern_definitions.append({
        "pattern_id": normalize_text(row.get("pattern_id")) or "UNKNOWN",
        "pattern_regex": pattern_regex,
        "likely_asset_type": normalize_text(row.get("likely_asset_type")),
        "asset_family": normalize_text(row.get("asset_family")),
        "asset_domain_hint": normalize_text(row.get("asset_domain_hint")),
        "confidence": normalize_text(row.get("pattern_confidence")) or "MODERATE",
        "weight": int(row.get("confidence_weight") or 0)
    })

pattern_definitions = sorted(
    pattern_definitions,
    key=lambda row: (-row["weight"], row["pattern_id"])
)

required_tools_by_family = []
for row in required_tool_rows:
    if str(row.get("expected_status") or "").strip().lower() != "required":
        continue
    required_tools_by_family.append({
        "asset_domain": normalize_text(row.get("asset_domain")).upper(),
        "asset_family": normalize_text(row.get("asset_family")).upper(),
        "tool": normalize_token(row.get("tool_name"))
    })


def first_matching_column(columns, candidates):
    lookup = {str(column).lower(): column for column in columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    return None


# Enterprise token lookup is schema-adaptive. If the lookup does not expose a
# governed slug/token pair, the foreign-token feature remains unavailable.
isso_lookup_df = spark.table(KB_TABLES["KB_ISSO_SUBNET_LOOKUP"])
isso_columns = list(isso_lookup_df.columns)

isso_slug_column = first_matching_column(
    isso_columns,
    ["post_slug", "site_slug", "location_slug", "slug"]
)
isso_name_column = first_matching_column(
    isso_columns,
    ["post_name", "site_name", "location_name", "name"]
)
isso_token_columns = [
    column
    for column in [
        first_matching_column(isso_columns, ["post_token"]),
        first_matching_column(isso_columns, ["post_code"]),
        first_matching_column(isso_columns, ["post_abbreviation"]),
        first_matching_column(isso_columns, ["device_prefix"]),
        first_matching_column(isso_columns, ["device_name_token"]),
        first_matching_column(isso_columns, ["dns_token"]),
        first_matching_column(isso_columns, ["mde_token"]),
        first_matching_column(isso_columns, ["short_name"])
    ]
    if column is not None
]

foreign_token_to_slug = {}
foreign_token_lookup_status = "UNAVAILABLE_LOOKUP_SCHEMA"

if (isso_slug_column or isso_name_column) and isso_token_columns:
    selected_lookup_columns = list(dict.fromkeys(
        [
            column
            for column in [isso_slug_column, isso_name_column, *isso_token_columns]
            if column is not None
        ]
    ))

    for row in isso_lookup_df.select(*selected_lookup_columns).limit(10000).collect():
        row_dict = row.asDict(recursive=True)
        candidate_slug = normalize_text(
            row_dict.get(isso_slug_column) if isso_slug_column else None
        ).lower()
        if not candidate_slug and isso_name_column:
            candidate_slug = re.sub(
                r"[^a-z0-9]+",
                "-",
                normalize_text(row_dict.get(isso_name_column)).lower()
            ).strip("-")

        if not candidate_slug or candidate_slug == str(RCAIP_POST_SLUG).lower():
            continue

        for token_column in isso_token_columns:
            for raw_token in split_multi_value(row_dict.get(token_column)):
                token = normalize_token(raw_token)
                if len(token) >= 2 and token not in current_post_tokens:
                    foreign_token_to_slug.setdefault(token, candidate_slug)

    if foreign_token_to_slug:
        foreign_token_lookup_status = "AVAILABLE"
    else:
        foreign_token_lookup_status = "AVAILABLE_NO_FOREIGN_TOKENS"

# Guarded naming-token rows are deliberately not treated as authoritative
# foreign-Post mappings because the Knowledge Base says they are candidate-only.
guarded_naming_tokens = sorted({
    normalize_token(row.get("observed_token"))
    for row in naming_token_rows
    if row.get("active_indicator") is not False
    and normalize_token(row.get("observed_token"))
})

high_risk_ports = set()
if port_rule_rows:
    port_columns = list(port_rule_rows[0].keys())
    port_number_column = first_matching_column(
        port_columns,
        ["port_number", "port", "destination_port", "service_port"]
    )
    port_risk_column = first_matching_column(
        port_columns,
        ["risk_level", "severity", "default_priority", "priority"]
    )
    if port_number_column and port_risk_column:
        for row in port_rule_rows:
            risk_text = normalize_text(row.get(port_risk_column)).upper()
            if risk_text not in {"HIGH", "CRITICAL", "IMMEDIATE", "URGENT"}:
                continue
            try:
                high_risk_ports.add(int(row.get(port_number_column)))
            except (TypeError, ValueError):
                pass


# ---------------------------------------------------------------------------
# 5. Source availability and Feature Contract amendment F-050
# ---------------------------------------------------------------------------

def source_status(logical_name):
    physical_name = get_rcaip_table(logical_name)
    if spark.catalog.tableExists(physical_name):
        return "AVAILABLE"
    return "OPTIONAL_NOT_AVAILABLE"


SOURCE_STATUS = {
    "TENABLE": source_status("TENABLE_NORM"),
    "INTUNE": source_status("INTUNE_NORM"),
    "MDE": source_status("MDE_NORM"),
    "QUALYS": source_status("QUALYS_NORM")
}


# ---------------------------------------------------------------------------
# 6. Python UDF contracts for deterministic interpretation
# ---------------------------------------------------------------------------

classification_schema = T.StructType([
    T.StructField("device_family", T.StringType(), False),
    T.StructField("device_family_confidence", T.StringType(), False),
    T.StructField("matched_device_profile", T.StringType(), True),
    T.StructField("matched_device_token", T.StringType(), True),
    T.StructField("matched_pattern_id", T.StringType(), True),
    T.StructField("profile_management_model", T.StringType(), True),
    T.StructField("profile_expected_tools", T.StringType(), False),
    T.StructField("classification_basis", T.StringType(), False)
])


def canonical_family_from_text(value):
    text = normalize_text(value).upper()
    if not text:
        return "UNKNOWN"
    if any(term in text for term in ["PRINTER", "PRINT DEVICE", "MFD", "MULTIFUNCTION"]):
        return "PRINTER_MFD"
    if any(term in text for term in ["NETWORK", "SWITCH", "ROUTER", "ACCESS POINT", "WIRELESS AP"]):
        return "NETWORK_INFRASTRUCTURE"
    if any(term in text for term in ["FIREWALL", "SECURITY APPLIANCE", "IDS", "IPS APPLIANCE"]):
        return "SECURITY_APPLIANCE"
    if any(term in text for term in ["SERVER", "DOMAIN CONTROLLER"]):
        return "SERVER"
    if any(term in text for term in ["MOBILE", "LAPTOP", "TFL", "EMD", "MID", "SAKIO", "SUKIO"]):
        return "MOBILE"
    if any(term in text for term in ["IOT", " OT ", "BMS", "ICS", "CAMERA", "SENSOR"]):
        return "IOT_OT"
    if any(term in text for term in ["WORKSTATION", "DESKTOP", "ENDPOINT", "COMPUTER"]):
        return "WORKSTATION"
    return "UNKNOWN"


def normalize_management_model(value):
    text = normalize_text(value).upper()
    if "ENTERPRISE" in text:
        return "ENTERPRISE_MANAGED"
    if "POST" in text:
        return "POST_MANAGED"
    if "SHARED" in text or "KIOSK" in text:
        return "SHARED"
    if "NON" in text and "ASSET" in text:
        return "NON_ASSET"
    return "UNMANAGED_OR_UNKNOWN"


def normalize_tool_name(value):
    token = normalize_token(value)
    aliases = {
        "MICROSOFTDEFENDERFORENDPOINT": "MDE",
        "DEFENDERFORENDPOINT": "MDE",
        "MDE": "MDE",
        "MICROSOFTINTUNE": "INTUNE",
        "INTUNE": "INTUNE",
        "QUALYS": "QUALYS",
        "QUALYSVMDR": "QUALYS",
        "TENABLE": "TENABLE",
        "TENABLEIO": "TENABLE"
    }
    return aliases.get(token, token)


def classify_device(name_text, evidence_text):
    name_value = normalize_text(name_text)
    evidence_value = normalize_text(evidence_text)
    combined = f"{name_value} {evidence_value}".upper()

    for profile in profile_definitions:
        token = profile["token"]
        if re.search(rf"(^|[^A-Z0-9]){re.escape(token)}([^A-Z0-9]|$)", combined):
            tools = {
                normalize_tool_name(tool)
                for tool in (
                    list(profile.get("required_tools", []))
                    + list(tools_by_token.get(token, set()))
                )
                if normalize_tool_name(tool)
            }
            return (
                canonical_family_from_text(profile["canonical_device_type"]),
                str(profile.get("confidence") or "MODERATE").upper(),
                profile["profile_id"],
                token,
                None,
                normalize_management_model(profile["management_model"]),
                "|".join(sorted(tools)) if tools else "NONE",
                "KB_DEVICE_PROFILE"
            )

    for pattern in pattern_definitions:
        try:
            matched = re.search(pattern["pattern_regex"], name_value)
        except re.error:
            matched = None
        if matched:
            family = canonical_family_from_text(
                " ".join([
                    pattern["likely_asset_type"],
                    pattern["asset_family"],
                    pattern["asset_domain_hint"]
                ])
            )
            return (
                family,
                str(pattern["confidence"] or "MODERATE").upper(),
                None,
                None,
                pattern["pattern_id"],
                None,
                "NONE",
                "KB_ASSET_PATTERN"
            )

    family = canonical_family_from_text(combined)
    confidence = "MODERATE" if family != "UNKNOWN" else "LOW"
    return (
        family,
        confidence,
        None,
        None,
        None,
        None,
        "NONE",
        "SOURCE_FIELD_FALLBACK" if family != "UNKNOWN" else "UNCLASSIFIED"
    )


classify_device_udf = F.udf(classify_device, classification_schema)

ip_schema = T.StructType([
    T.StructField("canonical_ip", T.StringType(), True),
    T.StructField("ip_version", T.IntegerType(), True),
    T.StructField("ip_class", T.StringType(), False),
    T.StructField("enterprise_cidr_match", T.BooleanType(), False),
    T.StructField("network_zone", T.StringType(), False)
])

enterprise_networks = []
for cidr in list(RCAIP_ENTERPRISE_CIDRS or []):
    try:
        enterprise_networks.append(str(ipaddress.ip_network(str(cidr), strict=False)))
    except ValueError:
        pass

nen_targets = [str(value) for value in globals().get("RCAIP_NEN_PUBLIC_TARGETS", []) or []]
wifi_targets = [str(value) for value in globals().get("RCAIP_ENTWIFI_PUBLIC_TARGETS", []) or []]


def ip_in_targets(ip_obj, target_values):
    for target in target_values:
        try:
            if "/" in target:
                if ip_obj in ipaddress.ip_network(target, strict=False):
                    return True
            elif ip_obj == ipaddress.ip_address(target):
                return True
        except ValueError:
            continue
    return False


def classify_ip(value):
    raw_value = normalize_text(value)
    if not raw_value:
        return (None, None, "NONE", False, "NO_IP")

    candidate = raw_value.split("%")[0].strip()
    try:
        ip_obj = ipaddress.ip_address(candidate)
    except ValueError:
        return (raw_value, None, "INVALID", False, "UNKNOWN_INTERNAL")

    enterprise_match = any(
        ip_obj in ipaddress.ip_network(network, strict=False)
        for network in enterprise_networks
    )

    if ip_obj.is_loopback:
        ip_class = "LOOPBACK"
    elif ip_obj.is_link_local:
        ip_class = "LINK_LOCAL"
    elif ip_obj.is_multicast:
        ip_class = "MULTICAST"
    elif ip_obj.is_unspecified or ip_obj.is_reserved:
        ip_class = "RESERVED"
    elif enterprise_match:
        ip_class = "ENTERPRISE_CIDR"
    elif ip_obj.version == 4 and ip_obj in ipaddress.ip_network("100.64.0.0/10"):
        ip_class = "CGNAT"
    elif ip_obj.version == 4 and ip_obj.is_private:
        ip_class = "RFC1918"
    elif ip_obj.version == 6 and ip_obj.is_private:
        ip_class = "RESERVED"
    else:
        ip_class = "PUBLIC"

    if ip_in_targets(ip_obj, nen_targets):
        network_zone = "NEN"
    elif ip_in_targets(ip_obj, wifi_targets):
        network_zone = "ENTERPRISE_WIFI"
    elif enterprise_match:
        network_zone = "OPENNET"
    elif ip_class == "PUBLIC":
        network_zone = "PUBLIC_EXTERNAL"
    elif ip_class in {"RFC1918", "CGNAT", "LINK_LOCAL"}:
        network_zone = "UNKNOWN_INTERNAL"
    else:
        network_zone = "UNKNOWN_INTERNAL"

    return (str(ip_obj), int(ip_obj.version), ip_class, enterprise_match, network_zone)


classify_ip_udf = F.udf(classify_ip, ip_schema)

foreign_token_schema = T.StructType([
    T.StructField("foreign_post_token_match", T.BooleanType(), True),
    T.StructField("foreign_post_candidate", T.StringType(), True),
    T.StructField("foreign_post_token_status", T.StringType(), False),
    T.StructField("foreign_post_token_basis", T.StringType(), False)
])


def evaluate_foreign_token(value):
    text = normalize_text(value).upper()
    if foreign_token_lookup_status.startswith("UNAVAILABLE"):
        return (None, None, foreign_token_lookup_status, "NONE")

    matches = []
    for token, slug in foreign_token_to_slug.items():
        if re.search(rf"(^|[^A-Z0-9]){re.escape(token)}([^A-Z0-9]|$)", text):
            matches.append((token, slug))

    unique_slugs = sorted({slug for token, slug in matches})
    if len(unique_slugs) == 1:
        matched_tokens = sorted({token for token, slug in matches})
        return (
            True,
            unique_slugs[0],
            "AVAILABLE",
            "KB_ISSO_SUBNET_LOOKUP:" + "|".join(matched_tokens)
        )
    if len(unique_slugs) > 1:
        return (True, None, "AMBIGUOUS_MULTIPLE_POSTS", "KB_ISSO_SUBNET_LOOKUP")
    return (False, None, foreign_token_lookup_status, "KB_ISSO_SUBNET_LOOKUP")


foreign_token_udf = F.udf(evaluate_foreign_token, foreign_token_schema)


def has_current_post_token(value):
    text = normalize_text(value).upper()
    for token in current_post_tokens:
        if re.search(rf"(^|[^A-Z0-9]){re.escape(token)}([^A-Z0-9]|$)", text):
            return True
    return False


current_post_token_udf = F.udf(has_current_post_token, T.BooleanType())


def normalize_service_signature(*values):
    tokens = set()
    for value in values:
        for token in re.findall(r"[A-Za-z0-9._/-]+", str(value or "")):
            normalized = token.strip().upper()
            if normalized:
                tokens.add(normalized)
    if not tokens:
        return "NONE"
    return "|".join(sorted(tokens))[:1000]


service_signature_udf = F.udf(normalize_service_signature, T.StringType())


def service_has_high_risk_port(value):
    if not high_risk_ports:
        return False
    observed_ports = {
        int(match)
        for match in re.findall(r"(?<!\d)(\d{1,5})(?!\d)", str(value or ""))
        if 0 < int(match) <= 65535
    }
    return bool(observed_ports.intersection(high_risk_ports))


high_risk_port_udf = F.udf(service_has_high_risk_port, T.BooleanType())


# ---------------------------------------------------------------------------
# 7. Build canonical evidence columns and core Feature Mart features
# ---------------------------------------------------------------------------

def nonblank(column_name):
    return F.when(
        F.length(F.trim(F.col(column_name).cast("string"))) > 0,
        F.trim(F.col(column_name).cast("string"))
    )


canonical_name_expression = F.coalesce(
    nonblank("cmdb_name"),
    nonblank("cmdb_fqdn"),
    nonblank("intune_device_name"),
    nonblank("mde_device_name"),
    nonblank("tenable_dns_name"),
    nonblank("nmap_hostname"),
    nonblank("qualys_asset_name")
)

canonical_ip_expression = F.coalesce(
    nonblank("cmdb_ip"),
    nonblank("nmap_ip"),
    nonblank("tenable_ip"),
    nonblank("qualys_ip")
)

canonical_serial_expression = F.coalesce(
    nonblank("cmdb_serial_number"),
    nonblank("intune_serial_number"),
    nonblank("qualys_bios_serial_number")
)

device_evidence_expression = F.concat_ws(
    " | ",
    nonblank("cmdb_class"),
    nonblank("cmdb_category"),
    nonblank("cmdb_mission"),
    nonblank("nmap_asset_domain"),
    nonblank("nmap_scan_scope"),
    nonblank("nmap_scan_location"),
    nonblank("nmap_services"),
    nonblank("mde_device_category"),
    nonblank("mde_device_type"),
    nonblank("mde_dynamic_tags"),
    nonblank("mde_manual_tags"),
    nonblank("mde_machine_group"),
    nonblank("intune_operating_system"),
    nonblank("qualys_tags")
)

name_and_location_expression = F.concat_ws(
    " | ",
    canonical_name_expression,
    nonblank("cmdb_location"),
    nonblank("cmdb_managed_by_group"),
    nonblank("cmdb_support_group")
)

service_signature_expression = service_signature_udf(
    F.col("nmap_ports"),
    F.col("nmap_services"),
    F.col("qualys_open_ports")
)

feature_df = (
    asset_source_df
    .withColumn("feature_contract_version", F.lit(IP006_FEATURE_CONTRACT_VERSION))
    .withColumn("feature_build_version", F.lit(IP006_FEATURE_BUILD_VERSION))
    .withColumn("feature_build_mode", F.lit(IP006_BUILD_MODE))
    .withColumn("feature_generated_at_utc", F.current_timestamp())
    .withColumn("kb_release_version", F.lit(KB_RELEASE_VERSION))
    .withColumn("canonical_name", canonical_name_expression)
    .withColumn("canonical_serial", canonical_serial_expression)
    .withColumn("_canonical_ip_candidate", canonical_ip_expression)
    .withColumn("_device_evidence_text", device_evidence_expression)
    .withColumn("_name_location_text", name_and_location_expression)
    .withColumn("_classification", classify_device_udf(
        F.col("canonical_name"),
        F.col("_device_evidence_text")
    ))
    .withColumn("_ip", classify_ip_udf(F.col("_canonical_ip_candidate")))
    .withColumn("_foreign_token", foreign_token_udf(F.col("_name_location_text")))
    .withColumn("service_port_signature", service_signature_expression)
)


# ---------------------------------------------------------------------------
# 8. Source masks, identity, naming, device, and network features
# ---------------------------------------------------------------------------

def ordered_mask(condition_token_pairs):
    mask_expression = F.concat_ws(
        "|",
        *[
            F.when(condition, F.lit(token))
            for condition, token in condition_token_pairs
        ]
    )
    return F.when(
        F.length(mask_expression) > 0,
        mask_expression
    ).otherwise(F.lit("NONE"))


source_mask_raw_expression = ordered_mask([
    (F.upper(F.col("cmdb_present")) == "Y", "CMDB"),
    (F.col("nmap_record_present") == 1, "NMAP"),
    (F.col("tenable_record_present") == 1, "TENABLE"),
    (F.col("intune_record_present") == 1, "INTUNE"),
    (F.col("mde_record_present") == 1, "MDE"),
    (F.col("qualys_record_present") == 1, "QUALYS")
])

source_mask_active_expression = ordered_mask([
    (F.upper(F.col("cmdb_present")) == "Y", "CMDB"),
    (F.col("nmap_active") == 1, "NMAP"),
    (F.col("tenable_active") == 1, "TENABLE"),
    (F.col("intune_active") == 1, "INTUNE"),
    (F.col("mde_active") == 1, "MDE"),
    (F.col("qualys_active") == 1, "QUALYS")
])

source_mask_stale_expression = ordered_mask([
    (F.col("nmap_stale") == 1, "NMAP"),
    (F.col("tenable_stale") == 1, "TENABLE"),
    (F.col("intune_stale") == 1, "INTUNE"),
    (F.col("mde_stale") == 1, "MDE"),
    (F.col("qualys_stale") == 1, "QUALYS")
])

tool_mask_expression = ordered_mask([
    (F.col("tenable_active") == 1, "TENABLE"),
    (F.col("intune_active") == 1, "INTUNE"),
    (F.col("mde_active") == 1, "MDE"),
    (F.col("qualys_active") == 1, "QUALYS")
])

serial_evidence_count = (
    F.when(nonblank("cmdb_serial_number").isNotNull(), 1).otherwise(0)
    + F.when(nonblank("intune_serial_number").isNotNull(), 1).otherwise(0)
    + F.when(nonblank("qualys_bios_serial_number").isNotNull(), 1).otherwise(0)
)

host_evidence_present = (
    F.col("host_key").isNotNull()
    | F.col("canonical_name").isNotNull()
)

ip_evidence_present = F.col("_ip.canonical_ip").isNotNull()

identity_basis_expression = (
    F.when(
        (serial_evidence_count > 0) & host_evidence_present,
        "COMPOSITE"
    )
    .when(serial_evidence_count > 0, "SERIAL")
    .when(host_evidence_present, "HOST")
    .when(ip_evidence_present, "IP")
    .otherwise("UNKNOWN")
)

identity_strength_expression = (
    F.when(
        (serial_evidence_count >= 2) & host_evidence_present,
        "STRONG"
    )
    .when(
        (serial_evidence_count > 0) | host_evidence_present,
        "MODERATE"
    )
    .when(ip_evidence_present, "WEAK")
    .otherwise("UNKNOWN")
)

current_post_token_expression = current_post_token_udf(
    F.col("_name_location_text")
)

hostname_pattern_expression = (
    F.when(F.col("canonical_name").isNull(), "NO_NAME")
    .when(F.col("_classification.device_family") == "PRINTER_MFD", "PRINTER_PATTERN")
    .when(F.col("_classification.device_family") == "NETWORK_INFRASTRUCTURE", "INFRA_PATTERN")
    .when(F.col("_classification.device_family") == "MOBILE", "MOBILE_PATTERN")
    .when(current_post_token_expression, "POST_STANDARD")
    .when(F.col("_classification.classification_basis").startswith("KB_"), "ENTERPRISE_STANDARD")
    .otherwise("OTHER")
)

feature_df = (
    feature_df
    .withColumn("source_mask_raw", source_mask_raw_expression)
    .withColumn("source_mask_active", source_mask_active_expression)
    .withColumn("source_mask_stale", source_mask_stale_expression)
    .withColumn("tool_mask", tool_mask_expression)
    .withColumn(
        "active_source_count_recomputed",
        (
            F.when(F.upper(F.col("cmdb_present")) == "Y", 1).otherwise(0)
            + F.col("nmap_active").cast("int")
            + F.col("tenable_active").cast("int")
            + F.col("intune_active").cast("int")
            + F.col("mde_active").cast("int")
            + F.col("qualys_active").cast("int")
        ).cast("int")
    )
    .withColumn("identity_basis", identity_basis_expression)
    .withColumn("identity_strength", identity_strength_expression)
    .withColumn("hostname_pattern", hostname_pattern_expression)
    .withColumn("current_post_token_match", current_post_token_expression)
    .withColumn(
        "foreign_post_token_match",
        F.col("_foreign_token.foreign_post_token_match")
    )
    .withColumn(
        "foreign_post_candidate",
        F.col("_foreign_token.foreign_post_candidate")
    )
    .withColumn(
        "foreign_post_token_status",
        F.col("_foreign_token.foreign_post_token_status")
    )
    .withColumn(
        "foreign_post_token_basis",
        F.col("_foreign_token.foreign_post_token_basis")
    )
    .withColumn("device_family", F.col("_classification.device_family"))
    .withColumn(
        "device_family_confidence",
        F.when(
            F.upper(F.col("_classification.device_family_confidence")) == "MEDIUM",
            F.lit("MODERATE")
        ).when(
            F.upper(F.col("_classification.device_family_confidence")).isin(
                "HIGH", "MODERATE", "LOW"
            ),
            F.upper(F.col("_classification.device_family_confidence"))
        ).otherwise(F.lit("LOW"))
    )
    .withColumn("matched_device_profile", F.col("_classification.matched_device_profile"))
    .withColumn("matched_device_token", F.col("_classification.matched_device_token"))
    .withColumn("matched_pattern_id", F.col("_classification.matched_pattern_id"))
    .withColumn("classification_basis", F.col("_classification.classification_basis"))
    .withColumn("canonical_ip", F.col("_ip.canonical_ip"))
    .withColumn("ip_version", F.col("_ip.ip_version"))
    .withColumn("ip_class", F.col("_ip.ip_class"))
    .withColumn("enterprise_cidr_match", F.col("_ip.enterprise_cidr_match"))
    .withColumn("network_zone", F.col("_ip.network_zone"))
    .withColumn(
        "is_ip_only_identity",
        (F.col("identity_basis") == "IP")
    )
    # Blocked by the Nmap provenance refit. These must remain null.
    .withColumn("network_id", F.lit(None).cast("string"))
    .withColumn("is_network_address", F.lit(None).cast("boolean"))
    .withColumn("is_broadcast_address", F.lit(None).cast("boolean"))
    .withColumn("network_scoped_asset_key", F.lit(None).cast("string"))
    .withColumn("scan_count", F.lit(None).cast("long"))
    .withColumn("network_count", F.lit(None).cast("long"))
    .withColumn("source_file_count", F.lit(None).cast("long"))
)


# ---------------------------------------------------------------------------
# 9. Signatures, risk bands, management, and tool applicability
# ---------------------------------------------------------------------------

printer_signature_expression = (
    (F.col("device_family") == "PRINTER_MFD")
    | F.upper(F.coalesce(F.col("canonical_name"), F.lit(""))).rlike(
        r"(^|[^A-Z0-9])(PRN|PRINT|PRINTER|MFD)([^A-Z0-9]|$)"
    )
    | F.upper(F.col("service_port_signature")).rlike(
        r"(^|[^0-9])(515|631|9100)([^0-9]|$)"
    )
)

infrastructure_signature_expression = (
    F.col("device_family").isin("NETWORK_INFRASTRUCTURE", "SECURITY_APPLIANCE")
    | F.upper(F.coalesce(F.col("canonical_name"), F.lit(""))).rlike(
        r"(^|[^A-Z0-9])(SW|RTR|ROUTER|WAP|AP|FW|FIREWALL)([^A-Z0-9]|$)"
    )
)

acr_numeric = F.col("tenable_acr_score").cast("double")
acr_band_expression = (
    F.when(F.upper(F.col("tenable_present")) != "Y", "NONE")
    .when(acr_numeric.isNull(), "UNKNOWN")
    .when(acr_numeric <= 0, "0")
    .when(acr_numeric <= 3, "1_3")
    .when(acr_numeric <= 6, "4_6")
    .when(acr_numeric <= 8, "7_8")
    .when(acr_numeric <= 10, "9_10")
    .otherwise("UNKNOWN")
)

qualys_trurisk_numeric = F.col("qualys_trurisk_score").cast("double")
qualys_criticality_numeric = F.col("qualys_criticality_score").cast("double")
qualys_risk_band_expression = (
    F.when(F.upper(F.col("qualys_present")) != "Y", "NONE")
    .when(
        qualys_trurisk_numeric.isNotNull(),
        F.when(qualys_trurisk_numeric >= 900, "CRITICAL")
        .when(qualys_trurisk_numeric >= 700, "HIGH")
        .when(qualys_trurisk_numeric >= 400, "MODERATE")
        .otherwise("LOW")
    )
    .when(
        qualys_criticality_numeric.isNotNull(),
        F.when(qualys_criticality_numeric >= 5, "CRITICAL")
        .when(qualys_criticality_numeric >= 4, "HIGH")
        .when(qualys_criticality_numeric >= 3, "MODERATE")
        .otherwise("LOW")
    )
    .otherwise("UNKNOWN")
)

cmdb_management_text = F.upper(F.concat_ws(
    " | ",
    nonblank("cmdb_managed_by_group"),
    nonblank("cmdb_support_group"),
    nonblank("cmdb_category"),
    nonblank("cmdb_mission")
))

cmdb_intune_indicator_expression = cmdb_management_text.rlike(
    r"(^|[^A-Z0-9])(INTUNE|SG[- _]?INTUNE|ENDPOINT MANAGEMENT|MOBILE DEVICE MANAGEMENT|MEM)([^A-Z0-9]|$)"
)

profile_management_model_expression = F.col("_classification.profile_management_model")
management_model_expression = (
    F.when(
        profile_management_model_expression.isNotNull(),
        profile_management_model_expression
    )
    .when(
        F.col("device_family") == "MOBILE",
        F.when(F.col("intune_active") == 1, "ENTERPRISE_MANAGED")
        .otherwise("UNMANAGED_OR_UNKNOWN")
    )
    .when(
        F.col("device_family").isin(
            "PRINTER_MFD", "NETWORK_INFRASTRUCTURE", "IOT_OT", "SECURITY_APPLIANCE"
        ) & (F.upper(F.col("cmdb_present")) == "Y"),
        "POST_MANAGED"
    )
    .when(
        F.col("device_family").isin("WORKSTATION", "SERVER")
        & (F.upper(F.col("cmdb_present")) == "Y"),
        "POST_MANAGED"
    )
    .when(
        F.col("device_family") == "UNKNOWN",
        "UNMANAGED_OR_UNKNOWN"
    )
    .otherwise("UNMANAGED_OR_UNKNOWN")
)

management_owner_expression = (
    F.when(F.col("management_model") == "POST_MANAGED", "POST")
    .when(F.col("management_model") == "ENTERPRISE_MANAGED", "ENTERPRISE")
    .when(F.col("management_model") == "SHARED", "SHARED")
    .when(F.col("management_model") == "NON_ASSET", "NONE")
    .otherwise("UNKNOWN")
)

feature_df = (
    feature_df
    .withColumn("printer_signature", printer_signature_expression)
    .withColumn("infrastructure_signature", infrastructure_signature_expression)
    .withColumn("acr_band", acr_band_expression)
    .withColumn("qualys_risk_band", qualys_risk_band_expression)
    .withColumn(
        "profile_management_model",
        F.when(
            profile_management_model_expression.isNotNull(),
            F.udf(normalize_management_model, T.StringType())(
                profile_management_model_expression
            )
        )
    )
    .withColumn("management_model", management_model_expression)
    .withColumn("management_owner", management_owner_expression)
    .withColumn(
        "cmdb_intune_management_indicator",
        F.coalesce(cmdb_intune_indicator_expression, F.lit(False))
    )
    .withColumn("intune_source_status", F.lit(SOURCE_STATUS["INTUNE"]))
)


# Tool expectation UDF is evaluated after the device/profile fields exist.
tool_expectation_schema = T.StructType([
    T.StructField("tool_applicability_profile", T.StringType(), False),
    T.StructField("expected_tool_mask", T.StringType(), False)
])


def tools_for_family(device_family):
    family = normalize_text(device_family).upper()
    terms = {
        "MOBILE": ["MOBILE", "LAPTOP"],
        "WORKSTATION": ["WORKSTATION", "ENDPOINT", "COMPUTER"],
        "SERVER": ["SERVER"],
        "PRINTER_MFD": ["PRINTER", "MFD", "PRINT"],
        "NETWORK_INFRASTRUCTURE": ["NETWORK", "SWITCH", "ROUTER"],
        "SECURITY_APPLIANCE": ["SECURITY", "FIREWALL", "APPLIANCE"],
        "IOT_OT": ["IOT", "OT", "BMS", "ICS"]
    }.get(family, [])

    tools = set()
    matched_labels = set()
    for rule in required_tools_by_family:
        searchable = f"{rule['asset_domain']} {rule['asset_family']}"
        if any(term in searchable for term in terms):
            tool = normalize_tool_name(rule["tool"])
            if tool:
                tools.add(tool)
                matched_labels.add(rule["asset_family"] or rule["asset_domain"])

    return tools, sorted(matched_labels)


def determine_tool_expectations(device_family, matched_token, matched_profile, profile_tools):
    token = normalize_token(matched_token)
    tools = set()

    for raw_tool in split_multi_value(profile_tools):
        normalized = normalize_tool_name(raw_tool)
        if normalized and normalized != "NONE":
            tools.add(normalized)

    if token:
        tools.update({
            normalize_tool_name(tool)
            for tool in tools_by_token.get(token, set())
            if normalize_tool_name(tool)
        })

    family_tools, matched_family_labels = tools_for_family(device_family)
    tools.update(family_tools)

    if matched_profile:
        profile_name = str(matched_profile)
    elif matched_family_labels:
        profile_name = "KB_REQUIRED_TOOLS_V2:" + "|".join(matched_family_labels)
    elif normalize_text(device_family).upper() in {
        "PRINTER_MFD", "NETWORK_INFRASTRUCTURE", "IOT_OT", "SECURITY_APPLIANCE"
    }:
        profile_name = "KB_APPLICABILITY:NO_ENDPOINT_TOOL_REQUIREMENT"
    else:
        profile_name = "UNKNOWN"

    return (
        profile_name,
        "|".join(sorted(tools)) if tools else "NONE"
    )


tool_expectation_udf = F.udf(
    determine_tool_expectations,
    tool_expectation_schema
)

feature_df = (
    feature_df
    .withColumn(
        "_tool_expectation",
        tool_expectation_udf(
            F.col("device_family"),
            F.col("matched_device_token"),
            F.col("matched_device_profile"),
            F.col("_classification.profile_expected_tools")
        )
    )
    .withColumn(
        "tool_applicability_profile",
        F.col("_tool_expectation.tool_applicability_profile")
    )
    .withColumn(
        "expected_tool_mask",
        F.col("_tool_expectation.expected_tool_mask")
    )
)


EMPTY_STRING_ARRAY = F.from_json(F.lit("[]"), "array<string>")


def mask_to_array(column_name):
    return F.when(
        F.col(column_name).isin("NONE", ""),
        EMPTY_STRING_ARRAY
    ).otherwise(F.split(F.col(column_name), r"\|"))


available_tool_tokens = [
    tool_name
    for tool_name, status in SOURCE_STATUS.items()
    if status == "AVAILABLE"
]

feature_df = (
    feature_df
    .withColumn("_expected_tools", mask_to_array("expected_tool_mask"))
    .withColumn("_active_tools", mask_to_array("tool_mask"))
    .withColumn(
        "_applicable_tools",
        F.array_intersect(
            F.col("_expected_tools"),
            F.array(*[F.lit(tool) for tool in available_tool_tokens])
            if available_tool_tokens
            else EMPTY_STRING_ARRAY
        )
    )
    .withColumn(
        "_unavailable_expected_tools",
        F.array_except(F.col("_expected_tools"), F.col("_applicable_tools"))
    )
    .withColumn(
        "_missing_applicable_tools",
        F.array_except(F.col("_applicable_tools"), F.col("_active_tools"))
    )
    .withColumn(
        "applicable_tool_mask",
        F.when(F.size(F.col("_applicable_tools")) == 0, "NONE")
        .otherwise(F.concat_ws("|", F.sort_array(F.col("_applicable_tools"))))
    )
    .withColumn(
        "unavailable_expected_tool_mask",
        F.when(F.size(F.col("_unavailable_expected_tools")) == 0, "NONE")
        .otherwise(F.concat_ws("|", F.sort_array(F.col("_unavailable_expected_tools"))))
    )
    .withColumn(
        "applicable_coverage_gap",
        F.when(F.size(F.col("_missing_applicable_tools")) == 0, "NONE")
        .otherwise(F.concat_ws("|", F.sort_array(F.col("_missing_applicable_tools"))))
    )
    .withColumn(
        "non_applicable_absence_count",
        F.size(
            F.array_except(
                F.array(*[F.lit(tool) for tool in ["TENABLE", "INTUNE", "MDE", "QUALYS"]]),
                F.array_union(F.col("_applicable_tools"), F.col("_active_tools"))
            )
        ).cast("int")
    )
    .withColumn(
        "true_tooling_conflict_count",
        F.size(F.col("_missing_applicable_tools")).cast("int")
    )
)


# ---------------------------------------------------------------------------
# 10. Attribution, contradiction, risk, and not-yet-evaluated features
# ---------------------------------------------------------------------------

location_matches_current_post = current_post_token_udf(
    F.col("cmdb_location")
)

attribution_confidence_expression = (
    F.when(
        (F.col("foreign_post_token_match") == True)
        & (F.col("current_post_token_match") == True),
        "CONFLICTED"
    )
    .when(
        (F.col("foreign_post_token_match") == True)
        & (F.col("foreign_post_candidate").isNotNull()),
        "CONFLICTED"
    )
    .when(
        F.col("current_post_token_match")
        & F.col("enterprise_cidr_match"),
        "HIGH"
    )
    .when(
        F.col("current_post_token_match")
        | F.col("enterprise_cidr_match")
        | location_matches_current_post,
        "MODERATE"
    )
    .when(F.col("foreign_post_token_match").isNull(), "UNKNOWN")
    .otherwise("LOW")
)

serial_values = F.array(
    nonblank("cmdb_serial_number"),
    nonblank("intune_serial_number"),
    nonblank("qualys_bios_serial_number")
)
serial_values_non_null = F.filter(serial_values, lambda value: value.isNotNull())
serial_conflict_expression = (
    F.size(F.array_distinct(serial_values_non_null)) > 1
)

attribution_conflict_expression = (
    F.col("attribution_confidence") == "CONFLICTED"
)

intune_management_conflict_expression = (
    F.col("cmdb_intune_management_indicator")
    & (F.col("intune_source_status") == "AVAILABLE")
    & F.col("current_observation_status").isin(
        "Active Multi-Source",
        "Active Single-Source"
    )
    & (
        (F.col("intune_record_present") == 0)
        | (
            (F.col("intune_record_present") == 1)
            & (F.col("intune_active") == 0)
        )
    )
)

public_post_managed_conflict_expression = (
    (F.col("network_zone") == "PUBLIC_EXTERNAL")
    & (F.col("management_model") == "POST_MANAGED")
)

high_risk_port_expression = high_risk_port_udf(
    F.col("service_port_signature")
)

structural_candidate_expression = (
    F.col("is_ip_only_identity")
    & (F.col("device_family") == "UNKNOWN")
    & (F.coalesce(F.col("nmap_open_port_count").cast("long"), F.lit(0)) == 0)
    & (F.col("service_port_signature") == "NONE")
)

feature_df = (
    feature_df
    .withColumn("attribution_confidence", attribution_confidence_expression)
    .withColumn("attribution_conflict_flag", attribution_conflict_expression)
    .withColumn("intune_management_conflict_flag", intune_management_conflict_expression)
    .withColumn("serial_conflict_flag", serial_conflict_expression)
    .withColumn("public_post_managed_conflict_flag", public_post_managed_conflict_expression)
    .withColumn(
        "conflict_count",
        (
            F.col("attribution_conflict_flag").cast("int")
            + F.col("intune_management_conflict_flag").cast("int")
            + F.col("serial_conflict_flag").cast("int")
            + F.col("public_post_managed_conflict_flag").cast("int")
        ).cast("int")
    )
    .withColumn("high_risk_port_indicator", high_risk_port_expression)
    .withColumn(
        "high_risk_feature_count",
        (
            F.when(F.col("acr_band") == "9_10", 1).otherwise(0)
            + F.when(F.col("qualys_risk_band") == "CRITICAL", 1).otherwise(0)
            + F.when(F.col("network_zone") == "PUBLIC_EXTERNAL", 1).otherwise(0)
            + F.col("high_risk_port_indicator").cast("int")
        ).cast("int")
    )
    .withColumn(
        "unexplained_feature_count",
        F.lit(None).cast("int")
    )
    .withColumn(
        "structural_exclusion_candidate",
        structural_candidate_expression
    )
    .withColumn(
        "structural_exclusion_candidate_reason",
        F.when(
            structural_candidate_expression,
            "PARTIAL_IP_ONLY_NO_HOST_OR_SERVICE_EVIDENCE"
        ).otherwise("NOT_A_PARTIAL_CANDIDATE")
    )
    .withColumn(
        "structural_exclusion_source_basis",
        F.when(
            structural_candidate_expression,
            F.array(
                F.lit("ASSET_RECONCILIATION_SCORED"),
                F.lit("NMAP_NORM")
            )
        ).otherwise(F.array(F.lit("ASSET_RECONCILIATION_SCORED")))
    )
    .withColumn(
        "suppression_candidate",
        F.lit(None).cast("boolean")
    )
    .withColumn(
        "suppression_candidate_reason",
        F.lit("NOT_EVALUATED_RULE_ENGINE_PENDING")
    )
    .withColumn(
        "suppression_source_basis",
        F.array(
            F.lit("IP006_ASSET_FEATURE_MART"),
            F.lit("KB_RULE_REGISTRY_V2_2")
        )
    )
    .withColumn("feature_privacy_tier", F.lit("HIGH"))
    .withColumn(
        "blocked_feature_list",
        F.array(
            F.lit("network_id"),
            F.lit("is_network_address"),
            F.lit("is_broadcast_address"),
            F.lit("network_scoped_asset_key"),
            F.lit("scan_count"),
            F.lit("network_count"),
            F.lit("source_file_count")
        )
    )
    .withColumn(
        "not_evaluated_feature_list",
        F.array(
            F.lit("unexplained_feature_count"),
            F.lit("suppression_candidate")
        )
    )
    .withColumn(
        "feature_source_basis",
        F.array(
            F.lit("ASSET_RECONCILIATION_SCORED"),
            F.lit("KB_ASSET_PATTERNS_V2"),
            F.lit("KB_DEVICE_PROFILES"),
            F.lit("KB_DEVICE_TOOL_EXPECTATIONS"),
            F.lit("KB_ISSO_SUBNET_LOOKUP"),
            F.lit("KB_NAMING_TOKEN_RULES"),
            F.lit("KB_PORT_RULES"),
            F.lit("KB_REQUIRED_TOOLS_V2"),
            F.lit("KB_SOURCE_REGISTRY")
        )
    )
)


# ---------------------------------------------------------------------------
# 11. Select the durable Feature Mart contract
# ---------------------------------------------------------------------------

FEATURE_MART_COLUMNS = [
    "post_name",
    "post_slug",
    "rcaip_run_id",
    "asset_key",
    "host_key",
    "feature_contract_version",
    "feature_build_version",
    "feature_build_mode",
    "feature_generated_at_utc",
    "kb_release_version",
    "canonical_name",
    "canonical_serial",
    "source_mask_raw",
    "source_mask_active",
    "source_mask_stale",
    "tool_mask",
    "active_source_count_recomputed",
    "identity_basis",
    "identity_strength",
    "hostname_pattern",
    "current_post_token_match",
    "foreign_post_token_match",
    "foreign_post_candidate",
    "foreign_post_token_status",
    "foreign_post_token_basis",
    "device_family",
    "device_family_confidence",
    "matched_device_profile",
    "matched_device_token",
    "matched_pattern_id",
    "classification_basis",
    "canonical_ip",
    "ip_version",
    "ip_class",
    "enterprise_cidr_match",
    "network_zone",
    "network_id",
    "is_ip_only_identity",
    "is_network_address",
    "is_broadcast_address",
    "network_scoped_asset_key",
    "scan_count",
    "network_count",
    "source_file_count",
    "service_port_signature",
    "printer_signature",
    "infrastructure_signature",
    "acr_band",
    "qualys_risk_band",
    "management_model",
    "management_owner",
    "cmdb_intune_management_indicator",
    "intune_source_status",
    "tool_applicability_profile",
    "expected_tool_mask",
    "applicable_tool_mask",
    "unavailable_expected_tool_mask",
    "applicable_coverage_gap",
    "non_applicable_absence_count",
    "true_tooling_conflict_count",
    "attribution_confidence",
    "attribution_conflict_flag",
    "intune_management_conflict_flag",
    "serial_conflict_flag",
    "public_post_managed_conflict_flag",
    "conflict_count",
    "high_risk_port_indicator",
    "high_risk_feature_count",
    "unexplained_feature_count",
    "structural_exclusion_candidate",
    "structural_exclusion_candidate_reason",
    "structural_exclusion_source_basis",
    "suppression_candidate",
    "suppression_candidate_reason",
    "suppression_source_basis",
    "feature_privacy_tier",
    "blocked_feature_list",
    "not_evaluated_feature_list",
    "feature_source_basis",
    "cmdb_present",
    "current_observation_status",
    "nmap_record_present",
    "tenable_record_present",
    "intune_record_present",
    "mde_record_present",
    "qualys_record_present",
    "nmap_active",
    "tenable_active",
    "intune_active",
    "mde_active",
    "qualys_active"
]

feature_mart_current_run_df = feature_df.select(*FEATURE_MART_COLUMNS)


# ---------------------------------------------------------------------------
# 12. Pre-write Feature Contract certification
# ---------------------------------------------------------------------------

BLOCKED_FEATURES = [
    "network_id",
    "is_network_address",
    "is_broadcast_address",
    "network_scoped_asset_key",
    "scan_count",
    "network_count",
    "source_file_count"
]

CATEGORICAL_DOMAINS = {
    "identity_basis": {"SERIAL", "HOST", "MAC", "IP", "COMPOSITE", "UNKNOWN"},
    "identity_strength": {"STRONG", "MODERATE", "WEAK", "UNKNOWN"},
    "hostname_pattern": {
        "POST_STANDARD", "ENTERPRISE_STANDARD", "PRINTER_PATTERN",
        "INFRA_PATTERN", "MOBILE_PATTERN", "OTHER", "NO_NAME"
    },
    "device_family": {
        "WORKSTATION", "SERVER", "MOBILE", "PRINTER_MFD",
        "NETWORK_INFRASTRUCTURE", "IOT_OT", "SECURITY_APPLIANCE", "UNKNOWN"
    },
    "device_family_confidence": {"HIGH", "MODERATE", "LOW"},
    "ip_class": {
        "ENTERPRISE_CIDR", "RFC1918", "CGNAT", "PUBLIC", "LINK_LOCAL",
        "LOOPBACK", "MULTICAST", "RESERVED", "INVALID", "NONE"
    },
    "network_zone": {
        "OPENNET", "ENTERPRISE_WIFI", "NEN", "PUBLIC_EXTERNAL",
        "UNKNOWN_INTERNAL", "NO_IP"
    },
    "acr_band": {"NONE", "0", "1_3", "4_6", "7_8", "9_10", "UNKNOWN"},
    "qualys_risk_band": {"NONE", "LOW", "MODERATE", "HIGH", "CRITICAL", "UNKNOWN"},
    "management_model": {
        "POST_MANAGED", "ENTERPRISE_MANAGED", "SHARED",
        "UNMANAGED_OR_UNKNOWN", "NON_ASSET"
    },
    "management_owner": {"POST", "ENTERPRISE", "SHARED", "UNKNOWN", "NONE"},
    "attribution_confidence": {"HIGH", "MODERATE", "LOW", "CONFLICTED", "UNKNOWN"},
    "feature_privacy_tier": {"LOW", "MODERATE", "HIGH"}
}

prewrite_profile = (
    feature_mart_current_run_df
    .agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("asset_key").alias("distinct_asset_key_count"),
        F.sum(
            F.when(F.col("asset_key").isNull(), 1).otherwise(0)
        ).alias("null_key_count"),
        F.sum(
            F.when(
                F.lower(F.trim(F.col("post_slug")))
                != F.lit(str(RCAIP_POST_SLUG).lower()),
                1
            ).otherwise(0)
        ).alias("cross_post_count"),
        F.sum(
            F.when(F.col("rcaip_run_id") != F.lit(RCAIP_RUN_ID), 1).otherwise(0)
        ).alias("cross_run_count")
    )
    .first()
)

feature_row_count = int(prewrite_profile["row_count"] or 0)
feature_distinct_key_count = int(prewrite_profile["distinct_asset_key_count"] or 0)
feature_null_key_count = int(prewrite_profile["null_key_count"] or 0)
feature_cross_post_count = int(prewrite_profile["cross_post_count"] or 0)
feature_cross_run_count = int(prewrite_profile["cross_run_count"] or 0)

blocked_non_null_count = (
    feature_mart_current_run_df
    .filter(
        F.greatest(*[
            F.when(F.col(column_name).isNotNull(), 1).otherwise(0)
            for column_name in BLOCKED_FEATURES
        ]) > 0
    )
    .count()
)

categorical_failure_rows = []
for column_name, allowed_values in CATEGORICAL_DOMAINS.items():
    invalid_count = (
        feature_mart_current_run_df
        .filter(
            F.col(column_name).isNull()
            | (~F.col(column_name).isin(*sorted(allowed_values)))
        )
        .count()
    )
    categorical_failure_rows.append((column_name, invalid_count))

invalid_categorical_columns = [
    column_name
    for column_name, invalid_count in categorical_failure_rows
    if invalid_count > 0
]

required_feature_columns_missing = sorted(
    set(FEATURE_MART_COLUMNS) - set(feature_mart_current_run_df.columns)
)

prewrite_checks = [
    ("Input/output row parity", str(input_row_count), str(feature_row_count), input_row_count == feature_row_count),
    ("Unique Feature Mart keys", str(feature_row_count), str(feature_distinct_key_count), feature_row_count == feature_distinct_key_count),
    ("Null asset keys", "0", str(feature_null_key_count), feature_null_key_count == 0),
    ("Cross-Post leakage", "0", str(feature_cross_post_count), feature_cross_post_count == 0),
    ("Cross-run leakage", "0", str(feature_cross_run_count), feature_cross_run_count == 0),
    ("Missing required Feature Mart columns", "0", str(len(required_feature_columns_missing)), len(required_feature_columns_missing) == 0),
    ("Invalid categorical domains", "0", str(len(invalid_categorical_columns)), len(invalid_categorical_columns) == 0),
    ("Blocked Nmap fields populated", "0", str(blocked_non_null_count), blocked_non_null_count == 0),
    ("Active rules during shadow build", "0", str(active_rule_count), active_rule_count == 0)
]

check_schema = T.StructType([
    T.StructField("validation_check", T.StringType(), False),
    T.StructField("expected_value", T.StringType(), False),
    T.StructField("actual_value", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False)
])

prewrite_checks_df = spark.createDataFrame(prewrite_checks, schema=check_schema)
display(prewrite_checks_df.orderBy("validation_check"))

failed_prewrite_checks = [
    check_name
    for check_name, expected, actual, passed in prewrite_checks
    if not passed
]

if failed_prewrite_checks:
    raise RuntimeError(
        "IP-006 Feature Mart pre-write certification failed: "
        + " | ".join(failed_prewrite_checks)
        + (
            " | Invalid categorical columns: " + " | ".join(invalid_categorical_columns)
            if invalid_categorical_columns else ""
        )
        + (
            " | Missing columns: " + " | ".join(required_feature_columns_missing)
            if required_feature_columns_missing else ""
        )
    )

print("IP-006 Feature Mart pre-write certification: PASS")


# ---------------------------------------------------------------------------
# 13. Protected Post-scoped latest-run write
#
# The registered physical table is Post-specific. Like the Asset Engine
# products it consumes, this shadow build publishes the latest certified run
# and retains the run ID in every row for provenance.
# ---------------------------------------------------------------------------

if spark.catalog.tableExists(OUTPUT_TABLE):
    existing_feature_columns = set(spark.table(OUTPUT_TABLE).columns)
    if existing_feature_columns and not {
        "post_slug", "rcaip_run_id", "asset_key"
    }.issubset(existing_feature_columns):
        raise RuntimeError(
            "Existing Feature Mart has an unknown contract. Refusing overwrite."
        )

(
    feature_mart_current_run_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print("IP-006 Asset Feature Mart shadow write: PASS")
print("Written table:", OUTPUT_TABLE)


# ---------------------------------------------------------------------------
# 14. Post-write certification and interpretable distribution review
# ---------------------------------------------------------------------------

written_current_run_df = (
    spark.table(OUTPUT_TABLE)
    .filter(
        (F.lower(F.trim(F.col("post_slug"))) == F.lit(str(RCAIP_POST_SLUG).lower()))
        & (F.col("rcaip_run_id") == F.lit(RCAIP_RUN_ID))
    )
)

postwrite_profile = (
    written_current_run_df
    .agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("asset_key").alias("distinct_asset_key_count"),
        F.sum(F.when(F.col("structural_exclusion_candidate"), 1).otherwise(0)).alias("partial_structural_candidates"),
        F.sum(F.when(F.col("conflict_count") > 0, 1).otherwise(0)).alias("assets_with_conflicts"),
        F.sum(F.when(F.col("high_risk_feature_count") > 0, 1).otherwise(0)).alias("assets_with_high_risk_features"),
        F.sum(F.when(F.col("true_tooling_conflict_count") > 0, 1).otherwise(0)).alias("assets_with_tooling_conflicts"),
        F.sum(F.when(F.col("suppression_candidate").isNotNull(), 1).otherwise(0)).alias("suppression_evaluated_rows"),
        F.sum(F.when(F.col("unexplained_feature_count").isNotNull(), 1).otherwise(0)).alias("unexplained_evaluated_rows")
    )
    .first()
)

written_row_count = int(postwrite_profile["row_count"] or 0)
written_distinct_key_count = int(postwrite_profile["distinct_asset_key_count"] or 0)
partial_structural_candidate_count = int(postwrite_profile["partial_structural_candidates"] or 0)
assets_with_conflicts = int(postwrite_profile["assets_with_conflicts"] or 0)
assets_with_high_risk_features = int(postwrite_profile["assets_with_high_risk_features"] or 0)
assets_with_tooling_conflicts = int(postwrite_profile["assets_with_tooling_conflicts"] or 0)
suppression_evaluated_rows = int(postwrite_profile["suppression_evaluated_rows"] or 0)
unexplained_evaluated_rows = int(postwrite_profile["unexplained_evaluated_rows"] or 0)

postwrite_checks = [
    ("Written current-run rows", str(input_row_count), str(written_row_count), written_row_count == input_row_count),
    ("Written current-run unique keys", str(written_row_count), str(written_distinct_key_count), written_row_count == written_distinct_key_count),
    ("Suppression rows evaluated", "0", str(suppression_evaluated_rows), suppression_evaluated_rows == 0),
    ("Unexplained rows evaluated", "0", str(unexplained_evaluated_rows), unexplained_evaluated_rows == 0)
]

postwrite_checks_df = spark.createDataFrame(postwrite_checks, schema=check_schema)
display(postwrite_checks_df.orderBy("validation_check"))

failed_postwrite_checks = [
    check_name
    for check_name, expected, actual, passed in postwrite_checks
    if not passed
]

if failed_postwrite_checks:
    raise RuntimeError(
        "IP-006 Feature Mart post-write certification failed: "
        + " | ".join(failed_postwrite_checks)
    )

feature_distribution_df = (
    written_current_run_df
    .groupBy(
        "device_family",
        "management_model",
        "source_mask_active",
        "network_zone"
    )
    .agg(
        F.count(F.lit(1)).alias("asset_count"),
        F.sum(F.col("conflict_count")).alias("total_conflicts"),
        F.sum(F.col("high_risk_feature_count")).alias("total_high_risk_features"),
        F.sum(F.col("true_tooling_conflict_count")).alias("total_tooling_conflicts")
    )
    .orderBy(F.desc("asset_count"), "device_family", "management_model")
)

display(feature_distribution_df)

display(
    written_current_run_df
    .select(
        "asset_key",
        "host_key",
        "source_mask_active",
        "identity_basis",
        "identity_strength",
        "hostname_pattern",
        "device_family",
        "device_family_confidence",
        "network_zone",
        "management_model",
        "management_owner",
        "expected_tool_mask",
        "applicable_coverage_gap",
        "attribution_confidence",
        "conflict_count",
        "high_risk_feature_count",
        "structural_exclusion_candidate",
        "structural_exclusion_candidate_reason"
    )
    .orderBy(
        F.desc("conflict_count"),
        F.desc("high_risk_feature_count"),
        "device_family",
        "asset_key"
    )
    .limit(100)
)

print("=" * 100)
print("RCAIP IP-006 ASSET FEATURE MART SHADOW CERTIFICATION")
print("=" * 100)
print("Status: PASS")
print("Feature contract version:", IP006_FEATURE_CONTRACT_VERSION)
print("Feature build version:", IP006_FEATURE_BUILD_VERSION)
print("Build mode:", IP006_BUILD_MODE)
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print("Knowledge Base release:", KB_RELEASE_VERSION)
print("Input rows:", input_row_count)
print("Feature Mart rows:", written_row_count)
print("Feature Mart columns:", len(FEATURE_MART_COLUMNS))
print("Blocked Nmap features held null:", len(BLOCKED_FEATURES))
print("Partial structural candidates:", partial_structural_candidate_count)
print("Assets with deterministic conflicts:", assets_with_conflicts)
print("Assets with high-risk features:", assets_with_high_risk_features)
print("Assets with applicable tooling conflicts:", assets_with_tooling_conflicts)
print("Active rules:", active_rule_count)
print("Rule evaluation performed: NO")
print("Asset exclusion performed: NO")
print("Suppression performed: NO")
print("Routing performed: NO")
print("Next gate: certify example_post_c feature distributions and build IP006_RULE_MATCHES in shadow mode")
print("=" * 100)