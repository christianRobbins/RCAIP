# Databricks notebook source
# ============================================================================
# CELL 1 - FIELD INTAKE AND EVIDENCE READINESS
# ============================================================================

from datetime import datetime
import hashlib
import html
import ipaddress
import json
import re
import shlex

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_text_widget(name, default_value, label):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value, label)


def normalize_profile_key(value):
    value = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    return re.sub(r"_+", "_", value).strip("_")


def normalize_identity(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def ordered_unique(values, lowercase=False, uppercase=False):
    result = []
    for value in values:
        text = str(value or "").strip()
        if lowercase:
            text = text.lower()
        if uppercase:
            text = text.upper()
        if text and text not in result:
            result.append(text)
    return result


def split_multi_value(value):
    return ordered_unique(re.split(r"[,;\n|]+", str(value or "")))


def valid_cidr(value):
    try:
        return str(ipaddress.ip_network(str(value).strip(), strict=False))
    except Exception:
        return None


def parse_cidr_text(value):
    raw_values = split_multi_value(value)
    valid_values = []
    invalid_values = []
    for raw_value in raw_values:
        normalized = valid_cidr(raw_value)
        if normalized:
            if normalized not in valid_values:
                valid_values.append(normalized)
        elif raw_value:
            invalid_values.append(raw_value)
    return valid_values, invalid_values


def valid_iso_date(value):
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def table_exists_safe(table_name):
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def normalized_column_map(dataframe):
    return {
        re.sub(r"[^a-z0-9]+", "", str(column).lower()): column
        for column in dataframe.columns
    }


def first_existing_column(dataframe, candidates):
    column_map = normalized_column_map(dataframe)
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]+", "", str(candidate).lower())
        if key in column_map:
            return column_map[key]
    return None


def all_existing_columns(dataframe, candidates):
    result = []
    for candidate in candidates:
        column = first_existing_column(dataframe, [candidate])
        if column and column not in result:
            result.append(column)
    return result


def list_files_recursive(path):
    discovered = []
    for item in dbutils.fs.ls(path):
        if item.path.endswith("/"):
            discovered.extend(list_files_recursive(item.path))
        else:
            discovered.append(item)
    return discovered


def classify_input_file(file_name):
    lower_name = str(file_name or "").strip().lower()
    if lower_name.endswith(".xml"):
        return "NMAP_XML", "MANDATORY"
    if lower_name.endswith(".csv") and "cmdb" in lower_name:
        return "CMDB_CSV", "MANDATORY"
    if lower_name.endswith(".csv") and any(x in lower_name for x in ["qualys", "easm"]):
        return "QUALYS_CSV", "OPTIONAL"
    if lower_name.endswith(".csv"):
        return "OTHER_CSV", "REVIEW"
    if lower_name.endswith((".xlsx", ".xls")):
        return "SPREADSHEET", "REVIEW"
    if lower_name.endswith((".txt", ".md", ".json")):
        return "NOTES_OR_MANIFEST", "OPTIONAL"
    return "OTHER", "REVIEW"


def extract_nmap_cli_targets(file_path, max_bytes=262144):
    """Recover source-native target CIDRs from nmaprun@args without parsing hosts."""
    try:
        xml_header = dbutils.fs.head(file_path, max_bytes)
    except Exception as exc:
        return [], None, "XML_HEADER_READ_ERROR: " + str(exc)

    args_match = re.search(
        r'<nmaprun\b[^>]*\bargs="([^"]*)"',
        str(xml_header),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if args_match is None:
        return [], None, "NMAP_ARGS_NOT_FOUND"

    nmap_args = html.unescape(args_match.group(1)).strip()
    try:
        tokens = shlex.split(nmap_args)
    except Exception:
        tokens = nmap_args.split()

    options_with_values = {
        "-oA", "-oX", "-oN", "-oG", "-iL", "--excludefile", "-e", "-S",
        "-g", "--source-port", "-p", "--top-ports", "--min-rate", "--max-rate",
        "--min-hostgroup", "--max-hostgroup", "--min-parallelism",
        "--max-parallelism", "--max-retries", "--host-timeout", "--scan-delay",
        "--max-scan-delay", "--script", "--script-args",
    }

    targets = []
    skip_next = False
    for token in tokens:
        token = str(token).strip()
        if skip_next:
            skip_next = False
            continue
        if token in options_with_values:
            skip_next = True
            continue
        if token.startswith("-"):
            continue
        try:
            network = str(ipaddress.ip_network(token, strict=False))
        except Exception:
            continue
        if network not in targets:
            targets.append(network)

    return targets, nmap_args, "NMAP_ARGS_PARSED"


def infer_file_scope(file_name):
    lower_name = str(file_name).lower()
    if any(token in lower_name for token in ["entwifi", "enterprise_wifi", "wifi"]):
        return "Enterprise WiFi", "EntWifi", "Enterprise WiFi scan"
    if "nen" in lower_name:
        return "NEN", "NEN Public", "NEN public-target scan"
    return "Enterprise", "OpenNet", "OpenNet scan"


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_json(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Operator widgets - all user input lives in this cell
# ---------------------------------------------------------------------------

ensure_text_widget("rcaip_intake_post_name", "", "Official Post Name")
ensure_text_widget("rcaip_intake_post_token", "", "Post Token / Abbreviation")
ensure_text_widget("rcaip_intake_post_profile", "", "Post Profile Key - Optional; derived if blank")
ensure_text_widget("rcaip_intake_country_name", "", "Country Name - Optional if lookup resolves it")
ensure_text_widget("rcaip_intake_assessment_start_date", "", "Assessment Start Date - YYYY-MM-DD")
ensure_text_widget("rcaip_intake_assessment_end_date", "", "Assessment End Date - YYYY-MM-DD")
ensure_text_widget("rcaip_intake_results_namespace", "devresults", "Results Namespace")
ensure_text_widget("rcaip_intake_opennet_cidr", "", "OpenNet CIDR(s) - Optional if XML contains targets")
ensure_text_widget("rcaip_intake_nen_public_targets", "", "NEN Public Targets - Optional")
ensure_text_widget("rcaip_intake_entwifi_public_targets", "", "Enterprise WiFi Public Targets - Optional")
ensure_text_widget("rcaip_intake_entwifi_internal_cidrs", "", "Enterprise WiFi Internal CIDRs - Optional")
ensure_text_widget(
    "rcaip_intake_umbrella_edge_deployed",
    "UNKNOWN",
    "Umbrella on NEN Edge? YES / NO / UNKNOWN / NOT_APPLICABLE",
)
ensure_text_widget(
    "rcaip_intake_umbrella_edge_config_reviewed",
    "NOT_CHECKED",
    "Umbrella Edge Config Reviewed? YES / NO / NOT_CHECKED / NOT_APPLICABLE",
)


# ---------------------------------------------------------------------------
# Read and validate operator input
# ---------------------------------------------------------------------------

manual_post_name = dbutils.widgets.get("rcaip_intake_post_name").strip()
manual_post_token = dbutils.widgets.get("rcaip_intake_post_token").strip().upper()
manual_profile_key = dbutils.widgets.get("rcaip_intake_post_profile").strip()
manual_country_name = dbutils.widgets.get("rcaip_intake_country_name").strip()
assessment_start_text = dbutils.widgets.get("rcaip_intake_assessment_start_date").strip()
assessment_end_text = dbutils.widgets.get("rcaip_intake_assessment_end_date").strip()
results_namespace = dbutils.widgets.get("rcaip_intake_results_namespace").strip()

if not manual_post_name:
    raise ValueError("Official Post Name cannot be blank.")
if not manual_post_token:
    raise ValueError("Post Token / Abbreviation cannot be blank.")
if not re.fullmatch(r"[A-Za-z0-9_.]+", results_namespace):
    raise ValueError("Results Namespace contains unsupported characters.")

post_profile_key = normalize_profile_key(manual_profile_key or manual_post_name)
if not post_profile_key:
    raise ValueError("Post Profile Key could not be derived.")

manual_opennet_cidrs, invalid_opennet = parse_cidr_text(
    dbutils.widgets.get("rcaip_intake_opennet_cidr")
)
manual_nen_targets, invalid_nen = parse_cidr_text(
    dbutils.widgets.get("rcaip_intake_nen_public_targets")
)
manual_entwifi_public_targets, invalid_entwifi_public = parse_cidr_text(
    dbutils.widgets.get("rcaip_intake_entwifi_public_targets")
)
manual_entwifi_internal_cidrs, invalid_entwifi_internal = parse_cidr_text(
    dbutils.widgets.get("rcaip_intake_entwifi_internal_cidrs")
)

invalid_cidr_values = invalid_opennet + invalid_nen + invalid_entwifi_public + invalid_entwifi_internal
if invalid_cidr_values:
    raise ValueError("Invalid CIDR value(s): " + " | ".join(invalid_cidr_values))

if not valid_iso_date(assessment_start_text) or not valid_iso_date(assessment_end_text):
    raise ValueError("Assessment dates are required and must use YYYY-MM-DD.")

assessment_start_date = datetime.strptime(assessment_start_text, "%Y-%m-%d").date()
assessment_end_date = datetime.strptime(assessment_end_text, "%Y-%m-%d").date()
if assessment_end_date < assessment_start_date:
    raise ValueError("Assessment End Date cannot precede Assessment Start Date.")

umbrella_edge_deployed = dbutils.widgets.get("rcaip_intake_umbrella_edge_deployed").strip().upper()
umbrella_edge_reviewed = dbutils.widgets.get("rcaip_intake_umbrella_edge_config_reviewed").strip().upper()

allowed_edge_states = {"YES", "NO", "UNKNOWN", "NOT_APPLICABLE"}
allowed_review_states = {"YES", "NO", "NOT_CHECKED", "NOT_APPLICABLE"}
if umbrella_edge_deployed not in allowed_edge_states:
    raise ValueError("Invalid Umbrella edge deployment state.")
if umbrella_edge_reviewed not in allowed_review_states:
    raise ValueError("Invalid Umbrella edge review state.")
if umbrella_edge_deployed in {"NO", "NOT_APPLICABLE"} and umbrella_edge_reviewed == "YES":
    raise ValueError("Umbrella edge review cannot be YES when edge deployment is NO or NOT_APPLICABLE.")


# ---------------------------------------------------------------------------
# Enterprise lookup - corroboration and fallback only
# ---------------------------------------------------------------------------

lookup_candidates = [
    f"{results_namespace}.rcaip_kb_isso_subnet_lookup",
    "example_sources.binding_a97541d9",
]
selected_lookup_table = next((x for x in lookup_candidates if table_exists_safe(x)), None)
lookup_status = "LOOKUP_NOT_AVAILABLE"
lookup_match_count = 0
lookup_post_names = []
lookup_tokens = []
lookup_countries = []
lookup_cidrs = []

if selected_lookup_table:
    lookup_df = spark.table(selected_lookup_table)
    identity_columns = all_existing_columns(
        lookup_df,
        [
            "post_name", "post", "site_name", "sitename", "location_name", "mission_name",
            "embassy_name", "city", "site", "location", "post_slug", "post_code",
            "post_abbreviation", "post_abbrev", "site_code", "location_code", "post_token",
            "short_name", "acronym", "post_prefix", "device_prefix", "naming_prefix",
        ],
    )
    post_name_column = first_existing_column(
        lookup_df,
        ["post_name", "post", "site_name", "sitename", "location_name", "mission_name", "embassy_name", "city", "site", "location"],
    )
    token_column = first_existing_column(
        lookup_df,
        ["post_code", "post_abbreviation", "post_abbrev", "site_code", "location_code", "post_token", "short_name", "acronym", "post_prefix", "device_prefix", "naming_prefix"],
    )
    country_column = first_existing_column(
        lookup_df,
        ["country_name", "country", "host_country", "nation", "countryname", "country_code_description"],
    )
    cidr_columns = all_existing_columns(
        lookup_df,
        ["cidr", "subnet_cidr", "network_cidr", "opennet_cidr", "enterprise_cidr", "ip_cidr", "subnet", "network", "ip_range", "subnet_mapping", "subnetmapping", "subnets", "network_address", "ip_address", "ipaddress", "ipaddressrange", "prefix"],
    )

    match_values = ordered_unique(
        [normalize_identity(post_profile_key), normalize_identity(manual_post_name), normalize_identity(manual_post_token)]
    )
    identity_expression = None
    for column_name in identity_columns:
        normalized_column = F.regexp_replace(
            F.lower(F.trim(F.col(column_name).cast("string"))),
            r"[^a-z0-9]+",
            "",
        )
        current_match = normalized_column.isin(match_values)
        identity_expression = current_match if identity_expression is None else identity_expression | current_match

    if identity_expression is None:
        lookup_status = "LOOKUP_SCHEMA_REVIEW_REQUIRED"
    else:
        lookup_rows = lookup_df.filter(identity_expression).limit(250).collect()
        lookup_match_count = len(lookup_rows)
        if lookup_rows:
            if post_name_column:
                lookup_post_names = ordered_unique([row[post_name_column] for row in lookup_rows])
            if token_column:
                lookup_tokens = ordered_unique([row[token_column] for row in lookup_rows], uppercase=True)
            if country_column:
                lookup_countries = ordered_unique([row[country_column] for row in lookup_rows])
            lookup_cidr_values = []
            for row in lookup_rows:
                for cidr_column in cidr_columns:
                    lookup_cidr_values.append(row[cidr_column])
            lookup_cidrs = []
            for value in lookup_cidr_values:
                parsed, _ = parse_cidr_text(value)
                for cidr in parsed:
                    if cidr not in lookup_cidrs:
                        lookup_cidrs.append(cidr)
            lookup_status = "MATCHED"
        else:
            lookup_status = "NO_EXACT_POST_MATCH"

post_name_conflict = bool(lookup_post_names) and normalize_identity(manual_post_name) not in {
    normalize_identity(value) for value in lookup_post_names
}
post_token_conflict = bool(lookup_tokens) and manual_post_token not in {str(value).upper() for value in lookup_tokens}

if manual_country_name:
    country_name = manual_country_name
    country_source = "RCSO_INPUT"
    country_conflict = bool(lookup_countries) and normalize_identity(manual_country_name) not in {
        normalize_identity(value) for value in lookup_countries
    }
elif len(lookup_countries) == 1:
    country_name = lookup_countries[0]
    country_source = "ENTERPRISE_LOOKUP_FALLBACK"
    country_conflict = False
else:
    country_name = ""
    country_source = "NOT_AVAILABLE" if not lookup_countries else "MULTIPLE_LOOKUP_VALUES"
    country_conflict = bool(lookup_countries)


# ---------------------------------------------------------------------------
# File inventory and source-native Nmap scope
# ---------------------------------------------------------------------------

input_base_dir = f"dbfs:/FileStore/tables/rcso_csa_tools/inputs/{post_profile_key}/"
dbutils.fs.mkdirs(input_base_dir)
file_info_records = list_files_recursive(input_base_dir)

inventory_records = []
for item in file_info_records:
    evidence_type, requirement = classify_input_file(item.name)
    inventory_records.append(
        {
            "file_name": item.name,
            "file_path": item.path,
            "size_bytes": int(item.size),
            "evidence_type": evidence_type,
            "requirement": requirement,
        }
    )

cmdb_records = [x for x in inventory_records if x["evidence_type"] == "CMDB_CSV"]
nmap_records = [x for x in inventory_records if x["evidence_type"] == "NMAP_XML"]
qualys_records = [x for x in inventory_records if x["evidence_type"] == "QUALYS_CSV"]
umbrella_files = sorted(
    [x["file_name"] for x in inventory_records if "umbrella" in x["file_name"].lower()],
    key=str.lower,
)

nmap_file_names = [x["file_name"] for x in nmap_records]
if len(nmap_file_names) != len(set(nmap_file_names)):
    raise RuntimeError("Duplicate Nmap XML basenames exist in the input directory. Filenames must be unique for durable evidence identity.")

manifest_records = []
xml_opennet_cidrs = []
for file_record in sorted(nmap_records, key=lambda x: x["file_name"].lower()):
    asset_domain, scan_scope, notes = infer_file_scope(file_record["file_name"])
    xml_targets, nmap_args, args_status = extract_nmap_cli_targets(file_record["file_path"])

    if xml_targets:
        target_cidrs = list(xml_targets)
        target_source = "NMAP_XML_COMMAND_LINE"
    elif scan_scope == "OpenNet" and manual_opennet_cidrs:
        target_cidrs = list(manual_opennet_cidrs)
        target_source = "RCSO_OPENNET_CONTEXT"
    elif scan_scope == "NEN Public" and manual_nen_targets:
        target_cidrs = list(manual_nen_targets)
        target_source = "RCSO_NEN_CONTEXT"
    elif scan_scope == "EntWifi" and (manual_entwifi_internal_cidrs or manual_entwifi_public_targets):
        target_cidrs = ordered_unique(manual_entwifi_internal_cidrs + manual_entwifi_public_targets)
        target_source = "RCSO_ENTERPRISE_WIFI_CONTEXT"
    else:
        target_cidrs = []
        target_source = "NOT_AVAILABLE"

    if scan_scope == "OpenNet":
        for cidr in target_cidrs:
            if cidr not in xml_opennet_cidrs and target_source == "NMAP_XML_COMMAND_LINE":
                xml_opennet_cidrs.append(cidr)

    target_count = len(target_cidrs)
    target_cidr = target_cidrs[0] if target_count == 1 else "MULTI_TARGET" if target_count > 1 else "UNKNOWN"
    scope_status = "READY_SINGLE_TARGET" if target_count == 1 else "MULTI_TARGET_PRESERVED" if target_count > 1 else "TARGET_REQUIRED"

    manifest_records.append(
        {
            "file": file_record["file_name"],
            "source_file_path": file_record["file_path"],
            "asset_domain": asset_domain,
            "scan_scope": scan_scope,
            "scan_location": manual_post_name,
            "target_cidr": target_cidr,
            "target_cidrs": list(target_cidrs),
            "target_cidr_count": target_count,
            "target_source": target_source,
            "notes": notes,
            "xml_args_status": args_status,
            "nmap_args": nmap_args,
            "scope_status": scope_status,
        }
    )

if xml_opennet_cidrs:
    opennet_cidrs = list(xml_opennet_cidrs)
    opennet_cidr_source = "NMAP_XML_COMMAND_LINE"
elif manual_opennet_cidrs:
    opennet_cidrs = list(manual_opennet_cidrs)
    opennet_cidr_source = "RCSO_INPUT"
elif lookup_cidrs:
    opennet_cidrs = list(lookup_cidrs)
    opennet_cidr_source = "ENTERPRISE_LOOKUP_FALLBACK"
else:
    opennet_cidrs = []
    opennet_cidr_source = "NOT_AVAILABLE"

manual_xml_scope_conflict = bool(manual_opennet_cidrs and xml_opennet_cidrs) and set(manual_opennet_cidrs) != set(xml_opennet_cidrs)
missing_target_files = [x["file"] for x in manifest_records if x["target_cidr_count"] == 0]

cmdb_file = cmdb_records[0]["file_name"] if len(cmdb_records) == 1 else None
qualys_file = qualys_records[0]["file_name"] if len(qualys_records) == 1 else None

optional_evidence_status = {
    "QUALYS": "PROVIDED" if len(qualys_records) == 1 else "MULTIPLE_REVIEW_REQUIRED" if len(qualys_records) > 1 else "NOT_PROVIDED",
    "NEN_NMAP": "PROVIDED" if any(x["scan_scope"] == "NEN Public" for x in manifest_records) else "NOT_PROVIDED",
    "ENTERPRISE_WIFI_NMAP": "PROVIDED" if any(x["scan_scope"] == "EntWifi" for x in manifest_records) else "NOT_PROVIDED",
    "ENTERPRISE_WIFI_INTERNAL_CIDRS": "PROVIDED" if manual_entwifi_internal_cidrs else "NOT_PROVIDED",
    "UMBRELLA_ENDPOINT": "PROVIDED_REQUIRES_FORMAT_VALIDATION" if umbrella_files else "NOT_PROVIDED",
    "UMBRELLA_EDGE_DEPLOYMENT": umbrella_edge_deployed,
    "UMBRELLA_EDGE_CONFIG_REVIEW": umbrella_edge_reviewed,
}


# ---------------------------------------------------------------------------
# Final readiness - only consequential conditions gate the operator
# ---------------------------------------------------------------------------

readiness_checks = [
    ("Post identity", bool(manual_post_name and manual_post_token and post_profile_key), f"{manual_post_name} | {manual_post_token} | {post_profile_key}"),
    ("Country resolved", bool(country_name) and not country_conflict, f"{country_name or 'NOT AVAILABLE'} | {country_source}"),
    ("Assessment dates", assessment_end_date >= assessment_start_date, f"{assessment_start_text} to {assessment_end_text}"),
    ("Exactly one CMDB export", len(cmdb_records) == 1, str(len(cmdb_records))),
    ("At least one Nmap XML", len(nmap_records) >= 1, str(len(nmap_records))),
    ("Every Nmap XML has target scope", len(missing_target_files) == 0, " | ".join(missing_target_files) or "ALL IDENTIFIED"),
    ("OpenNet scope available", len(opennet_cidrs) >= 1, " | ".join(opennet_cidrs) or "NOT AVAILABLE"),
    ("Post name corroboration", not post_name_conflict, "CONFLICT" if post_name_conflict else "PASS"),
    ("Post token corroboration", not post_token_conflict, "CONFLICT" if post_token_conflict else "PASS"),
    ("Manual/XML OpenNet scope", not manual_xml_scope_conflict, "CONFLICT" if manual_xml_scope_conflict else "PASS"),
]

intake_ready = all(check[1] for check in readiness_checks)

profile_tokens = ordered_unique(
    [post_profile_key, manual_post_name, manual_post_token] + lookup_post_names + lookup_tokens,
    lowercase=True,
)

RCAIP_RUN000_INTAKE_CONTRACT = {
    "post_profile_key": post_profile_key,
    "post_name": manual_post_name,
    "post_token": manual_post_token,
    "country_name": country_name,
    "country_source": country_source,
    "assessment_start_date": assessment_start_text,
    "assessment_end_date": assessment_end_text,
    "results_namespace": results_namespace,
    "profile_tokens": profile_tokens,
    "opennet_cidrs": opennet_cidrs,
    "opennet_cidr_source": opennet_cidr_source,
    "nen_public_targets": manual_nen_targets,
    "entwifi_public_targets": manual_entwifi_public_targets,
    "entwifi_internal_cidrs": manual_entwifi_internal_cidrs,
    "input_base_dir": input_base_dir,
    "cmdb_file": cmdb_file,
    "qualys_file": qualys_file,
    "lookup_table": selected_lookup_table,
    "lookup_status": lookup_status,
    "lookup_match_count": lookup_match_count,
    "umbrella_edge_deployed": umbrella_edge_deployed,
    "umbrella_edge_config_reviewed": umbrella_edge_reviewed,
    "umbrella_endpoint_files": umbrella_files,
    "optional_evidence_status": optional_evidence_status,
    "nmap_manifest": manifest_records,
}
RCAIP_RUN000_INTAKE_SHA256 = sha256_json(RCAIP_RUN000_INTAKE_CONTRACT)
RCAIP_RUN000_INTAKE_READY = intake_ready

# Compatibility aliases retained for existing documentation and temporary use.
RCAIP_INTAKE_POST_PROFILE_KEY = post_profile_key
RCAIP_INTAKE_POST_NAME = manual_post_name
RCAIP_INTAKE_POST_TOKEN = manual_post_token
RCAIP_INTAKE_COUNTRY_NAME = country_name
RCAIP_INTAKE_COUNTRY_SOURCE = country_source
RCAIP_INTAKE_DEVICE_NAME_TOKENS = profile_tokens
RCAIP_INTAKE_OPENNET_CIDRS = list(opennet_cidrs)
RCAIP_INTAKE_NEN_PUBLIC_TARGETS = list(manual_nen_targets)
RCAIP_INTAKE_ENTWIFI_PUBLIC_TARGETS = list(manual_entwifi_public_targets)
RCAIP_INTAKE_ENTWIFI_INTERNAL_CIDRS = list(manual_entwifi_internal_cidrs)
RCAIP_INTAKE_INPUT_BASE_DIR = input_base_dir
RCAIP_INTAKE_CMDB_FILE = cmdb_file
RCAIP_INTAKE_QUALYS_FILE = qualys_file
RCAIP_INTAKE_NMAP_FILES = [x["file"] for x in manifest_records]
RCAIP_INTAKE_LOOKUP_TABLE = selected_lookup_table
RCAIP_INTAKE_LOOKUP_STATUS = lookup_status
RCAIP_INTAKE_UMBRELLA_EDGE_DEPLOYED = umbrella_edge_deployed
RCAIP_INTAKE_UMBRELLA_EDGE_CONFIG_REVIEWED = umbrella_edge_reviewed
RCAIP_INTAKE_UMBRELLA_ENDPOINT_FILES = list(umbrella_files)
RCAIP_INTAKE_OPTIONAL_EVIDENCE_STATUS = dict(optional_evidence_status)
RCAIP_INTAKE_READY_FOR_PROFILE_DRAFT = bool(manual_post_name and manual_post_token and country_name)
RCAIP_INTAKE_FIELD_EVIDENCE_READY = bool(cmdb_file and manifest_records and opennet_cidrs)
RCAIP_INTAKE_READY_FOR_ASSESSMENT_RUN = intake_ready

readiness_schema = T.StructType([
    T.StructField("readiness_check", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False),
    T.StructField("observed_value", T.StringType(), False),
])
rcaip_run000_readiness_df = spark.createDataFrame(readiness_checks, schema=readiness_schema)

inventory_schema = T.StructType([
    T.StructField("file_name", T.StringType(), False),
    T.StructField("file_path", T.StringType(), False),
    T.StructField("size_bytes", T.LongType(), False),
    T.StructField("evidence_type", T.StringType(), False),
    T.StructField("requirement", T.StringType(), False),
])
rcaip_run000_inventory_df = spark.createDataFrame(
    [tuple(x[k] for k in ["file_name", "file_path", "size_bytes", "evidence_type", "requirement"]) for x in inventory_records],
    schema=inventory_schema,
) if inventory_records else spark.createDataFrame([], schema=inventory_schema)

manifest_display_schema = T.StructType([
    T.StructField("file", T.StringType(), False),
    T.StructField("scan_scope", T.StringType(), False),
    T.StructField("target_cidr", T.StringType(), False),
    T.StructField("target_cidrs", T.StringType(), False),
    T.StructField("target_cidr_count", T.IntegerType(), False),
    T.StructField("target_source", T.StringType(), False),
    T.StructField("scope_status", T.StringType(), False),
])
rcaip_run000_manifest_df = spark.createDataFrame(
    [
        (
            x["file"], x["scan_scope"], x["target_cidr"], " | ".join(x["target_cidrs"]),
            int(x["target_cidr_count"]), x["target_source"], x["scope_status"],
        )
        for x in manifest_records
    ],
    schema=manifest_display_schema,
) if manifest_records else spark.createDataFrame([], schema=manifest_display_schema)

print("=" * 100)
print("RCAIP RUN 000 - CELL 1 - FIELD INTAKE AND EVIDENCE READINESS")
print("=" * 100)
print("Status:", "PASS" if intake_ready else "REVIEW_REQUIRED")
print("Post:", manual_post_name)
print("Post slug:", post_profile_key)
print("Country:", country_name or "NOT AVAILABLE")
print("Assessment dates:", assessment_start_text, "to", assessment_end_text)
print("OpenNet CIDRs:", " | ".join(opennet_cidrs) or "NOT AVAILABLE")
print("OpenNet source:", opennet_cidr_source)
print("CMDB files:", len(cmdb_records))
print("Nmap XML files:", len(nmap_records))
print("Multi-target XML files:", sum(1 for x in manifest_records if x["target_cidr_count"] > 1))
print("Optional evidence:", " | ".join(f"{k}={v}" for k, v in sorted(optional_evidence_status.items())))
print("Intake SHA-256:", RCAIP_RUN000_INTAKE_SHA256)
print("Delta writes: NONE")
print("=" * 100)

display(rcaip_run000_readiness_df.orderBy(F.col("check_passed").asc(), "readiness_check"))
display(rcaip_run000_inventory_df.orderBy("evidence_type", "file_name"))
display(rcaip_run000_manifest_df.orderBy("file"))

if not intake_ready:
    failed = [name for name, passed, _ in readiness_checks if not passed]
    raise RuntimeError("RUN 000 intake requires review: " + " | ".join(failed))

# COMMAND ----------

# ============================================================================
# CELL 2 - CERTIFICATION PREVIEW
#
# Purpose:
#   Build the complete durable Post/run contract and show exactly what Cell 3
#   will publish. No Delta writes occur in this cell.
# ============================================================================

import copy
import hashlib
import ipaddress
import json
import os
import re
import xml.etree.ElementTree as ET
from collections import Counter

from pyspark.sql import functions as F
from pyspark.sql import types as T

if not globals().get("RCAIP_RUN000_INTAKE_READY", False):
    raise RuntimeError("Run Cell 1 successfully before Cell 2.")
if "RCAIP_RUN000_INTAKE_CONTRACT" not in globals():
    raise RuntimeError("Cell 1 intake contract is unavailable.")

intake = copy.deepcopy(RCAIP_RUN000_INTAKE_CONTRACT)
if sha256_json(intake) != RCAIP_RUN000_INTAKE_SHA256:
    raise RuntimeError("Cell 1 intake contract changed in memory. Rerun Cell 1.")


def classify_single_target(cidr):
    network = ipaddress.ip_network(cidr, strict=False)
    first_octet = int(str(network.network_address).split(".")[0])
    if first_octet == 10:
        return "INTERNAL_OPENNET", "ADDRESS_SPACE_10"
    if first_octet == 100:
        return "INTERNAL_ENTERPRISE_WIFI", "ADDRESS_SPACE_100"
    if network.is_global:
        return "PUBLIC_FACING", "PUBLICLY_ROUTABLE_ADDRESS_SPACE"
    return "REVIEW_REQUIRED", "ADDRESS_SPACE_NOT_CLASSIFIED"


def classify_target_set(target_cidrs):
    if not target_cidrs:
        return "REVIEW_REQUIRED", "TARGET_SCOPE_MISSING"
    classifications = [classify_single_target(cidr) for cidr in target_cidrs]
    perspectives = ordered_unique([x[0] for x in classifications])
    bases = ordered_unique([x[1] for x in classifications])
    if len(perspectives) == 1 and perspectives[0] != "REVIEW_REQUIRED":
        basis = bases[0] if len(target_cidrs) == 1 else "MULTI_TARGET_ALL_" + perspectives[0]
        return perspectives[0], basis
    return "REVIEW_REQUIRED", "MIXED_OR_UNCLASSIFIED_TARGET_PERSPECTIVES"


def classify_network_role(scan_perspective, file_name):
    lower_name = str(file_name).lower()
    if scan_perspective == "INTERNAL_OPENNET":
        return "OPENNET", "Department Enterprise"
    if scan_perspective == "INTERNAL_ENTERPRISE_WIFI":
        return "ENTERPRISE_WIFI", "Enterprise WiFi"
    if scan_perspective == "PUBLIC_FACING":
        if any(token in lower_name for token in ["entwifi", "enterprise_wifi", "wifi"]):
            return "ENTERPRISE_WIFI_PUBLIC", "Enterprise WiFi"
        if "nen" in lower_name:
            return "NEN_PUBLIC", "NEN"
        return "PUBLIC_SERVICE", "UNRESOLVED"
    return "UNRESOLVED", "UNRESOLVED"


def compatibility_contract(scan_perspective, network_role, service_owner):
    if scan_perspective == "INTERNAL_OPENNET":
        return "Enterprise", "OpenNet", "Internal OpenNet enterprise scan."
    if scan_perspective == "INTERNAL_ENTERPRISE_WIFI":
        return "Enterprise WiFi", "EntWifi Internal", "Internal Enterprise WiFi host-discovery evidence."
    if scan_perspective == "PUBLIC_FACING" and network_role == "ENTERPRISE_WIFI_PUBLIC":
        return "Enterprise WiFi", "EntWifi Public", "Public-facing Enterprise WiFi scan."
    if scan_perspective == "PUBLIC_FACING":
        suffix = "" if service_owner in {"", "UNRESOLVED"} else f"; service context: {service_owner}"
        return "Public Facing", "Public Facing", "Public-facing service scan" + suffix + "."
    return "UNRESOLVED", "REVIEW_REQUIRED", "Classification requires RCSO review."


def collection_capability(unique_mac_count, hostname_count, port_record_count):
    values = ["HOST_OBSERVATION"]
    if unique_mac_count > 0:
        values.append("LAYER2_MAC")
    if hostname_count > 0:
        values.append("HOSTNAME")
    if port_record_count > 0:
        values.append("PORT_SERVICE")
    return "|".join(values)


# Read each XML exactly once; manifest basenames were already certified unique.
xml_binary_df = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.xml")
    .load(intake["input_base_dir"])
)
xml_by_name = {
    os.path.basename(row["path"]): bytes(row["content"])
    for row in xml_binary_df.select("path", "content").collect()
}

nmap_scans = []
classification_errors = []

for manifest in intake["nmap_manifest"]:
    file_name = manifest["file"]
    if file_name not in xml_by_name:
        classification_errors.append(file_name + ": XML content not found")
        continue

    try:
        root = ET.fromstring(xml_by_name[file_name])
        scan_start_raw = str(root.attrib.get("startstr", ""))
        ipv4_values = set()
        mac_values = set()
        hostname_values = set()
        port_record_count = 0

        for host in root.findall("host"):
            for address in host.findall("address"):
                address_type = str(address.attrib.get("addrtype", "")).lower()
                address_value = str(address.attrib.get("addr", "")).strip()
                if address_type == "ipv4" and address_value:
                    ipv4_values.add(address_value)
                elif address_type == "mac" and address_value:
                    mac_values.add(address_value.upper())
            hostnames_element = host.find("hostnames")
            if hostnames_element is not None:
                for hostname in hostnames_element.findall("hostname"):
                    name = str(hostname.attrib.get("name", "")).strip()
                    if name:
                        hostname_values.add(name)
            port_record_count += len(host.findall("./ports/port"))

        scan_perspective, perspective_basis = classify_target_set(manifest["target_cidrs"])
        network_role, service_owner = classify_network_role(scan_perspective, file_name)
        asset_domain, scan_scope, notes = compatibility_contract(scan_perspective, network_role, service_owner)

        if scan_perspective == "REVIEW_REQUIRED":
            classification_errors.append(file_name + ": " + perspective_basis)

        nmap_scans.append(
            {
                "file": file_name,
                "asset_domain": asset_domain,
                "scan_scope": scan_scope,
                "scan_location": intake["post_name"],
                "target_cidr": manifest["target_cidr"],
                "target_cidrs": list(manifest["target_cidrs"]),
                "target_cidr_count": int(manifest["target_cidr_count"]),
                "target_source": manifest["target_source"],
                "xml_args_status": manifest["xml_args_status"],
                "notes": notes,
                "scan_perspective": scan_perspective,
                "network_role": network_role,
                "service_owner": service_owner,
                "perspective_basis": perspective_basis,
                "collection_capability": collection_capability(len(mac_values), len(hostname_values), port_record_count),
                "scan_start_raw": scan_start_raw,
                "observed_ipv4_count": len(ipv4_values),
                "observed_mac_count": len(mac_values),
                "observed_hostname_count": len(hostname_values),
                "port_record_count": port_record_count,
                "mature_contract_role": "COMPATIBILITY_ONLY",
                "v3_semantic_authority": "scan_perspective|network_role|service_owner|target_cidrs",
                "manifest_contract_version": "3.1.0",
                "source_file_path": manifest["source_file_path"],
            }
        )
    except Exception as exc:
        classification_errors.append(file_name + ": XML_PARSE_ERROR: " + str(exc))

if classification_errors:
    raise RuntimeError("Nmap certification preview requires review: " + " | ".join(classification_errors))
if len(nmap_scans) != len(intake["nmap_manifest"]):
    raise RuntimeError("Nmap certification preview did not preserve one record per XML.")

assessment_start_date_preview = datetime.strptime(intake["assessment_start_date"], "%Y-%m-%d").date()
RCAIP_INTAKE_RELEASE_SERIES = "2.0"
release_version = f"{RCAIP_INTAKE_RELEASE_SERIES}-{intake['post_profile_key']}"
release_token = re.sub(r"[^a-z0-9_]+", "_", release_version.lower()).strip("_")
run_id = f"{intake['post_profile_key']}_{assessment_start_date_preview.strftime('%Y%m%d')}_{release_token}"
initial_run_mode = "DISCOVERY"

profile_entry = {
    "post_name": intake["post_name"],
    "post_slug": intake["post_profile_key"],
    "country_name": intake["country_name"],
    "post_device_prefix": intake["post_token"],
    "post_tokens": list(intake["profile_tokens"]),
    "dns_tokens": list(intake["profile_tokens"]),
    "user_location_tokens": list(intake["profile_tokens"]),
    "mde_tokens": list(intake["profile_tokens"]),
    "enterprise_cidrs": list(intake["opennet_cidrs"]),
    "nen_public_targets": list(intake["nen_public_targets"]),
    "entwifi_public_targets": list(intake["entwifi_public_targets"]),
    "base_dir": intake["input_base_dir"],
    "cmdb_file": intake["cmdb_file"],
    "qualys_easm_file": intake["qualys_file"],
    "nmap_scans": nmap_scans,
}
profile_hash = sha256_json(profile_entry)
manifest_sha256 = sha256_json(nmap_scans)

post_context_payload = {
    "contract_version": "3.1.0",
    "post_profile_key": intake["post_profile_key"],
    "base_profile_hash": profile_hash,
    "entwifi_internal_cidrs": list(intake["entwifi_internal_cidrs"]),
    "umbrella_edge_deployed": intake["umbrella_edge_deployed"],
}
post_context_hash = sha256_json(post_context_payload)

run_evidence_payload = {
    "contract_version": "3.1.0",
    "run_id": run_id,
    "post_profile_key": intake["post_profile_key"],
    "base_profile_hash": profile_hash,
    "umbrella_edge_config_reviewed": intake["umbrella_edge_config_reviewed"],
    "umbrella_endpoint_files": list(intake["umbrella_endpoint_files"]),
    "optional_evidence_status": dict(intake["optional_evidence_status"]),
}
run_evidence_hash = sha256_json(run_evidence_payload)

nmap_evidence_records = []
for scan in nmap_scans:
    hash_payload = {
        "run_id": run_id,
        "post_profile_key": intake["post_profile_key"],
        "base_profile_hash": profile_hash,
        "manifest_sha256": manifest_sha256,
        "file": scan["file"],
        "target_cidr": scan["target_cidr"],
        "target_cidrs": scan["target_cidrs"],
        "scan_perspective": scan["scan_perspective"],
        "network_role": scan["network_role"],
        "service_owner": scan["service_owner"],
        "collection_capability": scan["collection_capability"],
    }
    nmap_evidence_records.append(
        {
            "run_id": run_id,
            "post_profile_key": intake["post_profile_key"],
            "base_profile_hash": profile_hash,
            "contract_version": "3.1.0",
            "manifest_sha256": manifest_sha256,
            "record_sha256": sha256_json(hash_payload),
            "file": scan["file"],
            "source_file_path": scan["source_file_path"],
            "asset_domain": scan["asset_domain"],
            "scan_scope": scan["scan_scope"],
            "scan_location": scan["scan_location"],
            "target_cidr": scan["target_cidr"],
            "target_cidrs_json": canonical_json(scan["target_cidrs"]),
            "target_cidr_count": int(scan["target_cidr_count"]),
            "target_source": scan["target_source"],
            "xml_args_status": scan["xml_args_status"],
            "notes": scan["notes"],
            "scan_perspective": scan["scan_perspective"],
            "network_role": scan["network_role"],
            "service_owner": scan["service_owner"],
            "perspective_basis": scan["perspective_basis"],
            "collection_capability": scan["collection_capability"],
            "scan_start_raw": scan["scan_start_raw"],
            "mature_contract_role": scan["mature_contract_role"],
            "v3_semantic_authority": scan["v3_semantic_authority"],
            "evidence_type": "NMAP_XML",
            "evidence_status": "CERTIFIED",
        }
    )

preview_checks = [
    ("Cell 1 intake hash stable", sha256_json(intake) == RCAIP_RUN000_INTAKE_SHA256, RCAIP_RUN000_INTAKE_SHA256),
    ("One durable Nmap record per XML", len(nmap_evidence_records) == len(intake["nmap_manifest"]), str(len(nmap_evidence_records))),
    ("Unique Nmap evidence files", len({x["file"] for x in nmap_evidence_records}) == len(nmap_evidence_records), str(len({x["file"] for x in nmap_evidence_records}))),
    ("All Nmap perspectives classified", all(x["scan_perspective"] != "REVIEW_REQUIRED" for x in nmap_scans), "PASS"),
    ("D0024 target lists preserved", all(x["target_cidr_count"] == len(x["target_cidrs"]) and x["target_cidr_count"] >= 1 for x in nmap_scans), "PASS"),
    ("Profile hash generated", bool(profile_hash), profile_hash),
    ("Run ID generated", bool(run_id), run_id),
]
preview_ready = all(x[1] for x in preview_checks)

RCAIP_RUN000_CERTIFICATION_PREVIEW = {
    "intake_sha256": RCAIP_RUN000_INTAKE_SHA256,
    "profile_entry": profile_entry,
    "profile_hash": profile_hash,
    "run_id": run_id,
    "release_version": release_version,
    "run_mode": initial_run_mode,
    "post_context_payload": post_context_payload,
    "post_context_hash": post_context_hash,
    "run_evidence_payload": run_evidence_payload,
    "run_evidence_hash": run_evidence_hash,
    "manifest_sha256": manifest_sha256,
    "nmap_evidence_records": nmap_evidence_records,
}
RCAIP_RUN000_CERTIFICATION_READY = preview_ready

# Compatibility publication for existing transition documentation.
RCAIP_INTAKE_CONFIG_000_PROFILE_ENTRY = profile_entry
RCAIP_INTAKE_CONFIG_000_PROFILE_CERTIFIED = preview_ready

preview_schema = T.StructType([
    T.StructField("certification_check", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False),
    T.StructField("observed_value", T.StringType(), False),
])
rcaip_run000_preview_checks_df = spark.createDataFrame(preview_checks, schema=preview_schema)

nmap_preview_schema = T.StructType([
    T.StructField("file", T.StringType(), False),
    T.StructField("target_cidr", T.StringType(), False),
    T.StructField("target_cidrs", T.StringType(), False),
    T.StructField("target_cidr_count", T.IntegerType(), False),
    T.StructField("scan_perspective", T.StringType(), False),
    T.StructField("network_role", T.StringType(), False),
    T.StructField("collection_capability", T.StringType(), False),
])
rcaip_run000_nmap_preview_df = spark.createDataFrame(
    [
        (
            x["file"], x["target_cidr"], " | ".join(x["target_cidrs"]), int(x["target_cidr_count"]),
            x["scan_perspective"], x["network_role"], x["collection_capability"],
        )
        for x in nmap_scans
    ],
    schema=nmap_preview_schema,
)

print("=" * 100)
print("RCAIP RUN 000 - CELL 2 - CERTIFICATION PREVIEW")
print("=" * 100)
print("Status:", "PASS" if preview_ready else "REVIEW_REQUIRED")
print("Post:", intake["post_name"])
print("Post slug:", intake["post_profile_key"])
print("Country:", intake["country_name"])
print("Run ID:", run_id)
print("Release version:", release_version)
print("Profile hash:", profile_hash)
print("Nmap manifest SHA-256:", manifest_sha256)
print("Nmap XML records:", len(nmap_scans))
print("Multi-target XML records:", sum(1 for x in nmap_scans if x["target_cidr_count"] > 1))
print("V3 Post extension hash:", post_context_hash)
print("V3 run evidence hash:", run_evidence_hash)
print("Delta writes: NONE")
print("=" * 100)

display(rcaip_run000_preview_checks_df.orderBy(F.col("check_passed").asc(), "certification_check"))
display(rcaip_run000_nmap_preview_df.orderBy("file"))

if not preview_ready:
    failed = [name for name, passed, _ in preview_checks if not passed]
    raise RuntimeError("RUN 000 certification preview requires review: " + " | ".join(failed))

# COMMAND ----------

# ============================================================================
# CELL 3 - PUBLISH CERTIFIED INTAKE
#
# Purpose:
#   Persist the complete certified intake in one operator-visible write step.
#
# Writes:
#   <results_namespace>.rcaip_post_profile_registry
#   <results_namespace>.rcaip_assessment_run_registry
#   <results_namespace>.rcaip_post_profile_v3_extension
#   <results_namespace>.rcaip_assessment_run_v3_evidence
#   <results_namespace>.rcaip_assessment_run_v3_nmap_evidence
# ============================================================================

import copy
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

if not globals().get("RCAIP_RUN000_CERTIFICATION_READY", False):
    raise RuntimeError("Run Cell 2 successfully before Cell 3.")
if "RCAIP_RUN000_CERTIFICATION_PREVIEW" not in globals():
    raise RuntimeError("Cell 2 certification preview is unavailable.")

preview = copy.deepcopy(RCAIP_RUN000_CERTIFICATION_PREVIEW)
intake = copy.deepcopy(RCAIP_RUN000_INTAKE_CONTRACT)
if preview["intake_sha256"] != RCAIP_RUN000_INTAKE_SHA256 or sha256_json(intake) != RCAIP_RUN000_INTAKE_SHA256:
    raise RuntimeError("Intake contract changed after preview. Rerun Cells 1 and 2 before publishing.")

results_namespace = intake["results_namespace"]
post_profile_key = intake["post_profile_key"]
profile_entry = preview["profile_entry"]
profile_hash = preview["profile_hash"]
run_id = preview["run_id"]
release_version = preview["release_version"]
run_mode = preview["run_mode"]
manifest_sha256 = preview["manifest_sha256"]

assessment_start_date = datetime.strptime(intake["assessment_start_date"], "%Y-%m-%d").date()
assessment_end_date = datetime.strptime(intake["assessment_end_date"], "%Y-%m-%d").date()
certified_at = datetime.now(timezone.utc).replace(tzinfo=None)
certified_by = str(spark.sql("SELECT current_user() AS current_user").first()["current_user"]).strip()

profile_registry_table = f"{results_namespace}.rcaip_post_profile_registry"
run_registry_table = f"{results_namespace}.rcaip_assessment_run_registry"
post_extension_table = f"{results_namespace}.rcaip_post_profile_v3_extension"
run_evidence_table = f"{results_namespace}.rcaip_assessment_run_v3_evidence"
nmap_evidence_table = f"{results_namespace}.rcaip_assessment_run_v3_nmap_evidence"


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def add_missing_columns(table_name, required_fields):
    existing = {field.name: field.dataType.simpleString().lower() for field in spark.table(table_name).schema.fields}
    additions = []
    for column_name, sql_type in required_fields:
        if column_name not in existing:
            additions.append(f"{column_name} {sql_type}")
    if additions:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(additions)})")


def align_to_table(dataframe, table_name):
    target_schema = spark.table(table_name).schema
    current_columns = set(dataframe.columns)
    result = dataframe
    for field in target_schema.fields:
        if field.name not in current_columns:
            result = result.withColumn(field.name, F.lit(None).cast(field.dataType))
    return result.select(*[field.name for field in target_schema.fields])


# ---------------------------------------------------------------------------
# Create or evolve durable contracts
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {profile_registry_table} (
    post_profile_key STRING, post_name STRING, post_slug STRING, country_name STRING,
    post_device_prefix STRING, post_tokens_json STRING, dns_tokens_json STRING,
    user_location_tokens_json STRING, mde_tokens_json STRING, enterprise_cidrs_json STRING,
    nen_public_targets_json STRING, entwifi_public_targets_json STRING, input_base_dir STRING,
    cmdb_file STRING, qualys_easm_file STRING, nmap_scans_json STRING, profile_hash STRING,
    lookup_table STRING, lookup_status STRING, profile_status STRING, active BOOLEAN,
    certified_at TIMESTAMP, certified_by STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {run_registry_table} (
    run_id STRING, post_profile_key STRING, assessment_start_date DATE, assessment_end_date DATE,
    results_namespace STRING, release_version STRING, run_mode STRING, run_status STRING,
    profile_hash STRING, active BOOLEAN, certified_at TIMESTAMP, certified_by STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {post_extension_table} (
    post_profile_key STRING, base_profile_hash STRING, contract_version STRING,
    entwifi_internal_cidrs_json STRING, umbrella_edge_deployed STRING, extension_hash STRING,
    extension_status STRING, active BOOLEAN, certified_at TIMESTAMP, certified_by STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {run_evidence_table} (
    run_id STRING, post_profile_key STRING, base_profile_hash STRING, contract_version STRING,
    umbrella_edge_config_reviewed STRING, umbrella_endpoint_files_json STRING,
    optional_evidence_status_json STRING, extension_hash STRING, extension_status STRING,
    active BOOLEAN, certified_at TIMESTAMP, certified_by STRING
) USING DELTA
""")

# Existing V3 Nmap evidence tables use the mature singular target_cidr contract.
# D0024 adds four nullable columns without deleting or renaming any mature field.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {nmap_evidence_table} (
    run_id STRING, post_profile_key STRING, base_profile_hash STRING, contract_version STRING,
    manifest_sha256 STRING, record_sha256 STRING, file STRING, source_file_path STRING,
    asset_domain STRING, scan_scope STRING, scan_location STRING, target_cidr STRING,
    notes STRING, scan_perspective STRING, network_role STRING, service_owner STRING,
    perspective_basis STRING, collection_capability STRING, scan_start_raw STRING,
    mature_contract_role STRING, v3_semantic_authority STRING, evidence_type STRING,
    evidence_status STRING, active BOOLEAN, certified_at TIMESTAMP, certified_by STRING,
    target_cidrs_json STRING, target_cidr_count INT, target_source STRING, xml_args_status STRING
) USING DELTA
""")
add_missing_columns(
    nmap_evidence_table,
    [
        ("target_cidrs_json", "STRING"),
        ("target_cidr_count", "INT"),
        ("target_source", "STRING"),
        ("xml_args_status", "STRING"),
    ],
)


# ---------------------------------------------------------------------------
# Base Post profile
# ---------------------------------------------------------------------------

profile_schema = T.StructType([
    T.StructField("post_profile_key", T.StringType(), False),
    T.StructField("post_name", T.StringType(), False),
    T.StructField("post_slug", T.StringType(), False),
    T.StructField("country_name", T.StringType(), False),
    T.StructField("post_device_prefix", T.StringType(), False),
    T.StructField("post_tokens_json", T.StringType(), False),
    T.StructField("dns_tokens_json", T.StringType(), False),
    T.StructField("user_location_tokens_json", T.StringType(), False),
    T.StructField("mde_tokens_json", T.StringType(), False),
    T.StructField("enterprise_cidrs_json", T.StringType(), False),
    T.StructField("nen_public_targets_json", T.StringType(), False),
    T.StructField("entwifi_public_targets_json", T.StringType(), False),
    T.StructField("input_base_dir", T.StringType(), False),
    T.StructField("cmdb_file", T.StringType(), False),
    T.StructField("qualys_easm_file", T.StringType(), True),
    T.StructField("nmap_scans_json", T.StringType(), False),
    T.StructField("profile_hash", T.StringType(), False),
    T.StructField("lookup_table", T.StringType(), True),
    T.StructField("lookup_status", T.StringType(), False),
    T.StructField("profile_status", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("certified_at", T.TimestampType(), False),
    T.StructField("certified_by", T.StringType(), False),
])

profile_df = spark.createDataFrame([
    (
        post_profile_key, profile_entry["post_name"], profile_entry["post_slug"], profile_entry["country_name"],
        profile_entry["post_device_prefix"], canonical_json(profile_entry.get("post_tokens", [])),
        canonical_json(profile_entry.get("dns_tokens", [])), canonical_json(profile_entry.get("user_location_tokens", [])),
        canonical_json(profile_entry.get("mde_tokens", [])), canonical_json(profile_entry.get("enterprise_cidrs", [])),
        canonical_json(profile_entry.get("nen_public_targets", [])), canonical_json(profile_entry.get("entwifi_public_targets", [])),
        profile_entry["base_dir"], profile_entry["cmdb_file"], profile_entry.get("qualys_easm_file"),
        canonical_json(profile_entry.get("nmap_scans", [])), profile_hash, intake["lookup_table"], intake["lookup_status"],
        "CERTIFIED", True, certified_at, certified_by,
    )
], schema=profile_schema)
profile_df.createOrReplaceTempView("rcaip_run000_profile_source")
spark.sql(f"""
MERGE INTO {profile_registry_table} AS target
USING rcaip_run000_profile_source AS source
ON target.post_profile_key = source.post_profile_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# ---------------------------------------------------------------------------
# Assessment run
# ---------------------------------------------------------------------------

run_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("post_profile_key", T.StringType(), False),
    T.StructField("assessment_start_date", T.DateType(), False),
    T.StructField("assessment_end_date", T.DateType(), False),
    T.StructField("results_namespace", T.StringType(), False),
    T.StructField("release_version", T.StringType(), False),
    T.StructField("run_mode", T.StringType(), False),
    T.StructField("run_status", T.StringType(), False),
    T.StructField("profile_hash", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("certified_at", T.TimestampType(), False),
    T.StructField("certified_by", T.StringType(), False),
])
run_df = spark.createDataFrame([
    (
        run_id, post_profile_key, assessment_start_date, assessment_end_date, results_namespace,
        release_version, run_mode, "CERTIFIED_INTAKE", profile_hash, True, certified_at, certified_by,
    )
], schema=run_schema)
run_df.createOrReplaceTempView("rcaip_run000_run_source")
spark.sql(
    f"UPDATE {run_registry_table} SET active = false "
    f"WHERE post_profile_key = {sql_literal(post_profile_key)} AND run_id <> {sql_literal(run_id)}"
)
spark.sql(f"""
MERGE INTO {run_registry_table} AS target
USING rcaip_run000_run_source AS source
ON target.run_id = source.run_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# ---------------------------------------------------------------------------
# V3 Post context - exactly one active extension per Post
# ---------------------------------------------------------------------------

post_context = preview["post_context_payload"]
post_extension_schema = T.StructType([
    T.StructField("post_profile_key", T.StringType(), False),
    T.StructField("base_profile_hash", T.StringType(), False),
    T.StructField("contract_version", T.StringType(), False),
    T.StructField("entwifi_internal_cidrs_json", T.StringType(), False),
    T.StructField("umbrella_edge_deployed", T.StringType(), False),
    T.StructField("extension_hash", T.StringType(), False),
    T.StructField("extension_status", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("certified_at", T.TimestampType(), False),
    T.StructField("certified_by", T.StringType(), False),
])
post_extension_df = spark.createDataFrame([
    (
        post_profile_key, profile_hash, post_context["contract_version"],
        canonical_json(post_context["entwifi_internal_cidrs"]), post_context["umbrella_edge_deployed"],
        preview["post_context_hash"], "CERTIFIED", True, certified_at, certified_by,
    )
], schema=post_extension_schema)
post_extension_df.createOrReplaceTempView("rcaip_run000_post_extension_source")
spark.sql(
    f"UPDATE {post_extension_table} SET active = false "
    f"WHERE post_profile_key = {sql_literal(post_profile_key)} "
    f"AND (base_profile_hash <> {sql_literal(profile_hash)} OR extension_hash <> {sql_literal(preview['post_context_hash'])})"
)
spark.sql(f"""
MERGE INTO {post_extension_table} AS target
USING rcaip_run000_post_extension_source AS source
ON target.post_profile_key = source.post_profile_key
AND target.base_profile_hash = source.base_profile_hash
AND target.extension_hash = source.extension_hash
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# ---------------------------------------------------------------------------
# V3 run evidence - one active extension for the exact run
# ---------------------------------------------------------------------------

run_evidence = preview["run_evidence_payload"]
run_evidence_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("post_profile_key", T.StringType(), False),
    T.StructField("base_profile_hash", T.StringType(), False),
    T.StructField("contract_version", T.StringType(), False),
    T.StructField("umbrella_edge_config_reviewed", T.StringType(), False),
    T.StructField("umbrella_endpoint_files_json", T.StringType(), False),
    T.StructField("optional_evidence_status_json", T.StringType(), False),
    T.StructField("extension_hash", T.StringType(), False),
    T.StructField("extension_status", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("certified_at", T.TimestampType(), False),
    T.StructField("certified_by", T.StringType(), False),
])
run_evidence_df = spark.createDataFrame([
    (
        run_id, post_profile_key, profile_hash, run_evidence["contract_version"],
        run_evidence["umbrella_edge_config_reviewed"], canonical_json(run_evidence["umbrella_endpoint_files"]),
        canonical_json(run_evidence["optional_evidence_status"]), preview["run_evidence_hash"],
        "CERTIFIED", True, certified_at, certified_by,
    )
], schema=run_evidence_schema)
run_evidence_df.createOrReplaceTempView("rcaip_run000_run_evidence_source")
spark.sql(
    f"UPDATE {run_evidence_table} SET active = false "
    f"WHERE run_id = {sql_literal(run_id)} AND extension_hash <> {sql_literal(preview['run_evidence_hash'])}"
)
spark.sql(f"""
MERGE INTO {run_evidence_table} AS target
USING rcaip_run000_run_evidence_source AS source
ON target.run_id = source.run_id
AND target.base_profile_hash = source.base_profile_hash
AND target.extension_hash = source.extension_hash
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# ---------------------------------------------------------------------------
# Certified Nmap run evidence - one durable row per XML, D0024-safe
# ---------------------------------------------------------------------------

nmap_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("post_profile_key", T.StringType(), False),
    T.StructField("base_profile_hash", T.StringType(), False),
    T.StructField("contract_version", T.StringType(), False),
    T.StructField("manifest_sha256", T.StringType(), False),
    T.StructField("record_sha256", T.StringType(), False),
    T.StructField("file", T.StringType(), False),
    T.StructField("source_file_path", T.StringType(), False),
    T.StructField("asset_domain", T.StringType(), False),
    T.StructField("scan_scope", T.StringType(), False),
    T.StructField("scan_location", T.StringType(), False),
    T.StructField("target_cidr", T.StringType(), False),
    T.StructField("notes", T.StringType(), False),
    T.StructField("scan_perspective", T.StringType(), False),
    T.StructField("network_role", T.StringType(), False),
    T.StructField("service_owner", T.StringType(), False),
    T.StructField("perspective_basis", T.StringType(), False),
    T.StructField("collection_capability", T.StringType(), False),
    T.StructField("scan_start_raw", T.StringType(), True),
    T.StructField("mature_contract_role", T.StringType(), False),
    T.StructField("v3_semantic_authority", T.StringType(), False),
    T.StructField("evidence_type", T.StringType(), False),
    T.StructField("evidence_status", T.StringType(), False),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("certified_at", T.TimestampType(), False),
    T.StructField("certified_by", T.StringType(), False),
    T.StructField("target_cidrs_json", T.StringType(), True),
    T.StructField("target_cidr_count", T.IntegerType(), True),
    T.StructField("target_source", T.StringType(), True),
    T.StructField("xml_args_status", T.StringType(), True),
])

nmap_rows = []
for record in preview["nmap_evidence_records"]:
    nmap_rows.append(
        (
            record["run_id"], record["post_profile_key"], record["base_profile_hash"], record["contract_version"],
            record["manifest_sha256"], record["record_sha256"], record["file"], record["source_file_path"],
            record["asset_domain"], record["scan_scope"], record["scan_location"], record["target_cidr"], record["notes"],
            record["scan_perspective"], record["network_role"], record["service_owner"], record["perspective_basis"],
            record["collection_capability"], record["scan_start_raw"], record["mature_contract_role"],
            record["v3_semantic_authority"], record["evidence_type"], record["evidence_status"], True,
            certified_at, certified_by, record["target_cidrs_json"], int(record["target_cidr_count"]),
            record["target_source"], record["xml_args_status"],
        )
    )

nmap_df = spark.createDataFrame(nmap_rows, schema=nmap_schema)
nmap_df = align_to_table(nmap_df, nmap_evidence_table)
replace_predicate = (
    "post_profile_key = " + sql_literal(post_profile_key) +
    " AND run_id = " + sql_literal(run_id)
)
(
    nmap_df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", replace_predicate)
    .saveAsTable(nmap_evidence_table)
)


# ---------------------------------------------------------------------------
# Read-back certification
# ---------------------------------------------------------------------------

profile_count = (
    spark.table(profile_registry_table)
    .filter((F.col("post_profile_key") == post_profile_key) & F.col("active") & (F.col("profile_status") == "CERTIFIED"))
    .count()
)
run_count = (
    spark.table(run_registry_table)
    .filter((F.col("run_id") == run_id) & F.col("active") & (F.col("run_status") == "CERTIFIED_INTAKE") & (F.col("profile_hash") == profile_hash))
    .count()
)
post_extension_count = (
    spark.table(post_extension_table)
    .filter((F.col("post_profile_key") == post_profile_key) & F.col("active") & (F.col("base_profile_hash") == profile_hash) & (F.col("extension_hash") == preview["post_context_hash"]))
    .count()
)
run_evidence_count = (
    spark.table(run_evidence_table)
    .filter((F.col("run_id") == run_id) & F.col("active") & (F.col("base_profile_hash") == profile_hash) & (F.col("extension_hash") == preview["run_evidence_hash"]))
    .count()
)
certified_nmap_df = spark.table(nmap_evidence_table).filter(
    (F.col("post_profile_key") == post_profile_key) &
    (F.col("run_id") == run_id) &
    F.col("active") &
    (F.col("evidence_status") == "CERTIFIED")
)
nmap_count = certified_nmap_df.count()
nmap_unique_file_count = certified_nmap_df.select("file").distinct().count()
nmap_manifest_hashes = {row["manifest_sha256"] for row in certified_nmap_df.select("manifest_sha256").distinct().collect()}

publication_checks = [
    ("Active certified Post profile", profile_count == 1, str(profile_count)),
    ("Active certified assessment run", run_count == 1, str(run_count)),
    ("Active V3 Post extension", post_extension_count == 1, str(post_extension_count)),
    ("Active V3 run evidence", run_evidence_count == 1, str(run_evidence_count)),
    ("Certified Nmap row count", nmap_count == len(preview["nmap_evidence_records"]), f"{nmap_count} / {len(preview['nmap_evidence_records'])}"),
    ("Certified Nmap unique files", nmap_unique_file_count == len(preview["nmap_evidence_records"]), str(nmap_unique_file_count)),
    ("Certified Nmap manifest hash", nmap_manifest_hashes == {manifest_sha256}, " | ".join(sorted(nmap_manifest_hashes)) or "NONE"),
]
publication_ready = all(x[1] for x in publication_checks)

RCAIP_INTAKE_PROFILE_REGISTRY_TABLE = profile_registry_table
RCAIP_INTAKE_RUN_REGISTRY_TABLE = run_registry_table
RCAIP_INTAKE_PUBLISHED_RUN_ID = run_id
RCAIP_INTAKE_PUBLISHED_PROFILE_HASH = profile_hash
RCAIP_INTAKE_REGISTRY_PUBLICATION_READY = publication_ready
RCAIP_RUN000_PUBLICATION_READY = publication_ready

publication_schema = T.StructType([
    T.StructField("publication_check", T.StringType(), False),
    T.StructField("check_passed", T.BooleanType(), False),
    T.StructField("observed_value", T.StringType(), False),
])
rcaip_run000_publication_df = spark.createDataFrame(publication_checks, schema=publication_schema)

print("=" * 100)
print("RCAIP RUN 000 - CELL 3 - CERTIFIED INTAKE PUBLICATION")
print("=" * 100)
print("Status:", "PASS" if publication_ready else "FAIL")
print("Post:", profile_entry["post_name"])
print("Post slug:", post_profile_key)
print("Country:", profile_entry["country_name"])
print("Run ID:", run_id)
print("Profile hash:", profile_hash)
print("Nmap manifest SHA-256:", manifest_sha256)
print("Multi-target XML records:", sum(1 for x in profile_entry["nmap_scans"] if x["target_cidr_count"] > 1))
print("Profile registry:", profile_registry_table)
print("Run registry:", run_registry_table)
print("V3 Post extension:", post_extension_table)
print("V3 run evidence:", run_evidence_table)
print("V3 Nmap evidence:", nmap_evidence_table)
print("Automatic Post assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("Next step: RCAIP_Config_000_Post_Run_Config")
print("=" * 100)

display(rcaip_run000_publication_df.orderBy(F.col("check_passed").asc(), "publication_check"))

if not publication_ready:
    failed = [name for name, passed, _ in publication_checks if not passed]
    raise RuntimeError("RUN 000 publication failed certification: " + " | ".join(failed))