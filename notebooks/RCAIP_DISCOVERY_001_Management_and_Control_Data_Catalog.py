# Databricks notebook source
# MAGIC %run ./RCAIP_Config_001_Table_Registry

# COMMAND ----------

# ============================================================================
# RCAIP DISCOVERY 001
# Enterprise Management and Control Data Catalog
#
# Purpose:
#   Discover, classify, and prioritize accessible enterprise data sources for
#   the currently certified RCAIP Post and assessment run.
#
# Runtime:
#   Config 001 automatically loads Config 000, which resolves the current
#   certified intake registry record.
#
# Operating rules:
#   - No hard-coded Post identity.
#   - No manually entered Post widgets.
#   - Metadata-only source discovery.
#   - No source-row sampling.
#   - No exact source-table row counts.
#   - No automatic findings, assignments, tickets, or remediation.
#
# Outputs:
#   <namespace>.<post_slug>_discovery_001_schema_catalog
#   <namespace>.<post_slug>_discovery_001_table_catalog
#   <namespace>.<post_slug>_discovery_001_column_catalog
#   <namespace>.<post_slug>_discovery_001_candidate_sources
#   <namespace>.<post_slug>_discovery_001_domain_summary
#   <namespace>.<post_slug>_discovery_001_post_mapping_candidates
#   <namespace>.<post_slug>_discovery_001_ad_core_field_summary
#   <namespace>.<post_slug>_discovery_001_errors
#   <namespace>.<post_slug>_discovery_001_run_certificate
# ============================================================================

from datetime import datetime
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Validate the Config 000 / Config 001 runtime
# ---------------------------------------------------------------------------

required_runtime_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_COUNTRY_NAME",
    "RCAIP_POST_TOKENS",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_RUN_ID",
    "RCAIP_RUN_MODE",
    "RCAIP_ASSESSMENT_START_DATE_TEXT",
    "RCAIP_ASSESSMENT_END_DATE_TEXT",
    "RCAIP_RESOLVED_SOURCE_TABLES",
    "RCAIP_TABLES",
    "rcaip_table"
]

missing_runtime_objects = [
    object_name
    for object_name in required_runtime_objects
    if object_name not in globals()
]

if missing_runtime_objects:
    raise RuntimeError(
        "Discovery 001 did not receive the complete Config 001 runtime: "
        + " | ".join(missing_runtime_objects)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 did not publish a ready status."
    )

if str(RCAIP_RUN_MODE).upper() != "DISCOVERY":
    raise RuntimeError(
        "Discovery 001 requires the certified run to remain in DISCOVERY mode. "
        f"Observed mode: {RCAIP_RUN_MODE}"
    )

if not callable(rcaip_table):
    raise TypeError(
        "rcaip_table must be callable."
    )

DISCOVERY_TIMESTAMP_UTC = datetime.utcnow()

DISCOVERY_SCAN_MODE = "TARGETED"
DISCOVERY_PREFIX = "discovery_001"


# ---------------------------------------------------------------------------
# 2. Define Post-isolated output tables
# ---------------------------------------------------------------------------

RCAIP_DISCOVERY_OUTPUT_TABLES = {
    "schema_catalog":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_schema_catalog"
        ),

    "table_catalog":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_table_catalog"
        ),

    "column_catalog":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_column_catalog"
        ),

    "candidate_sources":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_candidate_sources"
        ),

    "domain_summary":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_domain_summary"
        ),

    "post_mapping_candidates":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_post_mapping_candidates"
        ),

    "ad_core_field_summary":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_ad_core_field_summary"
        ),

    "errors":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_errors"
        ),

    "run_certificate":
        rcaip_table(
            f"{DISCOVERY_PREFIX}_run_certificate"
        )
}


def add_discovery_runtime_columns(dataframe):
    """
    Add the certified RCAIP runtime identity to every durable discovery output.
    """

    return (
        dataframe

        .withColumn(
            "rcaip_run_id",
            F.lit(RCAIP_RUN_ID)
        )

        .withColumn(
            "runtime_post_name",
            F.lit(RCAIP_POST_NAME)
        )

        .withColumn(
            "runtime_post_slug",
            F.lit(RCAIP_POST_SLUG)
        )

        .withColumn(
            "runtime_country_name",
            F.lit(RCAIP_COUNTRY_NAME)
        )

        .withColumn(
            "runtime_run_mode",
            F.lit(RCAIP_RUN_MODE)
        )

        .withColumn(
            "discovery_timestamp_utc",
            F.lit(DISCOVERY_TIMESTAMP_UTC)
            .cast("timestamp")
        )
    )


def save_discovery_table(
    dataframe,
    output_key
):
    """
    Save one Post-isolated Discovery 001 product.

    Each Post has a separate physical table, so a Reykjavík run cannot
    overwrite or mix with example_post_d or another Post.
    """

    if output_key not in RCAIP_DISCOVERY_OUTPUT_TABLES:
        raise KeyError(
            f"Unknown Discovery 001 output key: {output_key}"
        )

    full_table_name = RCAIP_DISCOVERY_OUTPUT_TABLES[
        output_key
    ]

    output_dataframe = add_discovery_runtime_columns(
        dataframe
    )

    (
        output_dataframe.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(
        "Saved:",
        full_table_name
    )

    return full_table_name


# ---------------------------------------------------------------------------
# 3. Generic helpers
# ---------------------------------------------------------------------------

def quote_identifier(identifier):
    """
    Quote a one-, two-, or three-part Spark identifier.
    """

    return ".".join(
        [
            f"`{identifier_part.replace('`', '``')}`"
            for identifier_part
            in str(identifier).split(".")
        ]
    )


def normalized_text(value):
    """
    Return a comparison-safe identifier token.
    """

    return re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().lower()
    ).strip("_")


def first_existing_column(
    dataframe,
    candidate_names
):
    """
    Resolve a dataframe column name without depending on capitalization.
    """

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


def string_or_none(value):

    if value is None:
        return None

    return str(value)


def canonical_table_name(
    table_name,
    current_catalog
):
    """
    Convert schema.table names to catalog.schema.table names.
    """

    table_parts = str(
        table_name or ""
    ).strip().split(".")

    if len(table_parts) == 2:
        return (
            f"{current_catalog}."
            f"{table_parts[0]}."
            f"{table_parts[1]}"
        )

    return str(
        table_name or ""
    ).strip()


# ---------------------------------------------------------------------------
# 4. Discovery scope
#
# devresults is intentionally excluded so Discovery 001 does not recursively
# rediscover its own RCAIP outputs.
# ---------------------------------------------------------------------------

current_catalog = spark.sql(
    "SELECT current_catalog() AS current_catalog"
).first()[
    "current_catalog"
]

catalogs_to_scan = [
    current_catalog
]

target_schemas = ["example_sources"]  # Configure explicitly for an authorized environment.

schema_name_patterns = [
    "active",
    "directory",
    "identity",
    "o365",
    "m365",
    "exchange",
    "defender",
    "mde",
    "intune",
    "tenable",
    "qualys",
    "vulnerability",
    "asset",
    "cmdb",
    "network",
    "cisco",
    "splunk",
    "security",
    "endpoint",
    "mobile",
    "gomobile",
    "servicenow",
    "purview"
]

excluded_schemas = {
    "information_schema",
    "system",
    "default",
    str(RCAIP_RESULTS_NAMESPACE).lower()
}


# ---------------------------------------------------------------------------
# 5. RCAIP management and control domain rules
# ---------------------------------------------------------------------------

DOMAIN_RULES = {
    "Identity / Active Directory": [
        "active_directory",
        "activedirectory",
        "ldap",
        "identity",
        "identities",
        "samaccount",
        "distinguishedname",
        "useraccount",
        "groupmembership",
        "organizationalunit",
        "ad_lookup"
    ],

    "Authentication / Sign-In": [
        "signin",
        "sign_in",
        "authentication",
        "auth",
        "logon",
        "login",
        "mfa",
        "okta",
        "session",
        "actor",
        "risk_event"
    ],

    "Endpoint Management": [
        "intune",
        "manageddevice",
        "managed_device",
        "endpoint",
        "deviceinfo",
        "device_info",
        "defender",
        "mde",
        "machinegroup",
        "machine_group",
        "gomobile"
    ],

    "Security Controls / Configuration": [
        "secureconfiguration",
        "secure_configuration",
        "configurationassessment",
        "configuration_assessment",
        "bitlocker",
        "encryption",
        "firewall",
        "secureboot",
        "secure_boot",
        "tpm",
        "antivirus",
        "compliance",
        "securitycontrol",
        "security_control"
    ],

    "Software / Applications": [
        "software",
        "application",
        "applications",
        "installedapp",
        "installed_app",
        "appinventory",
        "app_inventory",
        "package",
        "publisher",
        "productversion",
        "product_version"
    ],

    "Vulnerability / Exposure": [
        "vulnerability",
        "vulnerabilities",
        "cve",
        "cvss",
        "severity",
        "exploit",
        "kev",
        "patch",
        "remediation",
        "tenable",
        "qualys",
        "trurisk",
        "acrscore"
    ],

    "Network Configuration": [
        "switch",
        "router",
        "firewall",
        "networkdevice",
        "network_device",
        "vlan",
        "interface",
        "snmp",
        "ntp",
        "vty",
        "telnet",
        "ssh",
        "ios",
        "portsecurity",
        "port_security",
        "accesslist",
        "access_list",
        "acl"
    ],

    "Exchange / Mailbox": [
        "exchange",
        "mailbox",
        "recipient",
        "smtp",
        "sendas",
        "send_as",
        "fullaccess",
        "full_access",
        "delegate",
        "delegation",
        "mailpermission",
        "mail_permission",
        "audit_exchange"
    ],

    "Asset / CMDB": [
        "cmdb",
        "asset",
        "inventory",
        "configurationitem",
        "configuration_item",
        "serialnumber",
        "serial_number",
        "manufacturer",
        "model"
    ],

    "Third-Party / Special-Purpose": [
        "printer",
        "mfd",
        "multifunction",
        "ups",
        "pdu",
        "bms",
        "bas",
        "buildingautomation",
        "building_automation",
        "appliance",
        "vendor",
        "firmware",
        "thirdparty",
        "third_party"
    ]
}


WORK_PACKAGE_MAP = {
    "Identity / Active Directory":
        "IP-004 Identity and Active Directory Hygiene",

    "Authentication / Sign-In":
        "IP-004 Identity and Active Directory Hygiene",

    "Endpoint Management":
        "IP-005 Endpoint Security Controls",

    "Security Controls / Configuration":
        "IP-005 Endpoint Security Controls",

    "Software / Applications":
        "IP-006 Software and Application Governance",

    "Vulnerability / Exposure":
        "IP-007 Vulnerability and Exposure Context",

    "Network Configuration":
        "IP-008 Network Infrastructure Configuration",

    "Exchange / Mailbox":
        "IP-009 Exchange and Mailbox Governance",

    "Asset / CMDB":
        "RCAIP Asset Engine / Asset Enrichment",

    "Third-Party / Special-Purpose":
        "IP-010 Third-Party and Special-Purpose Technology",

    "Unclassified":
        "Discovery Review"
}


USE_CASE_MAP = {
    "Identity / Active Directory":
        "Users, computers, groups, privileges, stale objects, and Post attribution.",

    "Authentication / Sign-In":
        "Account activity, MFA, sign-in recency, risk, and user validation.",

    "Endpoint Management":
        "Device enrollment, ownership, contact recency, and management coverage.",

    "Security Controls / Configuration":
        "Encryption, firewall, Defender, Secure Boot, TPM, and compliance.",

    "Software / Applications":
        "Installed software, versions, publishers, agents, and approval status.",

    "Vulnerability / Exposure":
        "CVEs, severity, exploitability, KEVs, remediation, and exposure.",

    "Network Configuration":
        "Switch, router, wireless, firewall, and management-plane configuration.",

    "Exchange / Mailbox":
        "Mailbox ownership, delegation, permissions, and cross-Post access.",

    "Asset / CMDB":
        "Asset identity, authoritative inventory, ownership, location, and lifecycle.",

    "Third-Party / Special-Purpose":
        "Printers, appliances, OT, facilities systems, and vendor technology.",

    "Unclassified":
        "Review table and columns for possible RCAIP use."
}


# ---------------------------------------------------------------------------
# 6. Column-role rules
# ---------------------------------------------------------------------------

COLUMN_ROLE_PATTERNS = {
    "asset_identifier": [
        "asset_id",
        "assetid",
        "device_id",
        "deviceid",
        "machine_id",
        "machineid",
        "host_id",
        "hostid",
        "computer_name",
        "computername",
        "device_name",
        "devicename",
        "host_name",
        "hostname",
        "fqdn",
        "dns_name",
        "dnsname",
        "serial_number",
        "serialnumber",
        "mac_address",
        "macaddress",
        "ip_address",
        "ipaddress",
        "ipv4",
        "publicip"
    ],

    "user_identifier": [
        "user_principal_name",
        "userprincipalname",
        "upn",
        "sam_account_name",
        "samaccountname",
        "username",
        "user_name",
        "account_name",
        "accountname",
        "employee_id",
        "employeeid",
        "actor_id",
        "actorid",
        "actor_email",
        "email",
        "mail"
    ],

    "timestamp": [
        "timestamp",
        "event_time",
        "eventtime",
        "event_date",
        "eventdate",
        "last_seen",
        "lastseen",
        "last_contact",
        "lastcontact",
        "last_logon",
        "lastlogon",
        "created",
        "updated",
        "modified",
        "ingest_time",
        "ingesttime",
        "scan_time",
        "scantime",
        "discovery"
    ],

    "post_mapping": [
        "post",
        "post_name",
        "post_code",
        "mission",
        "site",
        "site_name",
        "sitename",
        "location",
        "office",
        "bureau",
        "subsite",
        "subnet",
        "cidr",
        "ip_address",
        "ipaddress",
        "organizational_unit",
        "organizationalunit",
        "distinguished_name",
        "distinguishedname",
        "ou",
        "device_name",
        "devicename",
        "hostname"
    ],

    "security_control": [
        "bitlocker",
        "encryption",
        "firewall",
        "secure_boot",
        "secureboot",
        "tpm",
        "antivirus",
        "defender",
        "compliance",
        "configuration",
        "control",
        "sensor_health",
        "sensorhealth",
        "real_time_protection",
        "realtimeprotection",
        "asr"
    ],

    "software": [
        "software",
        "application",
        "app_name",
        "appname",
        "product",
        "product_name",
        "productname",
        "package",
        "publisher",
        "vendor",
        "version",
        "installed"
    ],

    "vulnerability": [
        "cve",
        "cvss",
        "vulnerability",
        "severity",
        "exploit",
        "kev",
        "patch",
        "remediation",
        "risk_score",
        "riskscore",
        "trurisk",
        "acr_score",
        "acrscore"
    ],

    "network_configuration": [
        "vlan",
        "switch",
        "router",
        "interface",
        "port",
        "snmp",
        "ntp",
        "vty",
        "telnet",
        "ssh",
        "ios",
        "firmware",
        "configuration",
        "config",
        "access_list",
        "accesslist",
        "acl"
    ],

    "mailbox_exchange": [
        "exchange",
        "mailbox",
        "recipient",
        "smtp",
        "send_as",
        "sendas",
        "full_access",
        "fullaccess",
        "delegate",
        "delegation",
        "mail_permission",
        "mailpermission"
    ],

    "third_party_special_purpose": [
        "printer",
        "mfd",
        "multifunction",
        "ups",
        "pdu",
        "bms",
        "bas",
        "appliance",
        "manufacturer",
        "model",
        "vendor",
        "firmware"
    ]
}


def classify_column(column_name):

    normalized_column = normalized_text(
        column_name
    )

    matched_roles = []

    for role_name, role_patterns in (
        COLUMN_ROLE_PATTERNS.items()
    ):

        if any(
            normalized_text(pattern)
            in
            normalized_column

            for pattern
            in role_patterns
        ):
            matched_roles.append(
                role_name
            )

    return matched_roles


def classify_table(
    catalog_name,
    schema_name,
    table_name,
    column_names
):

    name_text = normalized_text(
        f"{catalog_name} {schema_name} {table_name}"
    )

    column_text = normalized_text(
        " ".join(
            column_names
        )
    )

    domain_scores = {}

    for domain_name, domain_patterns in (
        DOMAIN_RULES.items()
    ):

        domain_score = 0

        for pattern in domain_patterns:

            normalized_pattern = normalized_text(
                pattern
            )

            if normalized_pattern in name_text:
                domain_score += 4

            if normalized_pattern in column_text:
                domain_score += 1

        domain_scores[
            domain_name
        ] = domain_score

    ranked_domains = sorted(
        domain_scores.items(),
        key=lambda item: item[1],
        reverse=True
    )

    primary_domain = "Unclassified"
    primary_domain_score = 0
    secondary_domain = None
    secondary_domain_score = 0

    if ranked_domains:

        if ranked_domains[0][1] > 0:
            primary_domain = ranked_domains[0][0]
            primary_domain_score = int(
                ranked_domains[0][1]
            )

        if (
            len(ranked_domains) > 1
            and
            ranked_domains[1][1] > 0
        ):
            secondary_domain = ranked_domains[1][0]
            secondary_domain_score = int(
                ranked_domains[1][1]
            )

    return {
        "primary_domain":
            primary_domain,

        "primary_domain_score":
            primary_domain_score,

        "secondary_domain":
            secondary_domain,

        "secondary_domain_score":
            secondary_domain_score,

        "recommended_work_package":
            WORK_PACKAGE_MAP.get(
                primary_domain,
                "Discovery Review"
            ),

        "recommended_rcaip_use":
            USE_CASE_MAP.get(
                primary_domain,
                USE_CASE_MAP["Unclassified"]
            )
    }


# ---------------------------------------------------------------------------
# 7. Discover accessible schemas
# ---------------------------------------------------------------------------

schema_rows = []
error_rows = []

for catalog_name in catalogs_to_scan:

    try:

        schema_result = spark.sql(
            f"SHOW SCHEMAS IN "
            f"{quote_identifier(catalog_name)}"
        )

        schema_column = first_existing_column(
            schema_result,
            [
                "databaseName",
                "namespace",
                "schemaName"
            ]
        )

        if schema_column is None:
            raise RuntimeError(
                "Unable to identify the schema-name column returned by "
                "SHOW SCHEMAS."
            )

        for schema_result_row in schema_result.collect():

            schema_name = schema_result_row[
                schema_column
            ]

            if schema_name is None:
                continue

            schema_name = str(
                schema_name
            ).strip()

            schema_name_lower = schema_name.lower()

            if schema_name_lower in excluded_schemas:
                continue

            exact_target_match = (
                schema_name_lower
                in
                {
                    target_schema.lower()
                    for target_schema
                    in target_schemas
                }
            )

            pattern_match = any(
                pattern.lower()
                in
                schema_name_lower

                for pattern
                in schema_name_patterns
            )

            if exact_target_match:

                selected_for_scan = "Y"
                selection_reason = "TARGET_SCHEMA"

            elif pattern_match:

                selected_for_scan = "Y"
                selection_reason = "PATTERN_MATCH"

            else:

                selected_for_scan = "N"
                selection_reason = "NOT_SELECTED"

            schema_rows.append({
                "catalog_name":
                    catalog_name,

                "schema_name":
                    schema_name,

                "full_schema_name":
                    f"{catalog_name}.{schema_name}",

                "selected_for_scan":
                    selected_for_scan,

                "selection_reason":
                    selection_reason
            })

    except Exception as schema_error:

        error_rows.append({
            "discovery_stage":
                "SCHEMA_DISCOVERY",

            "catalog_name":
                catalog_name,

            "schema_name":
                None,

            "table_name":
                None,

            "full_table_name":
                None,

            "error_message":
                str(schema_error)
        })


schema_catalog_schema = T.StructType([
    T.StructField(
        "catalog_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "schema_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "full_schema_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "selected_for_scan",
        T.StringType(),
        False
    ),

    T.StructField(
        "selection_reason",
        T.StringType(),
        False
    )
])

schema_catalog = spark.createDataFrame(
    schema_rows,
    schema=schema_catalog_schema
)

selected_schema_rows = (
    schema_catalog

    .filter(
        F.col(
            "selected_for_scan"
        )
        ==
        F.lit(
            "Y"
        )
    )

    .select(
        "catalog_name",
        "schema_name",
        "full_schema_name"
    )

    .collect()
)

save_discovery_table(
    schema_catalog,
    "schema_catalog"
)


# ---------------------------------------------------------------------------
# 8. Build the metadata-only table inventory
# ---------------------------------------------------------------------------

table_inventory_rows = []

for selected_schema_row in selected_schema_rows:

    catalog_name = selected_schema_row[
        "catalog_name"
    ]

    schema_name = selected_schema_row[
        "schema_name"
    ]

    full_schema_name = selected_schema_row[
        "full_schema_name"
    ]

    try:

        tables_result = spark.sql(
            f"SHOW TABLES IN "
            f"{quote_identifier(full_schema_name)}"
        )

        table_name_column = first_existing_column(
            tables_result,
            [
                "tableName",
                "table_name"
            ]
        )

        temporary_column = first_existing_column(
            tables_result,
            [
                "isTemporary",
                "is_temporary"
            ]
        )

        if table_name_column is None:
            raise RuntimeError(
                "Unable to identify the table-name column returned by "
                "SHOW TABLES."
            )

        for table_result_row in tables_result.collect():

            table_name = str(
                table_result_row[
                    table_name_column
                ]
            ).strip()

            is_temporary = (
                bool(
                    table_result_row[
                        temporary_column
                    ]
                )
                if temporary_column
                else False
            )

            if is_temporary:
                continue

            full_table_name = (
                f"{catalog_name}."
                f"{schema_name}."
                f"{table_name}"
            )

            table_format = None
            table_location = None
            created_at = None
            last_modified = None
            num_files = None
            size_in_bytes = None
            detail_status = "NOT_AVAILABLE"

            try:

                detail_row = (
                    spark.sql(
                        f"DESCRIBE DETAIL "
                        f"{quote_identifier(full_table_name)}"
                    )
                    .first()
                )

                if detail_row is not None:

                    detail_record = detail_row.asDict(
                        recursive=True
                    )

                    table_format = string_or_none(
                        detail_record.get(
                            "format"
                        )
                    )

                    table_location = string_or_none(
                        detail_record.get(
                            "location"
                        )
                    )

                    created_at = string_or_none(
                        detail_record.get(
                            "createdAt"
                        )
                    )

                    last_modified = string_or_none(
                        detail_record.get(
                            "lastModified"
                        )
                    )

                    num_files = detail_record.get(
                        "numFiles"
                    )

                    size_in_bytes = detail_record.get(
                        "sizeInBytes"
                    )

                    detail_status = "AVAILABLE"

            except Exception:
                detail_status = "NOT_SUPPORTED_OR_VIEW"

            table_inventory_rows.append({
                "catalog_name":
                    catalog_name,

                "schema_name":
                    schema_name,

                "table_name":
                    table_name,

                "full_table_name":
                    full_table_name,

                "is_temporary":
                    is_temporary,

                "access_status":
                    "ACCESSIBLE",

                "table_format":
                    table_format,

                "table_location":
                    table_location,

                "created_at":
                    created_at,

                "last_modified":
                    last_modified,

                "num_files":
                    num_files,

                "size_in_bytes":
                    size_in_bytes,

                "detail_status":
                    detail_status
            })

    except Exception as table_error:

        error_rows.append({
            "discovery_stage":
                "TABLE_DISCOVERY",

            "catalog_name":
                catalog_name,

            "schema_name":
                schema_name,

            "table_name":
                None,

            "full_table_name":
                None,

            "error_message":
                str(table_error)
        })


table_inventory_schema = T.StructType([
    T.StructField(
        "catalog_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "schema_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "full_table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "is_temporary",
        T.BooleanType(),
        True
    ),

    T.StructField(
        "access_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "table_format",
        T.StringType(),
        True
    ),

    T.StructField(
        "table_location",
        T.StringType(),
        True
    ),

    T.StructField(
        "created_at",
        T.StringType(),
        True
    ),

    T.StructField(
        "last_modified",
        T.StringType(),
        True
    ),

    T.StructField(
        "num_files",
        T.LongType(),
        True
    ),

    T.StructField(
        "size_in_bytes",
        T.LongType(),
        True
    ),

    T.StructField(
        "detail_status",
        T.StringType(),
        False
    )
])

table_inventory = spark.createDataFrame(
    table_inventory_rows,
    schema=table_inventory_schema
)


# ---------------------------------------------------------------------------
# 9. Build the column catalog
# ---------------------------------------------------------------------------

column_catalog_rows = []
columns_by_table = {}
roles_by_table = {}

for table_inventory_row in table_inventory_rows:

    catalog_name = table_inventory_row[
        "catalog_name"
    ]

    schema_name = table_inventory_row[
        "schema_name"
    ]

    table_name = table_inventory_row[
        "table_name"
    ]

    full_table_name = table_inventory_row[
        "full_table_name"
    ]

    columns_by_table[
        full_table_name
    ] = []

    roles_by_table[
        full_table_name
    ] = {
        role_name: []
        for role_name
        in COLUMN_ROLE_PATTERNS.keys()
    }

    try:

        describe_rows = spark.sql(
            f"DESCRIBE TABLE "
            f"{quote_identifier(full_table_name)}"
        ).collect()

        ordinal_position = 0

        for describe_row in describe_rows:

            describe_record = describe_row.asDict(
                recursive=True
            )

            column_name = describe_record.get(
                "col_name"
            )

            data_type = describe_record.get(
                "data_type"
            )

            column_comment = describe_record.get(
                "comment"
            )

            if column_name is None:
                continue

            column_name = str(
                column_name
            ).strip()

            if not column_name:
                continue

            if column_name.startswith("#"):
                break

            ordinal_position += 1

            column_roles = classify_column(
                column_name
            )

            columns_by_table[
                full_table_name
            ].append(
                column_name
            )

            for column_role in column_roles:

                roles_by_table[
                    full_table_name
                ][
                    column_role
                ].append(
                    column_name
                )

            contains_sensitive_identifier = (
                "Y"
                if (
                    "asset_identifier" in column_roles
                    or
                    "user_identifier" in column_roles
                )
                else "N"
            )

            column_catalog_rows.append({
                "catalog_name":
                    catalog_name,

                "schema_name":
                    schema_name,

                "table_name":
                    table_name,

                "full_table_name":
                    full_table_name,

                "ordinal_position":
                    ordinal_position,

                "column_name":
                    column_name,

                "data_type":
                    string_or_none(
                        data_type
                    ),

                "column_comment":
                    string_or_none(
                        column_comment
                    ),

                "rcaip_roles":
                    ", ".join(
                        column_roles
                    ),

                "is_asset_identifier":
                    (
                        "Y"
                        if "asset_identifier" in column_roles
                        else "N"
                    ),

                "is_user_identifier":
                    (
                        "Y"
                        if "user_identifier" in column_roles
                        else "N"
                    ),

                "is_timestamp":
                    (
                        "Y"
                        if "timestamp" in column_roles
                        else "N"
                    ),

                "is_post_mapping":
                    (
                        "Y"
                        if "post_mapping" in column_roles
                        else "N"
                    ),

                "is_security_control":
                    (
                        "Y"
                        if "security_control" in column_roles
                        else "N"
                    ),

                "is_software":
                    (
                        "Y"
                        if "software" in column_roles
                        else "N"
                    ),

                "is_vulnerability":
                    (
                        "Y"
                        if "vulnerability" in column_roles
                        else "N"
                    ),

                "is_network_configuration":
                    (
                        "Y"
                        if "network_configuration" in column_roles
                        else "N"
                    ),

                "is_mailbox_exchange":
                    (
                        "Y"
                        if "mailbox_exchange" in column_roles
                        else "N"
                    ),

                "is_third_party_special_purpose":
                    (
                        "Y"
                        if (
                            "third_party_special_purpose"
                            in
                            column_roles
                        )
                        else "N"
                    ),

                "contains_sensitive_identifier":
                    contains_sensitive_identifier
            })

    except Exception as column_error:

        error_rows.append({
            "discovery_stage":
                "COLUMN_DISCOVERY",

            "catalog_name":
                catalog_name,

            "schema_name":
                schema_name,

            "table_name":
                table_name,

            "full_table_name":
                full_table_name,

            "error_message":
                str(column_error)
        })


column_catalog_schema = T.StructType([
    T.StructField("catalog_name", T.StringType(), False),
    T.StructField("schema_name", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("full_table_name", T.StringType(), False),
    T.StructField("ordinal_position", T.IntegerType(), False),
    T.StructField("column_name", T.StringType(), False),
    T.StructField("data_type", T.StringType(), True),
    T.StructField("column_comment", T.StringType(), True),
    T.StructField("rcaip_roles", T.StringType(), False),
    T.StructField("is_asset_identifier", T.StringType(), False),
    T.StructField("is_user_identifier", T.StringType(), False),
    T.StructField("is_timestamp", T.StringType(), False),
    T.StructField("is_post_mapping", T.StringType(), False),
    T.StructField("is_security_control", T.StringType(), False),
    T.StructField("is_software", T.StringType(), False),
    T.StructField("is_vulnerability", T.StringType(), False),
    T.StructField("is_network_configuration", T.StringType(), False),
    T.StructField("is_mailbox_exchange", T.StringType(), False),
    T.StructField(
        "is_third_party_special_purpose",
        T.StringType(),
        False
    ),
    T.StructField(
        "contains_sensitive_identifier",
        T.StringType(),
        False
    )
])

column_catalog = spark.createDataFrame(
    column_catalog_rows,
    schema=column_catalog_schema
)

save_discovery_table(
    column_catalog,
    "column_catalog"
)


# ---------------------------------------------------------------------------
# 10. Classify and score discovered tables
# ---------------------------------------------------------------------------

table_catalog_rows = []

for table_inventory_row in table_inventory_rows:

    full_table_name = table_inventory_row[
        "full_table_name"
    ]

    table_columns = columns_by_table.get(
        full_table_name,
        []
    )

    table_roles = roles_by_table.get(
        full_table_name,
        {
            role_name: []
            for role_name
            in COLUMN_ROLE_PATTERNS.keys()
        }
    )

    table_classification = classify_table(
        table_inventory_row[
            "catalog_name"
        ],
        table_inventory_row[
            "schema_name"
        ],
        table_inventory_row[
            "table_name"
        ],
        table_columns
    )

    candidate_score = int(
        table_classification[
            "primary_domain_score"
        ]
    )

    if table_roles[
        "asset_identifier"
    ]:
        candidate_score += 4

    if table_roles[
        "user_identifier"
    ]:
        candidate_score += 4

    if table_roles[
        "timestamp"
    ]:
        candidate_score += 2

    if table_roles[
        "post_mapping"
    ]:
        candidate_score += 5

    if table_roles[
        "security_control"
    ]:
        candidate_score += 4

    if table_roles[
        "software"
    ]:
        candidate_score += 3

    if table_roles[
        "vulnerability"
    ]:
        candidate_score += 4

    if table_roles[
        "network_configuration"
    ]:
        candidate_score += 3

    if table_roles[
        "mailbox_exchange"
    ]:
        candidate_score += 3

    if table_roles[
        "third_party_special_purpose"
    ]:
        candidate_score += 2

    candidate_score += min(
        len(
            table_columns
        ) // 25,
        4
    )

    if candidate_score >= 15:

        candidate_priority = "High"

    elif candidate_score >= 8:

        candidate_priority = "Medium"

    else:

        candidate_priority = "Low"

    table_catalog_rows.append({
        **table_inventory_row,

        "column_count":
            len(
                table_columns
            ),

        "primary_domain":
            table_classification[
                "primary_domain"
            ],

        "primary_domain_score":
            table_classification[
                "primary_domain_score"
            ],

        "secondary_domain":
            table_classification[
                "secondary_domain"
            ],

        "secondary_domain_score":
            table_classification[
                "secondary_domain_score"
            ],

        "candidate_score":
            candidate_score,

        "candidate_priority":
            candidate_priority,

        "asset_identifier_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "asset_identifier"
                        ]
                    )
                )
            ),

        "user_identifier_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "user_identifier"
                        ]
                    )
                )
            ),

        "timestamp_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "timestamp"
                        ]
                    )
                )
            ),

        "post_mapping_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "post_mapping"
                        ]
                    )
                )
            ),

        "security_control_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "security_control"
                        ]
                    )
                )
            ),

        "software_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "software"
                        ]
                    )
                )
            ),

        "vulnerability_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "vulnerability"
                        ]
                    )
                )
            ),

        "network_configuration_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "network_configuration"
                        ]
                    )
                )
            ),

        "mailbox_exchange_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "mailbox_exchange"
                        ]
                    )
                )
            ),

        "third_party_columns":
            ", ".join(
                sorted(
                    set(
                        table_roles[
                            "third_party_special_purpose"
                        ]
                    )
                )
            ),

        "recommended_work_package":
            table_classification[
                "recommended_work_package"
            ],

        "recommended_rcaip_use":
            table_classification[
                "recommended_rcaip_use"
            ]
    })


table_catalog_schema = T.StructType([
    *table_inventory_schema.fields,

    T.StructField(
        "column_count",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "primary_domain",
        T.StringType(),
        False
    ),

    T.StructField(
        "primary_domain_score",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "secondary_domain",
        T.StringType(),
        True
    ),

    T.StructField(
        "secondary_domain_score",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "candidate_score",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "candidate_priority",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_identifier_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "user_identifier_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "timestamp_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "post_mapping_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "security_control_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "software_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "vulnerability_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "network_configuration_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "mailbox_exchange_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "third_party_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "recommended_work_package",
        T.StringType(),
        False
    ),

    T.StructField(
        "recommended_rcaip_use",
        T.StringType(),
        False
    )
])

table_catalog = spark.createDataFrame(
    table_catalog_rows,
    schema=table_catalog_schema
)

save_discovery_table(
    table_catalog,
    "table_catalog"
)


# ---------------------------------------------------------------------------
# 11. Candidate sources, domain summary, and Post-mapping candidates
# ---------------------------------------------------------------------------

candidate_sources = (
    table_catalog

    .filter(
        F.col(
            "candidate_score"
        )
        >=
        F.lit(
            8
        )
    )

    .orderBy(
        F.col(
            "candidate_score"
        ).desc(),
        "full_table_name"
    )
)

save_discovery_table(
    candidate_sources,
    "candidate_sources"
)


domain_summary = (
    candidate_sources

    .groupBy(
        "recommended_work_package",
        "primary_domain",
        "candidate_priority"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "table_count"
        ),

        F.max(
            "candidate_score"
        ).alias(
            "maximum_candidate_score"
        ),

        F.concat_ws(
            " | ",
            F.slice(
                F.sort_array(
                    F.collect_set(
                        "full_table_name"
                    )
                ),
                1,
                25
            )
        ).alias(
            "candidate_tables_first_25"
        )
    )

    .orderBy(
        "recommended_work_package",
        "primary_domain",
        F.col(
            "maximum_candidate_score"
        ).desc()
    )
)

save_discovery_table(
    domain_summary,
    "domain_summary"
)


post_mapping_candidates = (
    candidate_sources

    .filter(
        F.length(
            F.trim(
                F.col(
                    "post_mapping_columns"
                )
            )
        )
        >
        F.lit(
            0
        )
    )

    .select(
        "full_table_name",
        "primary_domain",
        "candidate_priority",
        "candidate_score",
        "post_mapping_columns",
        "asset_identifier_columns",
        "user_identifier_columns",
        "timestamp_columns",
        "recommended_work_package",
        "recommended_rcaip_use"
    )

    .orderBy(
        F.col(
            "candidate_score"
        ).desc(),
        "full_table_name"
    )
)

save_discovery_table(
    post_mapping_candidates,
    "post_mapping_candidates"
)


# ---------------------------------------------------------------------------
# 12. Metadata-only AD core-field summary
#
# Deeper row counts, duplicate analysis, membership expansion, and key-overlap
# analysis belong in IP-004. Discovery 001 identifies source contracts only.
# ---------------------------------------------------------------------------

ad_source_role_map = {}

configured_ad_sources = {
    "AD User and General Object Source":
        RCAIP_RESOLVED_SOURCE_TABLES.get(
            "ad_users"
        ),

    "AD Computer Source":
        RCAIP_RESOLVED_SOURCE_TABLES.get(
            "ad_computers"
        ),

    "AD Group Source":
        RCAIP_RESOLVED_SOURCE_TABLES.get(
            "ad_groups"
        )
}

for ad_source_role, ad_source_table in (
    configured_ad_sources.items()
):

    if ad_source_table:

        canonical_ad_table_name = canonical_table_name(
            ad_source_table,
            current_catalog
        )

        ad_source_role_map[
            canonical_ad_table_name
        ] = ad_source_role


ad_source_role_schema = T.StructType([
    T.StructField(
        "full_table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "ad_source_role",
        T.StringType(),
        False
    )
])

ad_source_role_df = spark.createDataFrame(
    [
        (
            full_table_name,
            ad_source_role
        )
        for (
            full_table_name,
            ad_source_role
        )
        in ad_source_role_map.items()
    ],
    schema=ad_source_role_schema
)


if ad_source_role_map:

    ad_core_fields = (
        column_catalog

        .join(
            ad_source_role_df,
            on="full_table_name",
            how="inner"
        )

        .withColumn(
            "column_name_lower",
            F.lower(
                F.col(
                    "column_name"
                )
            )
        )

        .withColumn(
            "ad_field_family",

            F.when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(objectguid|objectsid|distinguishedname|"
                    r"samaccountname|userprincipalname|cn|name)$"
                ),
                F.lit(
                    "Core Identity Keys"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(objectclass|objectcategory|samaccounttype|grouptype)$"
                ),
                F.lit(
                    "Object Type"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(whencreated|whenchanged|usncreated|usnchanged|"
                    r"lastlogon|lastlogontimestamp|pwdlastset|"
                    r"accountexpires|lockouttime|badpwdcount|"
                    r"logoncount|useraccountcontrol|isdeleted)$"
                ),
                F.lit(
                    "Lifecycle and Activity"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(distinguishedname|physicaldeliveryofficename|office|"
                    r"location|site|sitename|dnshostname|ipv4address|"
                    r"countrycode|co|l|st)$"
                ),
                F.lit(
                    "Post and Directory Attribution"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(dnshostname|operatingsystem|operatingsystemversion|"
                    r"serviceprincipalname|msds-supportedencryptiontypes)$"
                ),
                F.lit(
                    "Computer Identity and Posture"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(member|memberof|managedby|grouptype|admincount)$"
                ),
                F.lit(
                    "Group and Membership"
                )
            )

            .when(
                F.col(
                    "column_name_lower"
                ).rlike(
                    r"^(admincount|useraccountcontrol|"
                    r"msds-supportedencryptiontypes|sidhistory|"
                    r"primarygroupid)$"
                ),
                F.lit(
                    "Security and Privilege"
                )
            )

            .otherwise(
                F.lit(
                    None
                ).cast(
                    "string"
                )
            )
        )

        .filter(
            F.col(
                "ad_field_family"
            ).isNotNull()
        )
    )

    ad_core_field_summary = (
        ad_core_fields

        .groupBy(
            "ad_source_role",
            "full_table_name",
            "ad_field_family"
        )

        .agg(
            F.countDistinct(
                "column_name"
            ).alias(
                "matching_column_count"
            ),

            F.concat_ws(
                ", ",
                F.slice(
                    F.sort_array(
                        F.collect_set(
                            "column_name"
                        )
                    ),
                    1,
                    50
                )
            ).alias(
                "matching_columns_first_50"
            )
        )

        .orderBy(
            "ad_source_role",
            "full_table_name",
            "ad_field_family"
        )
    )

else:

    ad_core_field_summary_schema = T.StructType([
        T.StructField(
            "ad_source_role",
            T.StringType(),
            False
        ),

        T.StructField(
            "full_table_name",
            T.StringType(),
            False
        ),

        T.StructField(
            "ad_field_family",
            T.StringType(),
            False
        ),

        T.StructField(
            "matching_column_count",
            T.LongType(),
            False
        ),

        T.StructField(
            "matching_columns_first_50",
            T.StringType(),
            False
        )
    ])

    ad_core_field_summary = spark.createDataFrame(
        [],
        schema=ad_core_field_summary_schema
    )


save_discovery_table(
    ad_core_field_summary,
    "ad_core_field_summary"
)


# ---------------------------------------------------------------------------
# 13. Save discovery errors
# ---------------------------------------------------------------------------

discovery_error_schema = T.StructType([
    T.StructField(
        "discovery_stage",
        T.StringType(),
        False
    ),

    T.StructField(
        "catalog_name",
        T.StringType(),
        True
    ),

    T.StructField(
        "schema_name",
        T.StringType(),
        True
    ),

    T.StructField(
        "table_name",
        T.StringType(),
        True
    ),

    T.StructField(
        "full_table_name",
        T.StringType(),
        True
    ),

    T.StructField(
        "error_message",
        T.StringType(),
        False
    )
])

discovery_errors = spark.createDataFrame(
    error_rows,
    schema=discovery_error_schema
)

save_discovery_table(
    discovery_errors,
    "errors"
)


# ---------------------------------------------------------------------------
# 14. Final verification and run certificate
# ---------------------------------------------------------------------------

selected_schema_count = len(
    selected_schema_rows
)

discovered_table_count = table_catalog.count()
discovered_column_count = column_catalog.count()
candidate_source_count = candidate_sources.count()
post_mapping_candidate_count = post_mapping_candidates.count()
ad_source_contract_count = len(
    ad_source_role_map
)
discovery_error_count = discovery_errors.count()

output_table_names = list(
    RCAIP_DISCOVERY_OUTPUT_TABLES.values()
)

# D0021 / Post-agnostic output-isolation guard.
#
# Every Discovery 001 durable output must resolve inside the CURRENT runtime
# Post namespace.  Do not special-case any prior Post name: a table either
# belongs to the active Post namespace or it is cross-Post leakage.
normalized_results_namespace = (
    str(RCAIP_RESULTS_NAMESPACE)
    .strip()
    .strip("`")
    .rstrip(".")
    .lower()
)

normalized_runtime_post_slug = (
    str(RCAIP_POST_SLUG)
    .strip()
    .strip("`")
    .lower()
)

if not normalized_results_namespace:
    raise RuntimeError(
        "Discovery 001 cannot certify output isolation because "
        "RCAIP_RESULTS_NAMESPACE is blank."
    )

if not normalized_runtime_post_slug:
    raise RuntimeError(
        "Discovery 001 cannot certify output isolation because "
        "RCAIP_POST_SLUG is blank."
    )

expected_post_table_prefix = (
    f"{normalized_results_namespace}."
    f"{normalized_runtime_post_slug}_"
)

cross_post_output_leakage = [
    output_table_name
    for output_table_name
    in output_table_names
    if not (
        str(output_table_name)
        .strip()
        .replace("`", "")
        .lower()
        .startswith(
            expected_post_table_prefix
        )
    )
]

missing_output_tables_before_certificate = [
    output_table_name
    for output_key, output_table_name
    in RCAIP_DISCOVERY_OUTPUT_TABLES.items()
    if (
        output_key != "run_certificate"
        and
        not spark.catalog.tableExists(
            output_table_name
        )
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
        "Certified run mode",
        "DISCOVERY",
        str(
            RCAIP_RUN_MODE
        ),
        str(
            RCAIP_RUN_MODE
        ).upper()
        ==
        "DISCOVERY"
    ),

    (
        "Runtime Post slug",
        RCAIP_POST_SLUG,
        RCAIP_POST_SLUG,
        bool(
            RCAIP_POST_SLUG
        )
    ),

    (
        "Selected schemas",
        "At least one",
        str(
            selected_schema_count
        ),
        selected_schema_count > 0
    ),

    (
        "Discovered tables",
        "At least one",
        str(
            discovered_table_count
        ),
        discovered_table_count > 0
    ),

    (
        "Discovered columns",
        "At least one",
        str(
            discovered_column_count
        ),
        discovered_column_count > 0
    ),

    (
        "Candidate sources",
        "At least one",
        str(
            candidate_source_count
        ),
        candidate_source_count > 0
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
        ) == 0
    ),

    (
        "Missing durable outputs",
        "0",
        str(
            len(
                missing_output_tables_before_certificate
            )
        ),
        len(
            missing_output_tables_before_certificate
        ) == 0
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

discovery_run_certificate = spark.createDataFrame(
    certification_checks,
    schema=certificate_schema
)

failed_discovery_checks = [
    certification_check
    for certification_check
    in certification_checks
    if not certification_check[
        3
    ]
]

RCAIP_DISCOVERY_001_CERTIFIED = (
    not failed_discovery_checks
)

save_discovery_table(
    discovery_run_certificate,
    "run_certificate"
)


print("=" * 100)
print("RCAIP DISCOVERY 001 - FINAL RUNTIME CONTRACT")
print("=" * 100)
print(
    "Status:",
    (
        "PASS"
        if RCAIP_DISCOVERY_001_CERTIFIED
        else "FAIL"
    )
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
print("Results namespace:", RCAIP_RESULTS_NAMESPACE)
print("Selected schemas:", selected_schema_count)
print("Discovered tables:", discovered_table_count)
print("Discovered columns:", discovered_column_count)
print("Candidate sources:", candidate_source_count)
print(
    "Post-mapping candidates:",
    post_mapping_candidate_count
)
print(
    "Configured AD source contracts:",
    ad_source_contract_count
)
print("Discovery errors recorded:", discovery_error_count)
print(
    "Durable output tables:",
    len(
        RCAIP_DISCOVERY_OUTPUT_TABLES
    )
)
print(
    "Cross-Post output leakage:",
    len(
        cross_post_output_leakage
    )
)
print("Automatic Post assignments performed: 0")
print("Automatic findings created: 0")
print("Automatic tickets created: 0")
print("Automatic remediation performed: 0")
print("=" * 100)


display(
    discovery_run_certificate
    .orderBy(
        F.col(
            "check_passed"
        ).asc(),
        "certification_check"
    )
)

display(
    domain_summary
)

display(
    candidate_sources
    .select(
        "full_table_name",
        "primary_domain",
        "candidate_priority",
        "candidate_score",
        "post_mapping_columns",
        "recommended_work_package"
    )
    .limit(
        100
    )
)

display(
    ad_core_field_summary
)


if failed_discovery_checks:

    raise RuntimeError(
        "Discovery 001 certification failed: "
        +
        " | ".join(
            failed_check[
                0
            ]
            for failed_check
            in failed_discovery_checks
        )
    )