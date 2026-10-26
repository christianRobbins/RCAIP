# Databricks notebook source
# MAGIC %run ./RCAIP_Config_000_Post_Run_Config

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# Table Registry Foundation
#
# Purpose:
#   Load the authoritative Post/run configuration and define the shared
#   registry primitives used by every RCAIP analytical notebook.
#
# Enterprise design rule:
#   This notebook contains no hard-coded Post name, Post abbreviation,
#   country, assessment date, run directory, or Post-specific table prefix.
#   All Post-specific values are supplied by Config 000.
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Imports
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame


# ---------------------------------------------------------------------------
# 2. Validate the Config 000 runtime contract
# ---------------------------------------------------------------------------

required_configuration_objects = [
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RESULTS_NAMESPACE",
    "rcaip_table"
]


missing_configuration_objects = [
    object_name

    for object_name
    in required_configuration_objects

    if object_name not in globals()
]


if missing_configuration_objects:

    raise RuntimeError(
        "Config 000 did not supply the complete RCAIP runtime contract. "
        "Missing objects: "
        +
        " | ".join(
            missing_configuration_objects
        )
    )


configured_post_name = str(
    RCAIP_POST_NAME
).strip()


configured_post_slug = str(
    RCAIP_POST_SLUG
).strip()


configured_results_namespace = str(
    RCAIP_RESULTS_NAMESPACE
).strip()


if not configured_post_name:

    raise RuntimeError(
        "RCAIP_POST_NAME is empty."
    )


if not configured_post_slug:

    raise RuntimeError(
        "RCAIP_POST_SLUG is empty."
    )


if not configured_results_namespace:

    raise RuntimeError(
        "RCAIP_RESULTS_NAMESPACE is empty."
    )


if not callable(
    rcaip_table
):

    raise TypeError(
        "rcaip_table must be callable."
    )


# ---------------------------------------------------------------------------
# 3. Physical table-name helpers
# ---------------------------------------------------------------------------

def _rcaip_shared_table(
    table_name
):
    """
    Build an RCAIP-owned enterprise table name.
    """

    normalized_table_name = str(
        table_name
    ).strip()


    if not normalized_table_name:

        raise ValueError(
            "Enterprise table_name cannot be empty."
        )


    return (
        f"{configured_results_namespace}."
        f"{normalized_table_name}"
    )


def _rcaip_post_table(
    table_suffix
):
    """
    Build an RCAIP-owned Post-scoped table name through Config 000.
    """

    normalized_table_suffix = str(
        table_suffix
    ).strip()


    if not normalized_table_suffix:

        raise ValueError(
            "Post table_suffix cannot be empty."
        )


    return rcaip_table(
        normalized_table_suffix
    )


def _qualified_table_name_parts(
    physical_table_name
):
    """
    Return normalized parts for schema.table or catalog.schema.table names.
    """

    normalized_table_name = str(
        physical_table_name
    ).strip()


    if not normalized_table_name:

        return []


    return [
        table_name_part
        .strip()
        .strip("`")

        for table_name_part
        in normalized_table_name.split(
            "."
        )
    ]


def validate_enterprise_source_table_name(
    physical_table_name
):
    """
    Validate a source-owned enterprise table name without imposing an RCAIP
    output prefix.
    """

    table_name_parts = _qualified_table_name_parts(
        physical_table_name
    )


    if len(
        table_name_parts
    ) not in [
        2,
        3
    ]:

        return False


    return not any(
        not table_name_part

        for table_name_part
        in table_name_parts
    )


# ---------------------------------------------------------------------------
# 4. Scope model
# ---------------------------------------------------------------------------

RCAIP_ALLOWED_TABLE_SCOPES = {
    "POST_SCOPED",
    "ENTERPRISE_SHARED",
    "ENTERPRISE_MULTI_POST",
    "ENTERPRISE_SOURCE"
}


RCAIP_SCOPE_DESCRIPTIONS = {
    "POST_SCOPED": (
        "RCAIP-owned durable table generated separately for the configured "
        "Post."
    ),

    "ENTERPRISE_SHARED": (
        "RCAIP-owned enterprise table shared by every Post."
    ),

    "ENTERPRISE_MULTI_POST": (
        "RCAIP-owned enterprise table containing records from multiple Posts."
    ),

    "ENTERPRISE_SOURCE": (
        "Source-owned enterprise table registered for RCAIP consumption but "
        "not renamed by RCAIP."
    )
}


# ---------------------------------------------------------------------------
# 5. Architectural pipeline model
# ---------------------------------------------------------------------------

RCAIP_PIPELINE_MODEL = [
    {
        "stage_order": 0,
        "stage_name": "CONFIGURATION",
        "architectural_layer": "Configuration",
        "purpose": (
            "Define Post identity, run identity, input locations, output "
            "locations, and shared RCAIP settings."
        )
    },

    {
        "stage_order": 10,
        "stage_name": "ENTERPRISE_KNOWLEDGE_BASE",
        "architectural_layer": "Enterprise Knowledge Base",
        "purpose": (
            "Provide shared rules, terminology, device patterns, tool "
            "expectations, CSA mappings, and source interpretation."
        )
    },

    {
        "stage_order": 20,
        "stage_name": "SOURCE_COLLECTION",
        "architectural_layer": "Source Collection",
        "purpose": (
            "Register enterprise sources and normalize assessment evidence."
        )
    },

    {
        "stage_order": 30,
        "stage_name": "ASSET_PLANE",
        "architectural_layer": "Asset Plane",
        "purpose": (
            "Create durable asset identity, source visibility, lifecycle, "
            "reconciliation, and confidence records."
        )
    },

    {
        "stage_order": 40,
        "stage_name": "ASSET_PLANE_ANALYTICS",
        "architectural_layer": "Asset Plane Analytics",
        "purpose": (
            "Apply exposure, boundary, coverage, lifecycle, and control "
            "analysis to the Asset Plane."
        )
    },

    {
        "stage_order": 50,
        "stage_name": "ACTIVE_DIRECTORY_ANALYSIS",
        "architectural_layer": "Active Directory Analysis",
        "purpose": (
            "Add computer, user, group, ownership, membership, lifecycle, "
            "and privilege analysis without replacing the Asset Plane."
        )
    },

    {
        "stage_order": 60,
        "stage_name": "OPERATIONAL_VIEW",
        "architectural_layer": "Operational View",
        "purpose": (
            "Connect operational ownership, office, disposition, local "
            "knowledge, and action queues to durable asset identities."
        )
    },

    {
        "stage_order": 70,
        "stage_name": "REPORTING_AND_EXPORT",
        "architectural_layer": "Reporting and Export",
        "purpose": (
            "Produce RCSO workbooks, CSA evidence, technical appendices, "
            "summaries, and prioritized action packages."
        )
    }
]


rcaip_pipeline_model_schema = T.StructType([
    T.StructField(
        "stage_order",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "stage_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "architectural_layer",
        T.StringType(),
        False
    ),

    T.StructField(
        "purpose",
        T.StringType(),
        False
    )
])


RCAIP_PIPELINE_MODEL_DF = (
    spark.createDataFrame(
        [
            (
                stage[
                    "stage_order"
                ],
                stage[
                    "stage_name"
                ],
                stage[
                    "architectural_layer"
                ],
                stage[
                    "purpose"
                ]
            )

            for stage
            in RCAIP_PIPELINE_MODEL
        ],
        schema=rcaip_pipeline_model_schema
    )

    .orderBy(
        "stage_order"
    )
)


print(
    "RCAIP Config 001 foundation: PASS"
)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# Authoritative Table Registry Entries
#
# Rules:
#   - Every entry is declared before lookup dictionaries are built.
#   - Post-scoped names are generated only through _rcaip_post_table().
#   - RCAIP enterprise products are generated through _rcaip_shared_table().
#   - Source-owned enterprise tables use ENTERPRISE_SOURCE.
#   - No Post name or Post abbreviation is hard-coded in this cell.
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Registry entry constructor
# ---------------------------------------------------------------------------

def _registry_entry(
    logical_name,
    physical_table_name,
    scope,
    layer_order,
    architectural_layer,
    product_family,
    description
):
    """
    Create one normalized table-registry entry.
    """

    normalized_logical_name = str(
        logical_name
    ).strip().upper()


    normalized_physical_table_name = str(
        physical_table_name
    ).strip()


    normalized_scope = str(
        scope
    ).strip().upper()


    normalized_architectural_layer = str(
        architectural_layer
    ).strip()


    normalized_product_family = str(
        product_family
    ).strip()


    normalized_description = str(
        description
    ).strip()


    if not normalized_logical_name:

        raise ValueError(
            "logical_name cannot be empty."
        )


    if not normalized_physical_table_name:

        raise ValueError(
            "physical_table_name cannot be empty."
        )


    if normalized_scope not in RCAIP_ALLOWED_TABLE_SCOPES:

        raise ValueError(
            "Unsupported RCAIP table scope: "
            +
            normalized_scope
        )


    if not normalized_architectural_layer:

        raise ValueError(
            "architectural_layer cannot be empty."
        )


    if not normalized_product_family:

        raise ValueError(
            "product_family cannot be empty."
        )


    if not normalized_description:

        raise ValueError(
            "description cannot be empty."
        )


    return {
        "logical_name": normalized_logical_name,
        "physical_table_name": normalized_physical_table_name,
        "scope": normalized_scope,
        "layer_order": int(
            layer_order
        ),
        "architectural_layer": normalized_architectural_layer,
        "product_family": normalized_product_family,
        "description": normalized_description
    }


_registry_entries = []


def _add_registry_entry(
    logical_name,
    physical_table_name,
    scope,
    layer_order,
    architectural_layer,
    product_family,
    description
):
    """
    Append one normalized table contract to the working registry.
    """

    _registry_entries.append(
        _registry_entry(
            logical_name=logical_name,
            physical_table_name=physical_table_name,
            scope=scope,
            layer_order=layer_order,
            architectural_layer=architectural_layer,
            product_family=product_family,
            description=description
        )
    )


# ---------------------------------------------------------------------------
# 2. Enterprise Knowledge Base - ENTERPRISE_SHARED
# ---------------------------------------------------------------------------

_add_registry_entry(
    "KB_ASSET_PATTERNS_V1",
    _rcaip_shared_table(
        "rcaip_kb_asset_patterns"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 1 asset naming and classification patterns."
)

_add_registry_entry(
    "KB_ASSET_PATTERNS_V2",
    _rcaip_shared_table(
        "rcaip_kb_asset_patterns_v2"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 2 asset naming and classification patterns."
)

_add_registry_entry(
    "KB_CSA_QUESTION_MAP",
    _rcaip_shared_table(
        "rcaip_kb_csa_question_map"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Maps RCAIP evidence and findings to CSA questions."
)

_add_registry_entry(
    "KB_DEVICE_PROFILES",
    _rcaip_shared_table(
        "rcaip_kb_device_profiles"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Enterprise device profiles and interpretation guidance."
)

_add_registry_entry(
    "KB_DEVICE_TOOL_EXPECTATIONS",
    _rcaip_shared_table(
        "rcaip_kb_device_tool_expectations"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Expected security and management tools by device profile."
)

_add_registry_entry(
    "KB_ISSO_SUBNET_LOOKUP",
    _rcaip_shared_table(
        "rcaip_kb_isso_subnet_lookup"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Enterprise subnet and Post interpretation lookup."
)

_add_registry_entry(
    "KB_NAMING_TOKEN_RULES",
    _rcaip_shared_table(
        "rcaip_kb_naming_token_rules"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Naming-token rules used to interpret enterprise device names."
)

_add_registry_entry(
    "KB_OBSERVATION_RULES_V1",
    _rcaip_shared_table(
        "rcaip_kb_observation_rules"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 1 observed-asset interpretation and action rules."
)

_add_registry_entry(
    "KB_OBSERVATION_RULES_V2",
    _rcaip_shared_table(
        "rcaip_kb_observation_rules_v2"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 2 observed-asset interpretation and action rules."
)

_add_registry_entry(
    "KB_RULE_REGISTRY_V2_2",
    _rcaip_shared_table(
        "rcaip_kb_rule_registry_v2_2"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Executable RCAIP v2.2 rule registry with triggers, exclusions, routing, precedence, and guardrails."
)

_add_registry_entry(
    "KB_PORT_RULES",
    _rcaip_shared_table(
        "rcaip_kb_port_rules"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Port, service, risk-theme, and CSA relevance rules."
)

_add_registry_entry(
    "KB_RELEASE_MANIFEST",
    _rcaip_shared_table(
        "rcaip_kb_release_manifest"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Knowledge Base release and validation manifest."
)

_add_registry_entry(
    "KB_REQUIRED_TOOLS_V1",
    _rcaip_shared_table(
        "rcaip_kb_required_tools"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 1 required security and management tool rules."
)

_add_registry_entry(
    "KB_REQUIRED_TOOLS_V2",
    _rcaip_shared_table(
        "rcaip_kb_required_tools_v2"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Version 2 required security and management tool rules."
)

_add_registry_entry(
    "KB_SOURCE_REGISTRY",
    _rcaip_shared_table(
        "rcaip_kb_source_registry"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Registry of recognized RCAIP evidence and management sources."
)

_add_registry_entry(
    "KB_TERMINOLOGY_RULES",
    _rcaip_shared_table(
        "rcaip_kb_terminology_rules"
    ),
    "ENTERPRISE_SHARED",
    10,
    "Enterprise Knowledge Base",
    "Knowledge Base",
    "Enterprise terminology, display-name, and wording standards."
)


# ---------------------------------------------------------------------------
# 3. Source Collection - ENTERPRISE_SOURCE
# ---------------------------------------------------------------------------

_add_registry_entry(
    "AD_COMPUTERS_SOURCE",
    "example_sources.ad_computers_1",
    "ENTERPRISE_SOURCE",
    20,
    "Source Collection",
    "Active Directory",
    "Enterprise Active Directory computer-object source for IP-004."
)

_add_registry_entry(
    "AD_GROUPS_SOURCE",
    "example_sources.ad_groups_1",
    "ENTERPRISE_SOURCE",
    20,
    "Source Collection",
    "Active Directory",
    "Enterprise Active Directory group-object and membership source for IP-004."
)

_add_registry_entry(
    "AD_USERS_SOURCE",
    "example_sources.ad_users_1",
    "ENTERPRISE_SOURCE",
    20,
    "Source Collection",
    "Active Directory",
    "Enterprise Active Directory general and user-object source for IP-004."
)


# ---------------------------------------------------------------------------
# 4. Source Collection - POST_SCOPED
# ---------------------------------------------------------------------------

_add_registry_entry(
    "CMDB_NORM",
    _rcaip_post_table(
        "cmdb_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "CMDB",
    "Normalized CMDB asset records used by the RCAIP Asset Plane."
)

_add_registry_entry(
    "UMBRELLA_PUBLIC_IP_VALIDATION",
    _rcaip_post_table(
        "umbrella_public_ip_validation"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Cisco Umbrella",
    "Public-IP validation evidence derived from Cisco Umbrella observations."
)

_add_registry_entry(
    "INTUNE_NORM",
    _rcaip_post_table(
        "intune_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Intune",
    "Normalized Microsoft Intune device-management observations."
)

_add_registry_entry(
    "MDE_NORM",
    _rcaip_post_table(
        "mde_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Microsoft Defender for Endpoint",
    "Normalized Microsoft Defender for Endpoint device observations."
)

_add_registry_entry(
    "NMAP_NORM",
    _rcaip_post_table(
        "nmap_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Nmap",
    "Normalized Nmap asset-level observations."
)

_add_registry_entry(
    "NMAP_NORM_WITH_PROVENANCE",
    _rcaip_post_table(
        "nmap_norm_with_provenance"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Nmap",
    "Normalized Nmap asset observations enriched with provenance."
)

_add_registry_entry(
    "NMAP_PORTS_ALL",
    _rcaip_post_table(
        "nmap_ports_all"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Nmap",
    "Normalized open-port and service observations."
)

_add_registry_entry(
    "NMAP_PORTS_ALL_WITH_PROVENANCE",
    _rcaip_post_table(
        "nmap_ports_all_with_provenance"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Nmap",
    "Normalized Nmap port observations enriched with scan provenance."
)

_add_registry_entry(
    "NMAP_SCAN_PROVENANCE",
    _rcaip_post_table(
        "nmap_scan_provenance"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Nmap",
    "One record per Nmap XML scan with collection provenance."
)

_add_registry_entry(
    "QUALYS_NORM",
    _rcaip_post_table(
        "qualys_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Qualys",
    "Normalized Qualys asset and vulnerability-management observations."
)

_add_registry_entry(
    "TENABLE_NORM",
    _rcaip_post_table(
        "tenable_norm"
    ),
    "POST_SCOPED",
    20,
    "Source Collection",
    "Tenable",
    "Normalized Tenable asset and vulnerability-management observations."
)


# ---------------------------------------------------------------------------
# 5. Asset Plane - POST_SCOPED
# ---------------------------------------------------------------------------

_add_registry_entry(
    "OBSERVED_ASSETS",
    _rcaip_post_table(
        "observed_assets"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Asset Identity",
    "Source-level observed asset records with lifecycle interpretation."
)

_add_registry_entry(
    "OBSERVED_ASSETS_ROLLUP",
    _rcaip_post_table(
        "observed_assets_rollup"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Asset Identity",
    "One lifecycle-aware rollup per normalized observed asset identity."
)

_add_registry_entry(
    "SOURCE_LIFECYCLE_COUNTS",
    _rcaip_post_table(
        "source_lifecycle_counts"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Lifecycle",
    "Summary of source lifecycle and observation states."
)

_add_registry_entry(
    "STALE_OBSERVED_ASSETS",
    _rcaip_post_table(
        "stale_observed_assets"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Lifecycle",
    "Observed assets supported only by stale or historical evidence."
)

_add_registry_entry(
    "ASSET_RECONCILIATION",
    _rcaip_post_table(
        "asset_reconciliation"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Reconciliation",
    "Durable cross-source Asset Plane reconciliation."
)

_add_registry_entry(
    "ASSET_RECONCILIATION_SCORED",
    _rcaip_post_table(
        "asset_reconciliation_scored"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Reconciliation",
    "Lifecycle-aware and confidence-scored Asset Plane."
)

_add_registry_entry(
    "OBSERVED_NOT_CMDB",
    _rcaip_post_table(
        "observed_not_cmdb"
    ),
    "POST_SCOPED",
    30,
    "Asset Plane",
    "Reconciliation",
    "Observed assets not represented by a reconciled CMDB identity."
)


# ---------------------------------------------------------------------------
# 6. Asset Plane Analytics - POST_SCOPED
# ---------------------------------------------------------------------------

_add_registry_entry(
    "IP001_DEVICE_PORT_PRIORITIZED",
    _rcaip_post_table(
        "ip001_device_port_prioritized"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Prioritized device and port review records."
)

_add_registry_entry(
    "IP001_DEVICE_PORT_PRIORITIZED_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_device_port_prioritized_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Prioritized device-port records enriched with scan provenance."
)

_add_registry_entry(
    "IP001_DEVICE_PORT_ROLLUP",
    _rcaip_post_table(
        "ip001_device_port_rollup"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "One summarized open-port review record per device."
)

_add_registry_entry(
    "IP001_DEVICE_PORT_ROLLUP_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_device_port_rollup_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Device-port rollup enriched with scan provenance."
)

_add_registry_entry(
    "IP001_NEN_PUBLIC_EXPOSURE",
    _rcaip_post_table(
        "ip001_nen_public_exposure"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Potential NEN or public exposure review records."
)

_add_registry_entry(
    "IP001_NEN_PUBLIC_EXPOSURE_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_nen_public_exposure_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Potential exposure records enriched with scan provenance."
)

_add_registry_entry(
    "IP001_OPEN_PORT_REVIEW",
    _rcaip_post_table(
        "ip001_open_port_review"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Detailed open-port and service review records."
)

_add_registry_entry(
    "IP001_OPEN_PORT_REVIEW_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_open_port_review_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Detailed open-port review enriched with scan provenance."
)

_add_registry_entry(
    "IP001_PORT_EMAIL_SUMMARY",
    _rcaip_post_table(
        "ip001_port_email_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Condensed IP-001 summary suitable for stakeholder communication."
)

_add_registry_entry(
    "IP001_PORT_SUMMARY",
    _rcaip_post_table(
        "ip001_port_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Aggregated port and service summary."
)

_add_registry_entry(
    "IP001_POST_ACTION_TABLE",
    _rcaip_post_table(
        "ip001_post_action_table"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Post-facing prioritized IP-001 action table."
)

_add_registry_entry(
    "IP001_POST_ACTION_TABLE_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_post_action_table_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Post-facing action table enriched with scan provenance."
)

_add_registry_entry(
    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW",
    _rcaip_post_table(
        "ip001_q1143_printer_mfd_device_review"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Printer and MFD device review supporting CSA question 1143."
)

_add_registry_entry(
    "IP001_Q1143_PRINTER_MFD_DEVICE_REVIEW_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_q1143_printer_mfd_device_review_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Printer and MFD device review enriched with provenance."
)

_add_registry_entry(
    "IP001_Q1143_PRINTER_MFD_PORTS",
    _rcaip_post_table(
        "ip001_q1143_printer_mfd_ports"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Printer and MFD port evidence supporting CSA question 1143."
)

_add_registry_entry(
    "IP001_Q1143_PRINTER_MFD_PORTS_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_q1143_printer_mfd_ports_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Printer and MFD port evidence enriched with provenance."
)

_add_registry_entry(
    "IP001_Q1173_ENTWIFI_PORTS",
    _rcaip_post_table(
        "ip001_q1173_entwifi_ports"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Enterprise Wi-Fi port evidence supporting CSA question 1173."
)

_add_registry_entry(
    "IP001_Q1173_ENTWIFI_PORTS_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_q1173_entwifi_ports_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Enterprise Wi-Fi evidence enriched with scan provenance."
)

_add_registry_entry(
    "IP001_SCAN_PROVENANCE_SUMMARY",
    _rcaip_post_table(
        "ip001_scan_provenance_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Summary of scan provenance represented in IP-001 products."
)

_add_registry_entry(
    "IP001_NETWORK_HOST_ATTRIBUTION",
    _rcaip_post_table(
        "ip001_network_host_attribution"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    (
        "One durable record per certified Nmap host observation preserving "
        "structural Asset Plane linkage, independent enterprise-asset "
        "attribution state, and time-bound IP review context without "
        "treating Nmap structural linkage as durable device identity."
    ),
)

_add_registry_entry(
    "IP001_TECHNICAL_APPENDIX",
    _rcaip_post_table(
        "ip001_technical_appendix"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Detailed technical appendix supporting the IP-001 analysis."
)

_add_registry_entry(
    "IP001_TECHNICAL_APPENDIX_WITH_PROVENANCE",
    _rcaip_post_table(
        "ip001_technical_appendix_with_provenance"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Technical appendix enriched with scan provenance."
)

_add_registry_entry(
    "IP001_WORK_PACKAGE_SUMMARY",
    _rcaip_post_table(
        "ip001_work_package_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-001",
    "Summary of prioritized IP-001 work packages."
)

_add_registry_entry(
    "IP002_CLASSIFIED_OBSERVED_NOT_CMDB",
    _rcaip_post_table(
        "ip002_classified_observed_not_cmdb"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-002",
    "Knowledge-Base-classified observed-not-CMDB records."
)

_add_registry_entry(
    "IP002_SUMMARY_OBSERVED_NOT_CMDB",
    _rcaip_post_table(
        "ip002_summary_observed_not_cmdb"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-002",
    "Summary of observed-not-CMDB classifications and actions."
)

_add_registry_entry(
    "IP002_TOOL_COVERAGE_REVIEW",
    _rcaip_post_table(
        "ip002_tool_coverage_review"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-002",
    "Review of likely required security-tool coverage gaps for reconciled assets."
)

_add_registry_entry(
    "IP002_TRIAGED_OBSERVED_NOT_CMDB",
    _rcaip_post_table(
        "ip002_triaged_observed_not_cmdb"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-002",
    "Prioritized and owner-directed observed-not-CMDB records."
)

_add_registry_entry(
    "IP003_ASSET_LIFECYCLE_ROLLUP",
    _rcaip_post_table(
        "ip003_asset_lifecycle_rollup"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-003",
    "Asset-level lifecycle rollup across observation sources."
)

_add_registry_entry(
    "IP003_SOURCE_LIFECYCLE_OBSERVATIONS",
    _rcaip_post_table(
        "ip003_source_lifecycle_observations"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-003",
    "Source-level lifecycle observations for each asset identity."
)

_add_registry_entry(
    "IP006_ASSET_FEATURE_MART",
    _rcaip_post_table(
        "ip006_asset_feature_mart"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "One row per certified asset identity with deterministic cohort, applicability, attribution, network, and risk features."
)

_add_registry_entry(
    "IP006_RULE_MATCHES",
    _rcaip_post_table(
        "ip006_rule_matches"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Auditable one-row-per-asset-and-rule evaluation and match records."
)

_add_registry_entry(
    "IP006_RULE_IMPACT_SUMMARY",
    _rcaip_post_table(
        "ip006_rule_impact_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Per-rule evaluation, routing, exclusion, suppression, residual, leakage, and sampling metrics."
)

_add_registry_entry(
    "IP006_COHORT_SUMMARY",
    _rcaip_post_table(
        "ip006_cohort_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Aggregate deterministic cohort signatures, sizes, homogeneity, rarity, and conflict measures without raw identifiers."
)

_add_registry_entry(
    "IP006_TRIAGE_ROUTING",
    _rcaip_post_table(
        "ip006_triage_routing"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Final RCSO, Post, enterprise, boundary, context, anomaly, suppressed, and residual routing decisions."
)

_add_registry_entry(
    "IP006_UNCLASSIFIED_RESIDUAL",
    _rcaip_post_table(
        "ip006_unclassified_residual"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Assets not fully explained by active deterministic rules and therefore eligible for residual anomaly discovery."
)

_add_registry_entry(
    "IP006_CONTROL_ASSURANCE_EVALUATION",
    _rcaip_post_table(
        "ip006_control_assurance_evaluation"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Five-tool shadow evaluation with one auditable asset-control relationship per asset and tool."
)

_add_registry_entry(
    "IP006_CONTROL_ASSURANCE_FEATURE",
    _rcaip_post_table(
        "ip006_control_assurance_feature"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "One row per certified asset with boundary, visibility, per-tool assurance states, and review metrics."
)

_add_registry_entry(
    "IP006_CONTROL_ASSURANCE_REVIEW_SAMPLE",
    _rcaip_post_table(
        "ip006_control_assurance_review_sample"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Deterministic shadow-review sample by tool and adverse assurance state."
)

_add_registry_entry(
    "IP006_CONTROL_ASSURANCE_REVIEW_QUEUE",
    _rcaip_post_table(
        "ip006_control_assurance_review_queue"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Focused shadow review queue for actionable MDE and Intune assurance conditions plus active unresolved-IP identity reconciliation."
)

_add_registry_entry(
    "IP006_CONTROL_ASSURANCE_PROGRAM_SUMMARY",
    _rcaip_post_table(
        "ip006_control_assurance_program_summary"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Aggregated control-assurance, applicability, evidence, and enterprise-dependency summary for RCSO review and workbook production."
)

_add_registry_entry(
    "IP006_MDE_REDISCOVERY_SEARCH_INPUT",
    _rcaip_post_table(
        "ip006_mde_rediscovery_search_input"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Governed one-row-per-original-MDE-missing search authority containing deterministic trusted machine-name evidence, source provenance, search eligibility, and identity limitations."
)

_add_registry_entry(
    "IP006_MDE_REDISCOVERY_ENTITY_EVIDENCE",
    _rcaip_post_table(
        "ip006_mde_rediscovery_entity_evidence"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "Durable one-row-per-asset-and-MDE-entity rediscovery evidence boundary preserving deterministic match provenance, durable identity, temporal evidence, and MDE entity lineage."
)
_add_registry_entry(
    "IP006_MDE_REDISCOVERY_ENRICHMENT",
    _rcaip_post_table(
        "ip006_mde_rediscovery_enrichment"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    "One row per originally MDE-MISSING asset preserving the certified assurance state while adding governed enterprise-MDE rediscovery, attribution, durable-identity lineage, recency, and review interpretation."
)

# ---------------------------------------------------------------------------
# 7. Active Directory Analysis - POST_SCOPED
#
# IP-004 output contract is complete for every save_ip004_table() target in
# RCAIP_IP_004_Identity_and_AD_Hygiene. The three final parity products are
# registered alongside the durable foundation, relationship, summary,
# action, executive, verification, and CSA products that support them.
# ---------------------------------------------------------------------------

_add_registry_entry(
    "IP004_AD_USERS_NORM",
    _rcaip_post_table(
        "ip004_ad_users_norm"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Normalized and deterministically deduplicated Active Directory user and person objects."
)

_add_registry_entry(
    "IP004_AD_COMPUTERS_NORM",
    _rcaip_post_table(
        "ip004_ad_computers_norm"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Normalized and deterministically deduplicated Active Directory computer objects."
)

_add_registry_entry(
    "IP004_AD_GROUPS_NORM",
    _rcaip_post_table(
        "ip004_ad_groups_norm"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Normalized and deterministically deduplicated Active Directory group objects."
)

_add_registry_entry(
    "IP004_AD_OBJECT_DIRECTORY",
    _rcaip_post_table(
        "ip004_ad_object_directory"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Unified Active Directory object directory across normalized users, computers, and groups."
)

_add_registry_entry(
    "IP004_AD_MEMBEROF_BRIDGE",
    _rcaip_post_table(
        "ip004_ad_memberof_bridge"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Resolved object-to-group relationships derived from Active Directory memberOf attributes."
)

_add_registry_entry(
    "IP004_AD_GROUP_MEMBER_BRIDGE",
    _rcaip_post_table(
        "ip004_ad_group_member_bridge"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Resolved group-to-member relationships derived from Active Directory group member attributes."
)

_add_registry_entry(
    "IP004_AD_MANAGEDBY_BRIDGE",
    _rcaip_post_table(
        "ip004_ad_managedby_bridge"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Resolved Active Directory object ownership relationships derived from managedBy attributes."
)

_add_registry_entry(
    "IP004_COMPUTER_ASSET_MAP",
    _rcaip_post_table(
        "ip004_computer_asset_map"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Active Directory computer-to-Asset-Plane reconciliation and Post-attribution evidence."
)

_add_registry_entry(
    "IP004_POST_SCOPE_OBJECTS",
    _rcaip_post_table(
        "ip004_post_scope_objects"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Conservatively scoped Post Active Directory objects with attribution confidence and basis."
)

_add_registry_entry(
    "IP004_PHASE1_SUMMARY",
    _rcaip_post_table(
        "ip004_phase1_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Phase 1 Active Directory normalization, quality, and object-resolution summary."
)

_add_registry_entry(
    "IP004_RELATIONSHIP_SUMMARY",
    _rcaip_post_table(
        "ip004_relationship_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of Active Directory membership and ownership relationship resolution outcomes."
)

_add_registry_entry(
    "IP004_COMPUTER_MAPPING_SUMMARY",
    _rcaip_post_table(
        "ip004_computer_mapping_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of Active Directory computer-to-Asset-Plane mapping outcomes."
)

_add_registry_entry(
    "IP004_POST_ATTRIBUTION_SUMMARY",
    _rcaip_post_table(
        "ip004_post_attribution_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of Post attribution outcomes across Active Directory object types."
)

_add_registry_entry(
    "IP004_PHASE2_PARAMETERS",
    _rcaip_post_table(
        "ip004_phase2_parameters"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Durable IP-004 Phase 2 analytical thresholds, definitions, and review parameters."
)

_add_registry_entry(
    "IP004_USER_HYGIENE_REVIEW",
    _rcaip_post_table(
        "ip004_user_hygiene_review"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "User account lifecycle, privilege, membership, and hygiene review."
)

_add_registry_entry(
    "IP004_COMPUTER_HYGIENE_REVIEW",
    _rcaip_post_table(
        "ip004_computer_hygiene_review"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Computer-object lifecycle, ownership, scope, and hygiene review."
)

_add_registry_entry(
    "IP004_GROUP_MEMBER_STATUS",
    _rcaip_post_table(
        "ip004_group_member_status"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Enriched group-member status, lifecycle, scope, and relationship-resolution evidence."
)

_add_registry_entry(
    "IP004_GROUP_HYGIENE_PROFILE",
    _rcaip_post_table(
        "ip004_group_hygiene_profile"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Group ownership, membership, privilege, and hygiene profile."
)

_add_registry_entry(
    "IP004_PRIVILEGED_GROUP_HIERARCHY",
    _rcaip_post_table(
        "ip004_privileged_group_hierarchy"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Nested privileged-group hierarchy and relationship paths requiring review."
)

_add_registry_entry(
    "IP004_PRIVILEGED_ACCESS_REVIEW",
    _rcaip_post_table(
        "ip004_privileged_access_review"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Resolved privileged-access exposure across direct and nested group relationships."
)

_add_registry_entry(
    "IP004_MEMBERSHIP_INTEGRITY_REVIEW",
    _rcaip_post_table(
        "ip004_membership_integrity_review"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Membership integrity, asymmetry, unresolved-edge, and source-consistency review."
)

_add_registry_entry(
    "IP004_MEMBERSHIP_COMPARISON_SUMMARY",
    _rcaip_post_table(
        "ip004_membership_comparison_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of memberOf and group-member relationship comparison outcomes."
)

_add_registry_entry(
    "IP004_RISK_REGISTER",
    _rcaip_post_table(
        "ip004_risk_register"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Unified prioritized IP-004 risk register with action scope and recommended ownership."
)

_add_registry_entry(
    "IP004_POST_ACTION_TABLE",
    _rcaip_post_table(
        "ip004_post_action_table"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Prioritized Post and joint-validation action queue for IP-004 findings."
)

_add_registry_entry(
    "IP004_ENTERPRISE_BOUNDARY_TABLE",
    _rcaip_post_table(
        "ip004_enterprise_boundary_table"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "IP-004 conditions requiring enterprise ownership, governance, or joint resolution."
)

_add_registry_entry(
    "IP004_EXECUTIVE_SUMMARY",
    _rcaip_post_table(
        "ip004_executive_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Executive-level summary metrics for IP-004 Active Directory hygiene analysis."
)

_add_registry_entry(
    "IP004_PRIORITY_SUMMARY",
    _rcaip_post_table(
        "ip004_priority_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of IP-004 review records by risk priority."
)

_add_registry_entry(
    "IP004_REVIEW_CATEGORY_SUMMARY",
    _rcaip_post_table(
        "ip004_review_category_summary"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Summary of IP-004 review records by category and action scope."
)

_add_registry_entry(
    "IP004_PHASE2_VERIFICATION",
    _rcaip_post_table(
        "ip004_phase2_verification"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Phase 2 completeness and uniqueness verification results for IP-004 products."
)

_add_registry_entry(
    "IP004_CSA_QUESTION_MATRIX",
    _rcaip_post_table(
        "ip004_csa_question_matrix"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "CSA question, evidence, finding-applicability, and validation-guardrail matrix for IP-004."
)

_add_registry_entry(
    "IP004_CSA_EVIDENCE_METRICS",
    _rcaip_post_table(
        "ip004_csa_evidence_metrics"
    ),
    "POST_SCOPED",
    50,
    "Active Directory Analysis",
    "IP-004",
    "Measured IP-004 evidence metrics mapped to applicable CSA questions."
)

# ---------------------------------------------------------------------------
# 8. Operational View - ENTERPRISE_MULTI_POST
# ---------------------------------------------------------------------------

_add_registry_entry(
    "OPERATIONAL_ASSET_VIEW",
    _rcaip_post_table(
        "operational_asset_view"
    ),
    "POST_SCOPED",
    60,
    "Operational View",
    "IP-005",
    (
        "One durable operational row per asset_key across the certified "
        "Post asset universe, enriched with source observation, lifecycle, "
        "reconciliation, and security-tool coverage evidence."
    )
)

_add_registry_entry(
    "OPERATIONAL_INVESTIGATION_UNIVERSE",
    _rcaip_post_table(
        "operational_investigation_universe"
    ),
    "POST_SCOPED",
    61,
    "Operational View",
    "IP-005",
    (
        "One row per operational asset_key with mutually exclusive "
        "identity disposition, independent remediation flags, priority, "
        "owner scope, investigation reason, and recommended action."
    )
)

_add_registry_entry(
    "OPERATIONAL_IDENTITY_INVESTIGATION_QUEUE",
    _rcaip_post_table(
        "operational_identity_investigation_queue"
    ),
    "POST_SCOPED",
    62,
    "Operational View",
    "IP-005",
    (
        "Assets whose identity, ownership, lifecycle disposition, or "
        "Post attribution still requires investigation."
    )
)

_add_registry_entry(
    "SECURITY_TOOL_REMEDIATION_QUEUE",
    _rcaip_post_table(
        "security_tool_remediation_queue"
    ),
    "POST_SCOPED",
    63,
    "Operational View",
    "IP-005",
    (
        "Assets requiring validation or remediation of required "
        "security-tool installation and reporting coverage."
    )
)

_add_registry_entry(
    "CANONICAL_IDENTITY_RECONCILIATION_QUEUE",
    _rcaip_post_table(
        "canonical_identity_reconciliation_queue"
    ),
    "POST_SCOPED",
    64,
    "Operational View",
    "IP-005",
    (
        "Lifecycle-retained identities requiring reconciliation with "
        "the canonical observed-asset rollup."
    )
)

_add_registry_entry(
    "ENTERPRISE_ATTRIBUTION_CANDIDATE_QUEUE",
    _rcaip_post_table(
        "enterprise_attribution_candidate_queue"
    ),
    "POST_SCOPED",
    65,
    "Operational View",
    "IP-005",
    (
        "Current enterprise-observed identities outside the selected "
        "Post asset plane that require location and ownership attribution."
    )
)

_add_registry_entry(
    "CONFIRMED_OPERATIONAL_BASELINE",
    _rcaip_post_table(
        "confirmed_operational_baseline"
    ),
    "POST_SCOPED",
    66,
    "Operational View",
    "IP-005",
    (
        "Current CMDB-present identities confirmed inside the selected "
        "Post asset plane, with independent remediation flags preserved."
    )
)

_add_registry_entry(
    "OPERATIONAL_MANAGEMENT_ASSET_RECONCILIATION",
    _rcaip_shared_table(
        "rcaip_operational_management_asset_reconciliation"
    ),
    "ENTERPRISE_MULTI_POST",
    60,
    "Operational View",
    "Operational Management",
    "Management-record reconciliation to durable Asset Plane identities."
)

_add_registry_entry(
    "OPERATIONAL_MANAGEMENT_ASSET_RECONCILIATION_SUMMARY",
    _rcaip_shared_table(
        "rcaip_operational_management_asset_reconciliation_summary"
    ),
    "ENTERPRISE_MULTI_POST",
    60,
    "Operational View",
    "Operational Management",
    "Multi-Post summary of operational-to-asset reconciliation outcomes."
)

_add_registry_entry(
    "OPERATIONAL_MANAGEMENT_LIFECYCLE_REVIEW",
    _rcaip_shared_table(
        "rcaip_operational_management_lifecycle_review"
    ),
    "ENTERPRISE_MULTI_POST",
    60,
    "Operational View",
    "Operational Management",
    "Lifecycle and action-queue view combining management and asset context."
)



# ---------------------------------------------------------------------------
# IP-006 Assurance Case Products - POST_SCOPED
#
# Contract:
#   These products publish the deterministic decision-equivalence assurance
#   case layer derived from certified IP-006 review relationships.
#
#   The registry declaration is Post-agnostic. Case formation logic and
#   certification occur in the IP-006 Assurance Case build notebook.
# ---------------------------------------------------------------------------

_add_registry_entry(
    "IP006_ASSURANCE_CASE",
    _rcaip_post_table(
        "ip006_assurance_case"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    (
        "One row per versioned shadow assurance case formed from certified "
        "review relationships using deterministic decision-equivalence, "
        "lifecycle qualification, ownership, customer, and treatment semantics."
    )
)

_add_registry_entry(
    "IP006_ASSURANCE_CASE_MEMBER",
    _rcaip_post_table(
        "ip006_assurance_case_member"
    ),
    "POST_SCOPED",
    40,
    "Asset Plane Analytics",
    "IP-006",
    (
        "One row per certified review relationship assigned to a versioned "
        "shadow assurance case, preserving member-level evidence, visibility, "
        "identity, lifecycle, provenance, and drill-down context."
    )
)


print(
    "RCAIP registry entries declared:",
    len(
        _registry_entries
    )
)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# Registry Construction, Validation, and Access Functions
#
# This cell builds authoritative runtime objects only after every registry
# entry has been declared.
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Validate logical-name uniqueness
# ---------------------------------------------------------------------------

logical_name_counts = {}


for registry_entry in _registry_entries:

    logical_name = registry_entry[
        "logical_name"
    ]


    logical_name_counts[
        logical_name
    ] = (
        logical_name_counts.get(
            logical_name,
            0
        )
        +
        1
    )


duplicate_logical_names = sorted([
    logical_name

    for (
        logical_name,
        logical_name_count
    )
    in logical_name_counts.items()

    if logical_name_count > 1
])


if duplicate_logical_names:

    raise RuntimeError(
        "Duplicate logical table names exist in the RCAIP registry: "
        +
        " | ".join(
            duplicate_logical_names
        )
    )


# ---------------------------------------------------------------------------
# 2. Build authoritative runtime lookup dictionaries
# ---------------------------------------------------------------------------

RCAIP_TABLE_REGISTRY = {
    registry_entry[
        "logical_name"
    ]:
    dict(
        registry_entry
    )

    for registry_entry
    in _registry_entries
}


RCAIP_TABLES = {
    logical_name:
    registry_entry[
        "physical_table_name"
    ]

    for (
        logical_name,
        registry_entry
    )
    in RCAIP_TABLE_REGISTRY.items()
}


# ---------------------------------------------------------------------------
# 3. Resolve the scope-specific naming contracts
# ---------------------------------------------------------------------------

post_table_prefix = (
    f"{configured_results_namespace}."
    f"{configured_post_slug}_"
)


enterprise_table_prefix = (
    f"{configured_results_namespace}."
    "rcaip_"
)


registry_validation_rows = []


for registry_entry in _registry_entries:

    logical_name = registry_entry[
        "logical_name"
    ]


    physical_table_name = registry_entry[
        "physical_table_name"
    ]


    scope = registry_entry[
        "scope"
    ]


    if scope == "POST_SCOPED":

        expected_prefix = post_table_prefix


        naming_contract_status = (
            "PASS"

            if physical_table_name.startswith(
                expected_prefix
            )

            else
            "FAIL"
        )


    elif scope in [
        "ENTERPRISE_SHARED",
        "ENTERPRISE_MULTI_POST"
    ]:

        expected_prefix = enterprise_table_prefix


        naming_contract_status = (
            "PASS"

            if physical_table_name.startswith(
                expected_prefix
            )

            else
            "FAIL"
        )


    elif scope == "ENTERPRISE_SOURCE":

        expected_prefix = None


        naming_contract_status = (
            "PASS"

            if validate_enterprise_source_table_name(
                physical_table_name
            )

            else
            "FAIL"
        )


    else:

        expected_prefix = None


        naming_contract_status = (
            "INVALID_SCOPE"
        )


    try:

        table_exists = spark.catalog.tableExists(
            physical_table_name
        )


        availability_check_status = (
            "CHECKED"
        )


        availability_error = None


    except Exception as table_exists_exception:

        table_exists = False


        availability_check_status = (
            "ERROR"
        )


        availability_error = str(
            table_exists_exception
        )[
            :2000
        ]


    registry_validation_rows.append(
        (
            logical_name,
            physical_table_name,
            scope,
            registry_entry[
                "layer_order"
            ],
            registry_entry[
                "architectural_layer"
            ],
            registry_entry[
                "product_family"
            ],
            registry_entry[
                "description"
            ],
            expected_prefix,
            naming_contract_status,
            table_exists,
            availability_check_status,
            availability_error
        )
    )


registry_validation_schema = T.StructType([
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
        "scope",
        T.StringType(),
        False
    ),

    T.StructField(
        "layer_order",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "architectural_layer",
        T.StringType(),
        False
    ),

    T.StructField(
        "product_family",
        T.StringType(),
        False
    ),

    T.StructField(
        "description",
        T.StringType(),
        False
    ),

    T.StructField(
        "expected_prefix",
        T.StringType(),
        True
    ),

    T.StructField(
        "naming_contract_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "table_exists",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "availability_check_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "availability_error",
        T.StringType(),
        True
    )
])


RCAIP_TABLE_REGISTRY_DF = (
    spark.createDataFrame(
        registry_validation_rows,
        schema=registry_validation_schema
    )

    .orderBy(
        "layer_order",
        "product_family",
        "logical_name"
    )
)


# ---------------------------------------------------------------------------
# 4. Fail only on registry design or availability-check errors
#
# A missing durable output is valid at the beginning of a new Post run.
# ---------------------------------------------------------------------------

invalid_naming_contract_count = (
    RCAIP_TABLE_REGISTRY_DF

    .filter(
        F.col(
            "naming_contract_status"
        )
        !=
        "PASS"
    )

    .count()
)


availability_error_count = (
    RCAIP_TABLE_REGISTRY_DF

    .filter(
        F.col(
            "availability_check_status"
        )
        ==
        "ERROR"
    )

    .count()
)


if invalid_naming_contract_count > 0:

    display(
        RCAIP_TABLE_REGISTRY_DF

        .filter(
            F.col(
                "naming_contract_status"
            )
            !=
            "PASS"
        )

        .select(
            "logical_name",
            "physical_table_name",
            "scope",
            "expected_prefix",
            "naming_contract_status"
        )
    )


    raise RuntimeError(
        "One or more registry entries violate the RCAIP table naming "
        "contract."
    )


if availability_error_count > 0:

    display(
        RCAIP_TABLE_REGISTRY_DF

        .filter(
            F.col(
                "availability_check_status"
            )
            ==
            "ERROR"
        )

        .select(
            "logical_name",
            "physical_table_name",
            "scope",
            "availability_check_status",
            "availability_error"
        )
    )


    raise RuntimeError(
        "One or more registry table-existence checks failed."
    )


# ---------------------------------------------------------------------------
# 5. Logical table lookup function
# ---------------------------------------------------------------------------

def get_rcaip_table(
    logical_name,
    require_exists=False
):
    """
    Resolve a logical RCAIP table name to its physical table name.
    """

    normalized_logical_name = str(
        logical_name
    ).strip().upper()


    if normalized_logical_name not in RCAIP_TABLES:

        raise KeyError(
            "Unknown RCAIP logical table name: "
            +
            normalized_logical_name
            +
            ". Available logical names: "
            +
            " | ".join(
                sorted(
                    RCAIP_TABLES.keys()
                )
            )
        )


    physical_table_name = RCAIP_TABLES[
        normalized_logical_name
    ]


    if (
        require_exists
        and
        not spark.catalog.tableExists(
            physical_table_name
        )
    ):

        raise RuntimeError(
            "Required RCAIP table does not exist: "
            +
            normalized_logical_name
            +
            " -> "
            +
            physical_table_name
        )


    return physical_table_name


# ---------------------------------------------------------------------------
# 6. Table existence and read functions
# ---------------------------------------------------------------------------

def rcaip_table_exists(
    logical_name
):
    """
    Return True when the registered physical table currently exists.
    """

    return spark.catalog.tableExists(
        get_rcaip_table(
            logical_name,
            require_exists=False
        )
    )


def read_rcaip_table(
    logical_name,
    require_exists=True
):
    """
    Read a registered RCAIP table as a Spark DataFrame.
    """

    return spark.table(
        get_rcaip_table(
            logical_name,
            require_exists=require_exists
        )
    )


# ---------------------------------------------------------------------------
# 7. Table-contract validation function
# ---------------------------------------------------------------------------

def validate_rcaip_table_contract(
    contract_name,
    logical_names,
    fail_on_missing=True
):
    """
    Validate that every logical table required by a notebook stage is
    registered and currently available.
    """

    normalized_contract_name = str(
        contract_name
    ).strip()


    if not normalized_contract_name:

        raise ValueError(
            "contract_name cannot be empty."
        )


    normalized_logical_names = [
        str(
            logical_name
        ).strip().upper()

        for logical_name
        in logical_names
    ]


    if not normalized_logical_names:

        raise ValueError(
            "logical_names cannot be empty."
        )


    contract_rows = []


    for logical_name in normalized_logical_names:

        physical_table_name = get_rcaip_table(
            logical_name,
            require_exists=False
        )


        contract_rows.append(
            (
                normalized_contract_name,
                logical_name,
                physical_table_name,
                spark.catalog.tableExists(
                    physical_table_name
                )
            )
        )


    contract_df = spark.createDataFrame(
        contract_rows,
        [
            "contract_name",
            "logical_name",
            "physical_table_name",
            "table_exists"
        ]
    )


    missing_contract_table_count = (
        contract_df

        .filter(
            F.col(
                "table_exists"
            )
            ==
            False
        )

        .count()
    )


    if (
        fail_on_missing
        and
        missing_contract_table_count > 0
    ):

        display(
            contract_df
        )


        raise RuntimeError(
            "RCAIP table contract failed: "
            +
            normalized_contract_name
            +
            ". Missing registered table count: "
            +
            str(
                missing_contract_table_count
            )
        )


    return contract_df


print(
    "RCAIP registry construction and validation: PASS"
)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# Verified Stage Contracts
#
# Stage contracts are logical-name lists only. Physical table names continue
# to resolve through the authoritative registry.
# ============================================================================


RCAIP_STAGE_CONTRACTS = {
    "IP001_INPUTS": [
        "NMAP_SCAN_PROVENANCE",
        "NMAP_NORM",
        "NMAP_PORTS_ALL",
        "KB_PORT_RULES"
    ],

    "ASSET_LIFECYCLE_ENRICHMENT_INPUTS": [
        "OBSERVED_ASSETS_ROLLUP",
        "ASSET_RECONCILIATION"
    ],

    "IP006_FEATURE_MART_INPUTS": [
        "ASSET_RECONCILIATION_SCORED",
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
    ],

    "IP006_RULE_ENGINE_INPUTS": [
        "IP006_ASSET_FEATURE_MART",
        "KB_RULE_REGISTRY_V2_2"
    ],

    "IP006_CONTROL_ASSURANCE_INPUTS": [
        "ASSET_RECONCILIATION_SCORED",
        "IP006_ASSET_FEATURE_MART",
        "KB_RULE_REGISTRY_V2_2"
    ],

    "IP006_CONTROL_ASSURANCE_OUTPUTS": [
        "IP006_CONTROL_ASSURANCE_EVALUATION",
        "IP006_CONTROL_ASSURANCE_FEATURE",
        "IP006_CONTROL_ASSURANCE_REVIEW_SAMPLE"
    ],

    "IP006_CONTROL_ASSURANCE_REVIEW_INPUTS": [
        "IP006_CONTROL_ASSURANCE_EVALUATION",
        "IP006_CONTROL_ASSURANCE_FEATURE",
        "IP006_CONTROL_ASSURANCE_REVIEW_SAMPLE",
        "KB_RULE_REGISTRY_V2_2"
    ],

    "IP006_CONTROL_ASSURANCE_REVIEW_OUTPUTS": [
        "IP006_CONTROL_ASSURANCE_REVIEW_QUEUE",
        "IP006_CONTROL_ASSURANCE_PROGRAM_SUMMARY"
    ],

    "IP004_SOURCE_INPUTS": [
        "AD_USERS_SOURCE",
        "AD_COMPUTERS_SOURCE",
        "AD_GROUPS_SOURCE",
        "ASSET_RECONCILIATION_SCORED"
    ],

    "IP005_OPERATIONAL_VIEW_INPUTS": [
        "OBSERVED_ASSETS",
        "OBSERVED_ASSETS_ROLLUP",
        "ASSET_RECONCILIATION_SCORED",
        "IP003_ASSET_LIFECYCLE_ROLLUP",
        "IP002_TOOL_COVERAGE_REVIEW"
    ],

    "IP005_OPERATIONAL_VIEW_OUTPUTS": [
        "OPERATIONAL_ASSET_VIEW"
    ],

    "IP005_OPERATIONAL_INVESTIGATION_INPUTS": [
    "OPERATIONAL_ASSET_VIEW"
    ],

"IP005_OPERATIONAL_INVESTIGATION_OUTPUTS": [
    "OPERATIONAL_INVESTIGATION_UNIVERSE"
    ],

"IP005_OPERATIONAL_DECISION_INPUTS": [
    "OPERATIONAL_INVESTIGATION_UNIVERSE"
],

"IP005_OPERATIONAL_DECISION_OUTPUTS": [
    "OPERATIONAL_IDENTITY_INVESTIGATION_QUEUE",
    "SECURITY_TOOL_REMEDIATION_QUEUE",
    "CANONICAL_IDENTITY_RECONCILIATION_QUEUE",
    "ENTERPRISE_ATTRIBUTION_CANDIDATE_QUEUE",
    "CONFIRMED_OPERATIONAL_BASELINE"
]

}


def validate_rcaip_stage_contract(
    stage_contract_name,
    fail_on_missing=True
):
    """
    Validate one named stage contract from RCAIP_STAGE_CONTRACTS.
    """

    normalized_stage_contract_name = str(
        stage_contract_name
    ).strip().upper()


    if normalized_stage_contract_name not in RCAIP_STAGE_CONTRACTS:

        raise KeyError(
            "Unknown RCAIP stage contract: "
            +
            normalized_stage_contract_name
            +
            ". Available stage contracts: "
            +
            " | ".join(
                sorted(
                    RCAIP_STAGE_CONTRACTS.keys()
                )
            )
        )


    return validate_rcaip_table_contract(
        contract_name=normalized_stage_contract_name,
        logical_names=RCAIP_STAGE_CONTRACTS[
            normalized_stage_contract_name
        ],
        fail_on_missing=fail_on_missing
    )


print(
    "RCAIP verified stage contracts:",
    len(
        RCAIP_STAGE_CONTRACTS
    )
)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# Registry Certification Summary
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Registry summary
# ---------------------------------------------------------------------------

rcaip_registry_summary_df = (
    RCAIP_TABLE_REGISTRY_DF

    .groupBy(
        "layer_order",
        "architectural_layer",
        "scope"
    )

    .agg(
        F.count(
            F.lit(
                1
            )
        ).alias(
            "registered_table_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "table_exists"
                )
                ==
                True,
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
            "available_table_count"
        )
    )

    .withColumn(
        "missing_table_count",
        F.col(
            "registered_table_count"
        )
        -
        F.col(
            "available_table_count"
        )
    )

    .orderBy(
        "layer_order",
        "scope"
    )
)


missing_registered_tables_df = (
    RCAIP_TABLE_REGISTRY_DF

    .filter(
        F.col(
            "table_exists"
        )
        ==
        False
    )

    .select(
        "logical_name",
        "physical_table_name",
        "scope",
        "architectural_layer",
        "product_family"
    )

    .orderBy(
        "architectural_layer",
        "product_family",
        "logical_name"
    )
)


registered_table_count = len(
    RCAIP_TABLES
)


available_registered_table_count = (
    RCAIP_TABLE_REGISTRY_DF

    .filter(
        F.col(
            "table_exists"
        )
        ==
        True
    )

    .count()
)


missing_registered_table_count = (
    missing_registered_tables_df.count()
)


# ---------------------------------------------------------------------------
# 2. Validate stage contracts without requiring unfinished outputs
# ---------------------------------------------------------------------------

verified_stage_contract_dfs = [
    validate_rcaip_stage_contract(
        stage_contract_name,
        fail_on_missing=False
    )

    for stage_contract_name
    in sorted(
        RCAIP_STAGE_CONTRACTS.keys()
    )
]


if not verified_stage_contract_dfs:

    raise RuntimeError(
        "RCAIP_STAGE_CONTRACTS did not provide any contracts to validate."
    )


verified_stage_contracts_df = (
    verified_stage_contract_dfs[
        0
    ]
)


for stage_contract_df in verified_stage_contract_dfs[
    1:
]:

    verified_stage_contracts_df = (
        verified_stage_contracts_df
        .unionByName(
            stage_contract_df
        )
    )


# ---------------------------------------------------------------------------
# 3. Display review products
# ---------------------------------------------------------------------------

print("RCAIP architectural pipeline")

display(
    RCAIP_PIPELINE_MODEL_DF
)


print("RCAIP table registry summary")

display(
    rcaip_registry_summary_df
)


print("RCAIP registered tables")

display(
    RCAIP_TABLE_REGISTRY_DF
)


print("RCAIP verified stage contracts")

display(
    verified_stage_contracts_df
)


if missing_registered_table_count > 0:

    print("Registered tables not currently available")

    display(
        missing_registered_tables_df
    )


# ---------------------------------------------------------------------------
# 4. Final status
# ---------------------------------------------------------------------------

RCAIP_TABLE_REGISTRY_READY = True


registry_status_rows = [
    (
        "Status",
        "PASS"
    ),
    (
        "Configured Post",
        str(
            configured_post_name
        )
    ),
    (
        "Configured Post slug",
        str(
            configured_post_slug
        )
    ),
    (
        "Results namespace",
        str(
            configured_results_namespace
        )
    ),
    (
        "Registered logical tables",
        str(
            registered_table_count
        )
    ),
    (
        "Currently available registered tables",
        str(
            available_registered_table_count
        )
    ),
    (
        "Currently missing registered tables",
        str(
            missing_registered_table_count
        )
    ),
    (
        "Naming contract failures",
        str(
            invalid_naming_contract_count
        )
    ),
    (
        "Availability check errors",
        str(
            availability_error_count
        )
    ),
    (
        "Registered stage contracts",
        str(
            len(
                RCAIP_STAGE_CONTRACTS
            )
        )
    )
]


registry_status_df = spark.createDataFrame(
    registry_status_rows,
    [
        "status_item",
        "status_value"
    ]
)


print("=" * 100)
print("RCAIP TABLE REGISTRY STATUS")
print("=" * 100)

display(
    registry_status_df
)

print("The RCAIP table registry is ready for enterprise notebook adoption.")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 001
# V3 Certified Nmap Runtime Survival Check
#
# POST-AGNOSTIC / D0024-SAFE
#
# Purpose:
#   Prove that the certified Nmap runtime published by Config 000 survives
#   intact through the Config 001 bootstrap.
#
# Certification is structural and runtime-derived.
# There are NO fixed Post names, XML counts, perspective counts, or hashes.
#
# Writes: NONE
# ============================================================================

import ipaddress
import json

from collections import Counter


# ---------------------------------------------------------------------------
# 1. Required inherited runtime
# ---------------------------------------------------------------------------

required_v3_runtime_objects = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",

    "RCAIP_NMAP_SCANS",
    "RCAIP_V3_NMAP_RUNTIME_READY",
    "RCAIP_V3_NMAP_MANIFEST_SHA256",

    "RCAIP_OPENNET_SCAN_TARGETS",
    "RCAIP_ENTWIFI_INTERNAL_OBSERVED_TARGETS",
    "RCAIP_PUBLIC_FACING_TARGETS",

    "RCAIP_NEN_PUBLIC_TARGETS",
    "RCAIP_ENTWIFI_PUBLIC_TARGETS",
]


missing_v3_runtime_objects = [
    object_name
    for object_name in required_v3_runtime_objects
    if object_name not in globals()
]


if missing_v3_runtime_objects:
    raise RuntimeError(
        "Certified Nmap runtime did not survive into Config 001. Missing: "
        + " | ".join(
            missing_v3_runtime_objects
        )
    )


certification_errors = []


if not bool(
    RCAIP_CONFIG_000_READY
):
    certification_errors.append(
        "Config 000 ready flag is False."
    )


if not bool(
    RCAIP_V3_NMAP_RUNTIME_READY
):
    certification_errors.append(
        "Certified V3 Nmap runtime ready flag is False."
    )


if not isinstance(
    RCAIP_NMAP_SCANS,
    list
):
    certification_errors.append(
        "RCAIP_NMAP_SCANS is not a Python list."
    )

elif not RCAIP_NMAP_SCANS:
    certification_errors.append(
        "RCAIP_NMAP_SCANS contains no certified XML parent records."
    )


# ---------------------------------------------------------------------------
# 2. D0024-safe target helpers
# ---------------------------------------------------------------------------

def normalize_runtime_cidr(
    raw_value
):
    return str(
        ipaddress.ip_network(
            str(
                raw_value
            ).strip(),
            strict=False
        )
    )


def ordered_unique(
    values
):
    output = []

    for value in values:

        if value not in output:
            output.append(
                value
            )

    return output


def parse_runtime_targets(
    scan_record
):
    """
    Return the complete certified target list for one XML parent.

    D0024:
      target_cidrs / target_cidrs_json contain the authoritative target scope.
      target_cidr is a single-CIDR compatibility field.
      Historical MULTI_TARGET values remain readable for legacy evidence.
    """

    raw_targets = scan_record.get(
        "target_cidrs"
    )

    if isinstance(
        raw_targets,
        (
            list,
            tuple
        )
    ):
        candidate_targets = list(
            raw_targets
        )

    else:

        candidate_targets = []

        raw_json = scan_record.get(
            "target_cidrs_json"
        )

        if (
            raw_json is not None
            and
            str(
                raw_json
            ).strip()
        ):

            try:

                parsed_json = json.loads(
                    str(
                        raw_json
                    )
                )

                if isinstance(
                    parsed_json,
                    str
                ):
                    parsed_json = [
                        parsed_json
                    ]

                if isinstance(
                    parsed_json,
                    list
                ):
                    candidate_targets = (
                        parsed_json
                    )

            except Exception:

                candidate_targets = []


    # Compatibility fallback for mature singular records.
    if not candidate_targets:

        compatibility_target = str(
            scan_record.get(
                "target_cidr"
            )
            or
            ""
        ).strip()

        if (
            compatibility_target.upper()
            not in
            {
                "",
                "UNKNOWN",
                "MULTI_TARGET",
            }
        ):

            candidate_targets = [
                compatibility_target
            ]


    normalized_targets = []

    for raw_target in candidate_targets:

        try:

            normalized_target = (
                normalize_runtime_cidr(
                    raw_target
                )
            )

        except Exception:

            raise RuntimeError(
                "Certified Nmap record contains an invalid target CIDR: "
                + str(
                    raw_target
                )
            )

        if (
            normalized_target
            not in
            normalized_targets
        ):
            normalized_targets.append(
                normalized_target
            )


    return normalized_targets


def normalize_target_list(
    values,
    label
):
    normalized_values = []

    for raw_value in (
        values
        or
        []
    ):

        raw_text = str(
            raw_value
        ).strip()

        if raw_text.upper() in {
            "MULTI_TARGET",
            "UNKNOWN",
        }:

            certification_errors.append(
                f"{label} contains compatibility sentinel {raw_text} "
                "instead of a real CIDR."
            )

            continue

        try:

            normalized_value = (
                normalize_runtime_cidr(
                    raw_text
                )
            )

        except Exception:

            certification_errors.append(
                f"{label} contains invalid CIDR: {raw_text}"
            )

            continue

        if (
            normalized_value
            not in
            normalized_values
        ):
            normalized_values.append(
                normalized_value
            )

    return normalized_values


# ---------------------------------------------------------------------------
# 3. Reconstruct the runtime from the surviving XML parents
# ---------------------------------------------------------------------------

manifest_hashes = set()

xml_files = []

scan_perspectives = Counter()

derived_opennet_targets = []

derived_entwifi_internal_targets = []

derived_public_facing_targets = []

derived_nen_public_targets = []

derived_entwifi_public_targets = []

multi_target_parent_count = 0


for scan_position, scan_record in enumerate(
    RCAIP_NMAP_SCANS
):

    if not isinstance(
        scan_record,
        dict
    ):

        certification_errors.append(
            "Nmap runtime record "
            f"{scan_position + 1} is not a dictionary."
        )

        continue


    file_name = str(
        scan_record.get(
            "file"
        )
        or
        ""
    ).strip()


    if not file_name:

        certification_errors.append(
            "A certified Nmap runtime record has a blank file name."
        )

    else:

        xml_files.append(
            file_name
        )


    record_post = str(
        scan_record.get(
            "post_profile_key"
        )
        or
        ""
    ).strip().lower()


    if (
        record_post
        !=
        str(
            RCAIP_POST_SLUG
        ).strip().lower()
    ):

        certification_errors.append(
            "Certified Nmap record Post binding differs from the "
            f"active runtime: {file_name}"
        )


    record_run = str(
        scan_record.get(
            "run_id"
        )
        or
        ""
    ).strip()


    if (
        record_run
        !=
        str(
            RCAIP_RUN_ID
        ).strip()
    ):

        certification_errors.append(
            "Certified Nmap record run binding differs from the "
            f"active runtime: {file_name}"
        )


    manifest_hash = str(
        scan_record.get(
            "manifest_sha256"
        )
        or
        ""
    ).strip().lower()


    if manifest_hash:

        manifest_hashes.add(
            manifest_hash
        )

    else:

        certification_errors.append(
            "Certified Nmap record has no manifest SHA256: "
            + file_name
        )


    try:

        target_cidrs = (
            parse_runtime_targets(
                scan_record
            )
        )

    except Exception as exc:

        certification_errors.append(
            f"{file_name}: {exc}"
        )

        target_cidrs = []


    if not target_cidrs:

        certification_errors.append(
            "Certified Nmap record contains no usable target CIDRs: "
            + file_name
        )


    declared_target_count = (
        scan_record.get(
            "target_cidr_count"
        )
    )


    if declared_target_count is not None:

        try:

            declared_target_count = int(
                declared_target_count
            )

        except Exception:

            certification_errors.append(
                "Invalid target_cidr_count for "
                + file_name
            )

            declared_target_count = None


    if (
        declared_target_count is not None
        and
        declared_target_count
        !=
        len(
            target_cidrs
        )
    ):

        certification_errors.append(
            "D0024 target count mismatch for "
            f"{file_name}: declared={declared_target_count}, "
            f"observed={len(target_cidrs)}."
        )


    compatibility_target = str(
        scan_record.get(
            "target_cidr"
        )
        or
        ""
    ).strip()


    if len(
        target_cidrs
    ) > 1:

        multi_target_parent_count += 1

        # D0024: the complete target set is authoritative. target_cidr is
        # compatibility-only. Historical MULTI_TARGET values remain readable,
        # while current evidence may publish one valid member CIDR instead.
        if (
            compatibility_target.upper()
            !=
            "MULTI_TARGET"
        ):

            try:

                normalized_compatibility_target = (
                    normalize_runtime_cidr(
                        compatibility_target
                    )
                )

            except Exception:

                normalized_compatibility_target = None


            if (
                normalized_compatibility_target
                not in
                target_cidrs
            ):

                certification_errors.append(
                    "D0024 multi-CIDR compatibility target is not a valid "
                    "member of the certified target set: "
                    f"{file_name} | "
                    f"{compatibility_target or '<BLANK>'}"
                )


    elif len(
        target_cidrs
    ) == 1:

        try:

            normalized_compatibility_target = (
                normalize_runtime_cidr(
                    compatibility_target
                )
            )

        except Exception:

            normalized_compatibility_target = None


        if (
            normalized_compatibility_target
            !=
            target_cidrs[0]
        ):

            certification_errors.append(
                "Singular compatibility target differs from "
                f"the certified target list: {file_name}"
            )


    scan_perspective = str(
        scan_record.get(
            "scan_perspective"
        )
        or
        ""
    ).strip().upper()


    scan_perspectives[
        scan_perspective
    ] += 1


    network_role = str(
        scan_record.get(
            "network_role"
        )
        or
        ""
    ).strip().upper()


    if (
        scan_perspective
        ==
        "INTERNAL_OPENNET"
    ):

        derived_opennet_targets.extend(
            target_cidrs
        )


    elif (
        scan_perspective
        ==
        "INTERNAL_ENTERPRISE_WIFI"
    ):

        derived_entwifi_internal_targets.extend(
            target_cidrs
        )


    elif (
        scan_perspective
        ==
        "PUBLIC_FACING"
    ):

        derived_public_facing_targets.extend(
            target_cidrs
        )


        if (
            network_role
            ==
            "NEN_PUBLIC"
        ):

            derived_nen_public_targets.extend(
                target_cidrs
            )


        elif (
            network_role
            ==
            "ENTERPRISE_WIFI_PUBLIC"
        ):

            derived_entwifi_public_targets.extend(
                target_cidrs
            )


derived_opennet_targets = ordered_unique(
    derived_opennet_targets
)

derived_entwifi_internal_targets = ordered_unique(
    derived_entwifi_internal_targets
)

derived_public_facing_targets = ordered_unique(
    derived_public_facing_targets
)

derived_nen_public_targets = ordered_unique(
    derived_nen_public_targets
)

derived_entwifi_public_targets = ordered_unique(
    derived_entwifi_public_targets
)


# ---------------------------------------------------------------------------
# 4. Structural survival checks
#
# No Post-specific counts or hashes are allowed here.
# ---------------------------------------------------------------------------

if (
    len(
        set(
            xml_files
        )
    )
    !=
    len(
        xml_files
    )
):

    certification_errors.append(
        "Duplicate XML parent records survived into Config 001."
    )


if len(
    manifest_hashes
) != 1:

    certification_errors.append(
        "Surviving Nmap runtime does not contain exactly one "
        "certified manifest SHA256."
    )


else:

    surviving_manifest_hash = next(
        iter(
            manifest_hashes
        )
    )


    if (
        surviving_manifest_hash
        !=
        str(
            RCAIP_V3_NMAP_MANIFEST_SHA256
        ).strip().lower()
    ):

        certification_errors.append(
            "Config 001 Nmap manifest hash differs from "
            "the Config 000 certified runtime hash."
        )


published_opennet_targets = normalize_target_list(
    RCAIP_OPENNET_SCAN_TARGETS,
    "RCAIP_OPENNET_SCAN_TARGETS"
)

published_entwifi_internal_targets = normalize_target_list(
    RCAIP_ENTWIFI_INTERNAL_OBSERVED_TARGETS,
    "RCAIP_ENTWIFI_INTERNAL_OBSERVED_TARGETS"
)

published_public_facing_targets = normalize_target_list(
    RCAIP_PUBLIC_FACING_TARGETS,
    "RCAIP_PUBLIC_FACING_TARGETS"
)

published_nen_public_targets = normalize_target_list(
    RCAIP_NEN_PUBLIC_TARGETS,
    "RCAIP_NEN_PUBLIC_TARGETS"
)

published_entwifi_public_targets = normalize_target_list(
    RCAIP_ENTWIFI_PUBLIC_TARGETS,
    "RCAIP_ENTWIFI_PUBLIC_TARGETS"
)


if (
    set(
        published_opennet_targets
    )
    !=
    set(
        derived_opennet_targets
    )
):

    certification_errors.append(
        "OpenNet target projection changed between "
        "Config 000 and Config 001."
    )


if (
    set(
        published_entwifi_internal_targets
    )
    !=
    set(
        derived_entwifi_internal_targets
    )
):

    certification_errors.append(
        "Internal Enterprise WiFi target projection changed "
        "between Config 000 and Config 001."
    )


if (
    set(
        published_public_facing_targets
    )
    !=
    set(
        derived_public_facing_targets
    )
):

    certification_errors.append(
        "Public-facing target projection changed between "
        "Config 000 and Config 001."
    )


if (
    set(
        published_nen_public_targets
    )
    !=
    set(
        derived_nen_public_targets
    )
):

    certification_errors.append(
        "NEN public compatibility projection changed between "
        "Config 000 and Config 001."
    )


if (
    set(
        published_entwifi_public_targets
    )
    !=
    set(
        derived_entwifi_public_targets
    )
):

    certification_errors.append(
        "Enterprise WiFi public compatibility projection changed "
        "between Config 000 and Config 001."
    )


# ---------------------------------------------------------------------------
# 5. Publish Config 001 survival result
# ---------------------------------------------------------------------------

RCAIP_CONFIG_001_V3_NMAP_SURVIVAL_READY = (
    len(
        certification_errors
    )
    ==
    0
)


# ---------------------------------------------------------------------------
# 6. Report
# ---------------------------------------------------------------------------

print("=" * 110)
print(
    "RCAIP CONFIG 001 - "
    "V3 CERTIFIED NMAP RUNTIME SURVIVAL CHECK"
)
print("=" * 110)

print(
    "Status:",
    (
        "PASS"
        if RCAIP_CONFIG_001_V3_NMAP_SURVIVAL_READY
        else
        "REVIEW_REQUIRED"
    )
)

print(
    "Config 000 ready:",
    RCAIP_CONFIG_000_READY
)

print(
    "Certified Nmap runtime ready:",
    RCAIP_V3_NMAP_RUNTIME_READY
)

print(
    "Post:",
    RCAIP_POST_NAME
)

print(
    "Run ID:",
    RCAIP_RUN_ID
)

print(
    "Certified XML parent records:",
    len(
        RCAIP_NMAP_SCANS
    )
)

print(
    "Multi-target XML parent records:",
    multi_target_parent_count
)

print(
    "Manifest SHA256:",
    RCAIP_V3_NMAP_MANIFEST_SHA256
)

print()
print("SCAN PERSPECTIVES")
print("-----------------")

for (
    perspective,
    perspective_count
) in sorted(
    scan_perspectives.items()
):

    print(
        f"{perspective or 'UNSPECIFIED'}:",
        perspective_count
    )


print()
print(
    "OpenNet target CIDRs:",
    len(
        published_opennet_targets
    )
)

print(
    "Internal Enterprise WiFi observed target CIDRs:",
    len(
        published_entwifi_internal_targets
    )
)

print(
    "Public-facing target CIDRs:",
    len(
        published_public_facing_targets
    )
)

print(
    "NEN public compatibility targets:",
    len(
        published_nen_public_targets
    )
)

print(
    "Enterprise WiFi public compatibility targets:",
    len(
        published_entwifi_public_targets
    )
)

print()
print(
    "Registry modifications: NONE"
)

print(
    "Delta writes: NONE"
)


if certification_errors:

    print()
    print("CERTIFICATION ERRORS")
    print("--------------------")

    for error_text in certification_errors:

        print(
            "-",
            error_text
        )


print("=" * 110)


if certification_errors:

    raise RuntimeError(
        "Config 001 V3 Nmap runtime survival certification failed: "
        + " | ".join(
            certification_errors
        )
    )