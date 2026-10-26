# Databricks notebook source
# MAGIC %run ./RCAIP_Config_000_Post_Run_Config

# COMMAND ----------

# ============================================================================
# RCAIP_KB_001_Knowledge_Base
#
# Purpose:
#   Build reusable RCAIP knowledge tables used by RCAIP intelligence products.
#
# Output:
#   devresults.rcaip_kb_isso_subnet_lookup
#   devresults.rcaip_kb_asset_patterns
#   devresults.rcaip_kb_observation_rules
#   devresults.rcaip_kb_required_tools
#   devresults.rcaip_kb_port_rules
#   devresults.rcaip_kb_csa_question_map
#
# Optional test outputs:
#   devresults.example_post_d_example_run
#   devresults.example_post_d_example_run
# ============================================================================

output_schema = "devresults"

# Current post used only for test cells at the bottom.
# The KB itself is global / reusable.
post_name = "example_post_d"
post_slug = "example_post_d"

# Publication copy: internal review attribution omitted.
isso_lookup_source_table = "example_sources.binding_9e3678b1"

print("RCAIP KB Config Loaded")
print("Output schema:", output_schema)
print("Current test post:", post_name)
print("ISSO lookup source:", isso_lookup_source_table)


# COMMAND ----------

# ============================================================================
# Cell 2 - Helpers
# ============================================================================

import re

from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark.sql(f"CREATE DATABASE IF NOT EXISTS {output_schema}")

def clean_column_name(name):
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned

def clean_dataframe_columns(df):
    return df.toDF(*[clean_column_name(c) for c in df.columns])

def save_delta(df, full_table_name):
    (
        df.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print("Saved:", full_table_name)
    return full_table_name

def col_or_null(df, column_name, alias_name):
    if column_name in df.columns:
        return col(column_name).cast("string").alias(alias_name)
    return lit(None).cast("string").alias(alias_name)

def show_table(table_name, n=20):
    print(table_name)
    display(spark.table(table_name).limit(n))


# COMMAND ----------

# ============================================================================
# Cell 3 - Build ISSO / Subnet Lookup Knowledge Table
#
# Source:
#   example_sources.binding_9e3678b1
#
# Purpose:
#   Normalize subnet-to-post lookup data for RCAIP attribution.
# ============================================================================

isso_raw = spark.table(isso_lookup_source_table)

print("Raw ISSO lookup columns:")
print(isso_raw.columns)

display(isso_raw.limit(20))

isso_clean = clean_dataframe_columns(isso_raw)

print("Cleaned ISSO lookup columns:")
print(isso_clean.columns)

kb_isso_subnet_lookup = (
    isso_clean
    .select(
        col_or_null(isso_clean, "subnet", "subnet_cidr"),
        col_or_null(isso_clean, "post", "post"),
        col_or_null(isso_clean, "postcode", "post_code"),
        col_or_null(isso_clean, "mission", "mission"),
        col_or_null(isso_clean, "bureau", "bureau"),
        col_or_null(isso_clean, "ipostid", "ipost_id"),
        col_or_null(isso_clean, "subsite", "subsite")
    )
    .withColumn("source_system", lit("ISSO/CDTO Lookup"))
    .withColumn("kb_table", lit("rcaip_kb_isso_subnet_lookup"))
    .dropDuplicates(["subnet_cidr", "post", "post_code", "subsite"])
)

display(kb_isso_subnet_lookup.limit(50))

save_delta(kb_isso_subnet_lookup, f"{output_schema}.rcaip_kb_isso_subnet_lookup")


# COMMAND ----------

# ============================================================================
# Cell 4 - Asset Classification Patterns
#
# Purpose:
#   Store reusable regex patterns that help RCAIP infer likely asset type,
#   asset family, confidence, and recommended review owner.
#
# Notes:
#   These are not final determinations. They are intelligence signals.
# ============================================================================

asset_patterns = [
    # ------------------------------------------------------------------------
    # Server management interfaces
    # ------------------------------------------------------------------------
    (
        "srv_mgmt_idrac",
        r"idrac|i[-_ ]?drac|dell.*remote|integrated dell remote",
        "Server Management Interface",
        "Server",
        "Enterprise",
        "High",
        95,
        "Dell iDRAC or Dell remote/server management interface indicator.",
        "Validate server inventory and management interface exposure."
    ),
    (
        "srv_mgmt_ilo",
        r"(^|[-_ ])ilo($|[-_0-9])|hpe.*ilo|integrated lights[- ]out",
        "Server Management Interface",
        "Server",
        "Enterprise",
        "High",
        95,
        "HP/HPE iLO server management interface indicator.",
        "Validate server inventory and management interface exposure."
    ),
    (
        "srv_mgmt_bmc_ipmi",
        r"bmc|ipmi|asf-rmcp|baseboard management|imm",
        "Server Management Interface",
        "Server",
        "Enterprise",
        "Medium",
        85,
        "BMC/IPMI/server hardware management indicator.",
        "Validate server management ownership and access controls."
    ),

    # ------------------------------------------------------------------------
    # Printers and MFDs
    # ------------------------------------------------------------------------
    (
        "printer_generic",
        r"prnt|print|printer|jetdirect|xerox|ricoh|canon|lexmark|laserjet|officejet|mfp|mfd",
        "Printer or MFD",
        "Peripheral",
        "Enterprise",
        "High",
        90,
        "Printer or multifunction device naming/product/service indicator.",
        "Validate CMDB inventory, MFD baseline, and required print protocols."
    ),
    (
        "printer_ports",
        r"jetdirect|ipp|printer|lpd",
        "Printer or MFD",
        "Peripheral",
        "Enterprise",
        "Medium",
        80,
        "Printer service indicator from NMAP/service data.",
        "Validate that only required printer services are enabled."
    ),

    # ------------------------------------------------------------------------
    # Network infrastructure
    # ------------------------------------------------------------------------
    (
        "opennet_switch_name",
        r"^o[-_][a-z0-9]+[-_]ch[-_]sw[0-9]+|[-_]sw[0-9]+$|switch",
        "Switch",
        "Network Infrastructure",
        "Enterprise",
        "High",
        90,
        "OpenNet / enterprise switch naming pattern or switch keyword.",
        "Validate switch baseline and CMDB inventory."
    ),
    (
        "router_name",
        r"router|rtr|[-_]rtr[0-9]+|cisco.*router|juniper",
        "Router",
        "Network Infrastructure",
        "Enterprise",
        "Medium",
        85,
        "Router naming/product indicator.",
        "Validate network infrastructure inventory and secure configuration."
    ),
    (
        "firewall_vpn",
        r"firewall|fw[-_0-9]|vpn|asa|forti|palo|checkpoint|gateway",
        "Firewall or VPN",
        "Network Infrastructure",
        "NEN",
        "Medium",
        85,
        "Firewall, VPN, or gateway indicator.",
        "Validate public exposure, ownership, and required security controls."
    ),
    (
        "wireless_infra",
        r"wlc|wireless|wifi|wap|aironet|ap[-_0-9]|cisco.*air|aruba.*ap|unifi",
        "Wireless Infrastructure",
        "Network Infrastructure",
        "Enterprise WiFi",
        "Medium",
        80,
        "Wireless controller or access point indicator.",
        "Validate wireless management protocols and required configuration."
    ),

    # ------------------------------------------------------------------------
    # Servers
    # ------------------------------------------------------------------------
    (
        "domain_controller",
        r"(^|[-_])dc[0-9]+|domain controller|kerberos|ldap",
        "Domain Controller",
        "Server",
        "Enterprise",
        "High",
        90,
        "Domain controller naming or service indicator.",
        "Validate server inventory and audit/logging coverage."
    ),
    (
        "file_server",
        r"(^|[-_])fs[0-9]+|file[-_ ]?server|smb|microsoft-ds",
        "File Server",
        "Server",
        "Enterprise",
        "Medium",
        80,
        "File server naming/service indicator.",
        "Validate share permissions, logging, backup, and inventory."
    ),
    (
        "database_server",
        r"sql|mssql|mysql|postgres|oracle|database|db[0-9]+",
        "Database Server",
        "Server",
        "Enterprise",
        "Medium",
        80,
        "Database server naming/service indicator.",
        "Validate server inventory, patching, and logging."
    ),
    (
        "server_generic",
        r"srv|server|[-_]mgt[0-9]*|mgmt|management",
        "Server",
        "Server",
        "Enterprise",
        "Medium",
        70,
        "Generic server or management naming indicator.",
        "Validate server role and inventory."
    ),

    # ------------------------------------------------------------------------
    # Endpoint / workstation indicators
    # ------------------------------------------------------------------------
    (
        "admin_workstation",
        r"daw|admin[-_ ]?workstation|adm[-_]",
        "Administrative Workstation",
        "Endpoint",
        "Enterprise",
        "Medium",
        80,
        "Administrator workstation naming indicator.",
        "Validate privileged workstation management."
    ),
    (
        "tfl_emd",
        r"tfl|emd",
        "Enterprise Managed Device",
        "Endpoint",
        "Enterprise",
        "Medium",
        75,
        "TFL/EMD enterprise managed endpoint indicator.",
        "Validate ownership and management status."
    ),

    # ------------------------------------------------------------------------
    # Special function / site hints
    # ------------------------------------------------------------------------
    (
        "consular_function",
        r"cons|acs|niv|iv",
        "Consular-Associated Asset",
        "Function Hint",
        "Enterprise",
        "Low",
        55,
        "Consular naming/function indicator such as ACS, NIV, IV, CONS.",
        "Use as attribution hint; do not classify solely on this pattern."
    ),

    # ------------------------------------------------------------------------
    # Power / facilities / OT
    # ------------------------------------------------------------------------
    (
        "ups_pdu",
        r"ups|pdu|apc|eaton|powerchute",
        "UPS or Power Device",
        "Facilities / Infrastructure",
        "Enterprise",
        "Medium",
        80,
        "UPS/PDU/power device indicator.",
        "Validate ownership, monitoring, and network exposure."
    ),
    (
        "bas_bms_ot",
        r"bas|bms|meter|meternet|chiller|hvac|modbus|bacnet|tridium|niagara",
        "BAS/BMS or OT Device",
        "Facilities / OT",
        "Building Systems",
        "Medium",
        80,
        "Building automation / facilities / OT indicator.",
        "Validate segmentation, physical security, and modernization plan."
    ),

    # ------------------------------------------------------------------------
    # NEN / public / provider clues
    # ------------------------------------------------------------------------
    (
        "nen_public_provider",
        r"static|t-mobile|orange|telekom|isp|public|guest|nen|pronet",
        "NEN/Public Network Observation",
        "NEN/Public",
        "NEN",
        "Low",
        50,
        "Commercial ISP/static/public/NEN naming clue.",
        "Validate circuit ownership, site, Qualys/Umbrella coverage."
    )
]

kb_asset_patterns = spark.createDataFrame(
    asset_patterns,
    [
        "pattern_id",
        "pattern_regex",
        "likely_asset_type",
        "asset_family",
        "asset_domain_hint",
        "pattern_confidence",
        "confidence_weight",
        "pattern_reason",
        "recommended_use"
    ]
)

display(kb_asset_patterns)

save_delta(kb_asset_patterns, f"{output_schema}.rcaip_kb_asset_patterns")


# COMMAND ----------

# ============================================================================
# Cell 5 - Observation Interpretation Rules
#
# Purpose:
#   Convert source-observation patterns into RCAIP interpretation rules.
# ============================================================================

observation_rules = [
    (
        "nmap_only_low_context",
        "NMAP-only low-context observation",
        "IP observed by NMAP does not equal confirmed endpoint.",
        "Low",
        "Technical Appendix Only",
        "Retain as evidence; do not task Post unless corroborated by hostname, meaningful services, MAC/vendor, Tenable, CMDB, MDE, Intune, or Qualys."
    ),
    (
        "nmap_only_meaningful_service",
        "NMAP-only with meaningful service evidence",
        "NMAP-only observation with hostname, service, product, or meaningful open ports may represent a real asset requiring attribution.",
        "Medium",
        "RCSO / Post Review",
        "Validate asset ownership and determine whether CMDB/local inventory should be updated."
    ),
    (
        "nmap_plus_tenable",
        "NMAP plus Tenable observed",
        "Asset observed by both NMAP and Tenable has stronger evidence of network presence.",
        "High",
        "Post + Enterprise Review",
        "Validate ownership, expected presence, and CMDB accuracy."
    ),
    (
        "mde_intune_only_no_ip",
        "Intune/MDE-only asset with no IP",
        "Cloud-managed or endpoint-observed asset; not necessarily locally reachable from Post network scans.",
        "Medium",
        "Enterprise / Attribution Review",
        "Review recency, user, serial number, naming convention, compliance, MDE status, and CMDB presence before tasking Post."
    ),
    (
        "recent_intune_mde_missing_cmdb",
        "Recent Intune/MDE asset missing CMDB",
        "Likely active enterprise endpoint missing from authoritative inventory.",
        "High",
        "CMDB / Enterprise Boundary Review",
        "Prioritize CMDB reconciliation and ownership attribution."
    ),
    (
        "mde_discovered_not_onboarded",
        "MDE discovered but not onboarded",
        "Possible unmanaged endpoint if recently and persistently observed.",
        "High",
        "Post + Enterprise Review",
        "Validate with Post/enterprise owner and determine whether onboarding or removal is required."
    ),
    (
        "stale_cloud_object",
        "Stale Intune/MDE object",
        "Old cloud/endpoint record with no recent check-in, no IP, weak metadata, or duplicate indicators.",
        "Low",
        "Enterprise Cleanup",
        "Retain as stale cleanup candidate; do not treat as active local endpoint without corroboration."
    ),
    (
        "cmdb_only_no_observation",
        "CMDB-only asset",
        "CMDB asset not observed by current RCAIP sources.",
        "Medium",
        "CMDB / Enterprise Boundary Review",
        "Validate whether asset is stale, assigned elsewhere, offline, incorrectly inventoried, or not visible through available discovery paths."
    ),
    (
        "nen_public_observed_no_qualys",
        "NEN public asset without Qualys coverage",
        "NEN/public-facing asset observed but not represented in Qualys coverage.",
        "High",
        "Post / NEN Compliance Review",
        "Validate Qualys coverage requirement and remediate if asset remains in scope."
    )
]

kb_observation_rules = spark.createDataFrame(
    observation_rules,
    [
        "rule_id",
        "rule_name",
        "interpretation",
        "default_priority",
        "recommended_owner",
        "recommended_action"
    ]
)

display(kb_observation_rules)

save_delta(kb_observation_rules, f"{output_schema}.rcaip_kb_observation_rules")


# COMMAND ----------

# ============================================================================
# Cell 6 - Required Tool Expectations
#
# Purpose:
#   Define expected RCAIP source coverage by asset type/domain.
# ============================================================================

required_tools = [
    # Enterprise endpoints
    ("Enterprise", "Endpoint", "MDE", "Required", "Enterprise endpoints should be visible to MDE."),
    ("Enterprise", "Endpoint", "Intune", "Required", "Enterprise endpoints should be managed/visible in Intune where applicable."),
    ("Enterprise", "Endpoint", "Tenable", "Required", "Enterprise endpoints should be visible to vulnerability scanning."),
    ("Enterprise", "Endpoint", "NMAP", "Expected if online/in-range", "May be absent if offline, traveling, or firewalled."),

    # Servers
    ("Enterprise", "Server", "MDE", "Required", "Enterprise servers should have endpoint security visibility where supported."),
    ("Enterprise", "Server", "Tenable", "Required", "Enterprise servers should be visible to vulnerability scanning."),
    ("Enterprise", "Server", "NMAP", "Expected", "Servers should normally be visible in network/service discovery."),
    ("Enterprise", "Server", "Intune", "Review", "Intune visibility may vary for servers."),

    # Network infrastructure
    ("Enterprise", "Network Infrastructure", "NMAP", "Expected", "Network devices should be visible to network discovery."),
    ("Enterprise", "Network Infrastructure", "Tenable", "Expected", "Network devices should generally be visible to vulnerability scanning."),
    ("Enterprise", "Network Infrastructure", "MDE", "Not Expected", "Traditional endpoint sensor may not apply."),
    ("Enterprise", "Network Infrastructure", "Intune", "Not Expected", "Intune typically does not manage network infrastructure."),

    # Printers / MFDs
    ("Enterprise", "Peripheral", "NMAP", "Expected", "Printers are typically visible to network discovery."),
    ("Enterprise", "Peripheral", "Tenable", "Expected", "Printers should generally be visible to vulnerability scanning."),
    ("Enterprise", "Peripheral", "MDE", "Review", "MDE visibility may vary by device integration."),
    ("Enterprise", "Peripheral", "Intune", "Review", "Intune visibility may vary."),

    # NEN / public
    ("NEN", "Endpoint", "Qualys", "Required", "NEN endpoints/assets should have required Qualys coverage where applicable."),
    ("NEN", "Endpoint", "MDE", "Required", "NEN endpoints should have MDE where required by policy."),
    ("NEN", "Endpoint", "Intune", "Required", "NEN endpoints should have Intune/management where required by policy."),
    ("NEN", "Endpoint", "Umbrella", "Required at edge", "Public/NEN edge should have Umbrella monitoring where applicable."),
    ("NEN", "NEN/Public", "NMAP", "Expected", "NEN/public IPs should be validated through authorized discovery."),

    # Enterprise WiFi
    ("Enterprise WiFi", "Network Infrastructure", "NMAP", "Expected", "Enterprise WiFi public/edge ranges should be reviewed for exposed services."),
    ("Enterprise WiFi", "Network Infrastructure", "Umbrella", "Required/Review", "Validate Umbrella monitoring/policy where applicable.")
]

kb_required_tools = spark.createDataFrame(
    required_tools,
    [
        "asset_domain",
        "asset_family",
        "tool_name",
        "expected_status",
        "expectation_reason"
    ]
)

display(kb_required_tools)

save_delta(kb_required_tools, f"{output_schema}.rcaip_kb_required_tools")


# COMMAND ----------

# ============================================================================
# Cell 7 - Port Review Rules
#
# Purpose:
#   Define open-port review rules used by IP-001 and CSA evidence modules.
# ============================================================================

port_rules = [
    (21, "FTP", "High", "Insecure file transfer", "FTP transmits data and credentials in clear text. Verify business need; disable if not required.", "Port Security"),
    (23, "Telnet", "High", "Insecure remote management", "Telnet transmits credentials in clear text. Replace with SSH or disable.", "Port Security"),
    (69, "TFTP", "High", "Unauthenticated file transfer", "TFTP is unauthenticated and commonly used for device configs. Disable unless specifically required.", "Port Security"),
    (79, "Finger", "Medium", "Legacy information disclosure", "Finger may disclose user/system information. Disable unless explicitly required.", "Port Security"),
    (80, "HTTP", "Medium", "Unencrypted web management", "HTTP management should redirect to HTTPS or be disabled when not required.", "Port Security / Q1173"),
    (161, "SNMP", "Medium", "Network management", "Validate SNMP version, community strings, and authorized management hosts. Prefer SNMPv3.", "Port Security"),
    (162, "SNMP Trap", "Medium", "Network management", "Validate SNMP trap configuration and authorized destinations.", "Port Security"),
    (389, "LDAP", "Medium", "Directory service exposure", "Validate need and protection of directory service exposure.", "Server / Directory Review"),
    (445, "SMB", "Medium", "File sharing", "Validate SMB exposure, segmentation, and business need.", "Server / Share Review"),
    (515, "LPD", "Medium", "Printer service", "Validate MFD baseline and whether LPD is required.", "Q1143"),
    (631, "IPP", "Medium", "Printer service", "Validate MFD baseline and whether IPP is required.", "Q1143"),
    (9100, "JetDirect", "Medium", "Raw printer service", "Validate MFD baseline and avoid unnecessary duplicate print protocols.", "Q1143"),
    (1900, "UPnP", "Medium", "UPnP discovery/control", "UPnP is generally unnecessary on managed networks. Disable unless explicitly required.", "Port Security"),
    (2049, "NFS", "High", "Network file sharing", "NFS should not be exposed unless specifically required and restricted.", "Server / Share Review"),
    (5900, "VNC", "High", "Remote desktop", "VNC should be disabled unless explicitly required and securely controlled.", "Remote Access"),
    (3389, "RDP", "High", "Remote desktop", "RDP should be restricted and only enabled where required.", "Remote Access"),
    (8080, "HTTP Alternate", "Medium", "Web management", "Possible web management interface. Verify business need and secure configuration.", "Port Security / Q1173"),
    (8443, "HTTPS Alternate", "Low", "Web management", "Possible secure web management interface. Validate authentication and certificate configuration.", "Port Security / Q1173")
]

kb_port_rules = spark.createDataFrame(
    port_rules,
    [
        "port",
        "port_label",
        "review_priority",
        "risk_theme",
        "standard_recommendation",
        "csa_relevance"
    ]
)

display(kb_port_rules)

save_delta(kb_port_rules, f"{output_schema}.rcaip_kb_port_rules")


# COMMAND ----------

# ============================================================================
# Cell 8 - CSA Question Mapping
#
# Purpose:
#   Map RCAIP evidence products to CSA question areas.
# ============================================================================

csa_question_map = [
    ("Q1143", "DCPS / MFD Configuration", "Printer/MFD inventory, open printer services, MFD baseline indicators", "NMAP, CMDB, MDE, Tenable", "IP-001, IP-002", "Partial"),
    ("Q1173", "Enterprise WiFi Management Protocols", "Open/insecure management protocols on EntWifi scan scope", "NMAP", "IP-001", "Strong"),
    ("Q1182.01", "NEN Hardware/Software Inventory", "NEN asset observations, Qualys coverage, NMAP public exposure, CMDB gaps", "NMAP, Qualys, CMDB", "IP-002", "Partial"),
    ("Q1183.01", "NEN Vulnerability Management", "Qualys coverage and observed public/NEN assets", "Qualys, NMAP", "Future IP-003", "Partial"),
    ("Q1189", "Umbrella Internet Visibility", "Public IP/CIDR validation worksheet", "Umbrella manual validation, NEN targets", "Future IP-003", "Manual"),
    ("Q1191.01", "NEN Segmentation", "NEN asset domains, public ranges, scan scope/location, observed services", "NMAP, Qualys, Post validation", "Future IP-003", "Partial"),
    ("Q1192.01", "NEN Security Logs", "NEN endpoints/assets requiring logging validation", "NMAP, Qualys, local collection", "Future local/NEN module", "Future"),
    ("Asset Boundary", "Post/Enterprise Asset Boundary", "CMDB-only assets and observed-not-CMDB assets", "CMDB, NMAP, Tenable, Intune, MDE, Qualys", "IP-002", "Strong"),
    ("Endpoint Coverage", "Required endpoint security tooling", "MDE, Intune, Tenable, Qualys coverage by asset class/domain", "CMDB, MDE, Intune, Tenable, Qualys", "Future IP-003", "Strong")
]

kb_csa_question_map = spark.createDataFrame(
    csa_question_map,
    [
        "question_id",
        "question_theme",
        "evidence_needed",
        "rcaip_sources",
        "rcaip_product",
        "coverage_status"
    ]
)

display(kb_csa_question_map)

save_delta(kb_csa_question_map, f"{output_schema}.rcaip_kb_csa_question_map")


# COMMAND ----------

# ============================================================================
# Cell 9 - Test KB Classification Against Observed-Not-CMDB
#
# Purpose:
#   Validate that the asset classification KB improves observed-not-CMDB triage.
# ============================================================================

observed_not_cmdb = spark.table(f"{output_schema}.{post_slug}_observed_not_cmdb")
patterns = spark.table(f"{output_schema}.rcaip_kb_asset_patterns")

observed_blob = (
    observed_not_cmdb
    .withColumn(
        "name_blob",
        lower(
            concat_ws(
                " ",
                col("asset_key"),
                col("host_key"),
                col("observed_ip"),
                col("observed_name"),
                col("observed_sources"),
                col("asset_domain")
            )
        )
    )
)

matches = (
    observed_blob.alias("o")
    .join(
        broadcast(patterns).alias("p"),
        expr("o.name_blob rlike p.pattern_regex"),
        "left"
    )
    .withColumn(
        "match_score",
        coalesce(col("p.confidence_weight"), lit(0))
    )
)

classification_window = Window.partitionBy(
    "o.asset_key",
    "o.host_key",
    "o.observed_ip",
    "o.observed_name",
    "o.asset_domain"
).orderBy(desc("match_score"))

classified = (
    matches
    .withColumn("rn", row_number().over(classification_window))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        col("o.asset_key"),
        col("o.host_key"),
        col("o.observed_ip"),
        col("o.observed_name"),
        col("o.asset_domain"),
        col("o.observed_sources"),
        col("o.observed_source_count"),
        coalesce(col("p.likely_asset_type"), lit("Unknown / Low Context")).alias("likely_asset_type"),
        coalesce(col("p.asset_family"), lit("Unknown")).alias("asset_family"),
        coalesce(col("p.asset_domain_hint"), col("o.asset_domain")).alias("asset_domain_hint"),
        coalesce(col("p.pattern_confidence"), lit("Low")).alias("pattern_confidence"),
        coalesce(col("p.confidence_weight"), lit(0)).alias("classification_score"),
        coalesce(col("p.pattern_reason"), lit("No knowledge-base pattern matched.")).alias("classification_reason"),
        coalesce(col("p.recommended_use"), lit("Review manually or retain in technical appendix.")).alias("classification_recommended_use")
    )
)

display(
    classified
    .groupBy("likely_asset_type", "asset_family", "pattern_confidence")
    .count()
    .orderBy(desc("count"))
)

display(classified.limit(100))

save_delta(classified, f"{output_schema}.{post_slug}_kb_test_observed_not_cmdb_classified")


# COMMAND ----------

# ============================================================================
# Cell 10 - Test Observation Interpretation Rules
#
# Purpose:
#   Apply first-pass RCAIP observation interpretation to observed-not-CMDB.
# ============================================================================

classified = spark.table(f"{output_schema}.{post_slug}_kb_test_observed_not_cmdb_classified")

interpreted = (
    classified
    .withColumn(
        "has_name_evidence",
        when(
            col("observed_name").isNotNull() &
            (trim(col("observed_name")) != "") &
            (~lower(col("observed_name")).rlike("^\\d+\\.\\d+\\.\\d+\\.\\d+")),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "interpretation_rule_id",
        when(
            (col("observed_sources") == "NMAP") &
            (col("has_name_evidence") == "N") &
            (col("observed_source_count") == 1),
            lit("nmap_only_low_context")
        )
        .when(
            (col("observed_sources").rlike("NMAP")) &
            (col("has_name_evidence") == "Y") &
            (col("observed_source_count") == 1),
            lit("nmap_only_meaningful_service")
        )
        .when(
            (col("observed_sources").rlike("NMAP")) &
            (col("observed_sources").rlike("Tenable")),
            lit("nmap_plus_tenable")
        )
        .when(
            (col("observed_sources").rlike("Intune|MDE")) &
            (col("observed_ip").isNull()),
            lit("mde_intune_only_no_ip")
        )
        .when(
            (col("asset_domain") == "NEN") &
            (col("observed_sources").rlike("NMAP|Qualys")),
            lit("nen_public_observed_no_qualys")
        )
        .otherwise(lit("manual_review"))
    )
)

rules = spark.table(f"{output_schema}.rcaip_kb_observation_rules")

interpreted_with_rules = (
    interpreted.alias("i")
    .join(
        rules.alias("r"),
        col("i.interpretation_rule_id") == col("r.rule_id"),
        "left"
    )
    .select(
        col("i.*"),
        coalesce(col("r.rule_name"), lit("Manual Review")).alias("rule_name"),
        coalesce(col("r.interpretation"), lit("Manual review required.")).alias("interpretation"),
        coalesce(col("r.default_priority"), lit("Review")).alias("kb_priority"),
        coalesce(col("r.recommended_owner"), lit("RCSO Review")).alias("kb_recommended_owner"),
        coalesce(col("r.recommended_action"), lit("Review manually.")).alias("kb_recommended_action")
    )
)

display(
    interpreted_with_rules
    .groupBy("interpretation_rule_id", "rule_name", "kb_priority")
    .count()
    .orderBy(desc("count"))
)

display(interpreted_with_rules.limit(100))

save_delta(interpreted_with_rules, f"{output_schema}.{post_slug}_kb_test_observed_not_cmdb_interpreted")


# COMMAND ----------

# ============================================================================
# Cell 11 - Final Verification
# ============================================================================

spark.sql(f"SHOW TABLES IN {output_schema} LIKE 'rcaip_kb*'").show(truncate=False)
spark.sql(f"SHOW TABLES IN {output_schema} LIKE '{post_slug}_kb_test*'").show(truncate=False)

show_table(f"{output_schema}.rcaip_kb_isso_subnet_lookup")
show_table(f"{output_schema}.rcaip_kb_asset_patterns")
show_table(f"{output_schema}.rcaip_kb_observation_rules")
show_table(f"{output_schema}.rcaip_kb_required_tools")
show_table(f"{output_schema}.rcaip_kb_port_rules")
show_table(f"{output_schema}.rcaip_kb_csa_question_map")


# COMMAND ----------

# ============================================================================
# RCAIP KNOWLEDGE BASE 001
# Existing Knowledge Base Inventory
#
# Purpose:
#   Discover the current Knowledge Base variables, DataFrames, durable tables,
#   schemas, and row counts before integrating the new central configuration.
#
# Safety:
#   - No tables are created.
#   - No tables are replaced.
#   - No data is modified.
#   - No configuration is changed.
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame


# ---------------------------------------------------------------------------
# 1. Define the namespace currently used by RCAIP
# ---------------------------------------------------------------------------

KNOWLEDGE_BASE_DISCOVERY_NAMESPACE = (
    "devresults"
)


# ---------------------------------------------------------------------------
# 2. Inventory likely Knowledge Base Python objects
# ---------------------------------------------------------------------------

knowledge_object_keywords = [
    "kb",
    "knowledge",
    "rule",
    "naming",
    "device",
    "taxonomy",
    "classification",
    "convention",
    "tooling",
    "source",
    "csa"
]


knowledge_object_rows = []


for (
    object_name,
    object_value
) in sorted(
    globals().items()
):

    normalized_object_name = (
        object_name
        .strip()
        .lower()
    )


    keyword_match = any(
        keyword in normalized_object_name

        for keyword
        in knowledge_object_keywords
    )


    if not keyword_match:

        continue


    object_type = type(
        object_value
    ).__name__


    object_size = None
    object_columns = None


    if isinstance(
        object_value,
        DataFrame
    ):

        object_size = None

        object_columns = " | ".join(
            object_value.columns
        )


    elif isinstance(
        object_value,
        (
            list,
            tuple,
            set,
            dict
        )
    ):

        object_size = len(
            object_value
        )


    knowledge_object_rows.append(
        (
            object_name,
            object_type,
            object_size,
            object_columns
        )
    )


knowledge_object_schema = T.StructType([
    T.StructField(
        "object_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "object_type",
        T.StringType(),
        False
    ),

    T.StructField(
        "object_size",
        T.LongType(),
        True
    ),

    T.StructField(
        "dataframe_columns",
        T.StringType(),
        True
    )
])


rcaip_existing_knowledge_objects_df = (
    spark.createDataFrame(
        knowledge_object_rows,
        schema=knowledge_object_schema
    )
)


print(
    "Existing Knowledge Base-related Python objects"
)


display(
    rcaip_existing_knowledge_objects_df
    .orderBy(
        "object_name"
    )
)


# ---------------------------------------------------------------------------
# 3. List all registered tables in the current results namespace
# ---------------------------------------------------------------------------

namespace_tables_df = spark.sql(
    f"SHOW TABLES IN {KNOWLEDGE_BASE_DISCOVERY_NAMESPACE}"
)


table_name_column = next(
    column_name

    for column_name
    in namespace_tables_df.columns

    if column_name.lower() == "tablename"
)


knowledge_table_keywords = [
    "kb",
    "knowledge",
    "rule",
    "naming",
    "device_type",
    "taxonomy",
    "classification",
    "convention",
    "tooling",
    "source_registry"
]


knowledge_table_filter = None


for keyword in knowledge_table_keywords:

    keyword_condition = F.lower(
        F.col(
            table_name_column
        )
    ).contains(
        keyword
    )


    knowledge_table_filter = (
        keyword_condition

        if knowledge_table_filter is None

        else
        (
            knowledge_table_filter
            |
            keyword_condition
        )
    )


candidate_knowledge_tables_df = (
    namespace_tables_df
    .filter(
        knowledge_table_filter
    )
    .select(
        F.col(
            table_name_column
        ).alias(
            "table_name"
        )
    )
    .distinct()
    .orderBy(
        "table_name"
    )
)


candidate_knowledge_table_names = [
    row[
        "table_name"
    ]

    for row
    in candidate_knowledge_tables_df.collect()
]


print(
    "Candidate durable Knowledge Base tables"
)


display(
    candidate_knowledge_tables_df
)


# ---------------------------------------------------------------------------
# 4. Inspect the schema and row count of each candidate table
# ---------------------------------------------------------------------------

knowledge_table_inventory_rows = []


for table_name in candidate_knowledge_table_names:

    fully_qualified_table_name = (
        f"{KNOWLEDGE_BASE_DISCOVERY_NAMESPACE}."
        f"{table_name}"
    )


    try:

        candidate_table_df = spark.table(
            fully_qualified_table_name
        )


        table_row_count = candidate_table_df.count()


        table_schema_text = " | ".join([
            (
                field.name
                +
                ":"
                +
                field.dataType.simpleString()
            )

            for field
            in candidate_table_df.schema.fields
        ])


        table_status = (
            "AVAILABLE"
        )


        table_error = None


    except Exception as table_exception:

        table_row_count = None

        table_schema_text = None

        table_status = (
            "ERROR"
        )

        table_error = str(
            table_exception
        )[
            :1000
        ]


    knowledge_table_inventory_rows.append(
        (
            fully_qualified_table_name,
            table_status,
            table_row_count,
            table_schema_text,
            table_error
        )
    )


knowledge_table_inventory_schema = T.StructType([
    T.StructField(
        "table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "table_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "row_count",
        T.LongType(),
        True
    ),

    T.StructField(
        "schema",
        T.StringType(),
        True
    ),

    T.StructField(
        "error",
        T.StringType(),
        True
    )
])


rcaip_existing_knowledge_tables_df = (
    spark.createDataFrame(
        knowledge_table_inventory_rows,
        schema=knowledge_table_inventory_schema
    )
)


print(
    "Existing durable Knowledge Base table inventory"
)


display(
    rcaip_existing_knowledge_tables_df
    .orderBy(
        "table_name"
    )
)


# ---------------------------------------------------------------------------
# 5. Identify likely output tables referenced by string variables
#
# Some notebooks store table names in variables that do not include words
# such as knowledge or rule. This section finds string variables that point
# into devresults.
# ---------------------------------------------------------------------------

referenced_table_rows = []


for (
    object_name,
    object_value
) in sorted(
    globals().items()
):

    if not isinstance(
        object_value,
        str
    ):

        continue


    normalized_value = (
        object_value
        .strip()
        .lower()
    )


    if normalized_value.startswith(
        KNOWLEDGE_BASE_DISCOVERY_NAMESPACE
        +
        "."
    ):

        table_available = spark.catalog.tableExists(
            object_value
        )


        referenced_table_rows.append(
            (
                object_name,
                object_value,
                table_available
            )
        )


referenced_table_schema = T.StructType([
    T.StructField(
        "variable_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "referenced_table",
        T.StringType(),
        False
    ),

    T.StructField(
        "table_available",
        T.BooleanType(),
        False
    )
])


rcaip_referenced_tables_df = (
    spark.createDataFrame(
        referenced_table_rows,
        schema=referenced_table_schema
    )
)


print(
    "Table names referenced by current Knowledge Base variables"
)


display(
    rcaip_referenced_tables_df
    .orderBy(
        "variable_name"
    )
)


# ---------------------------------------------------------------------------
# 6. Final discovery status
# ---------------------------------------------------------------------------

print("=" * 80)

print(
    "Status:",
    "PASS"
)

print(
    "Knowledge Base-related Python objects:",
    len(
        knowledge_object_rows
    )
)

print(
    "Candidate durable Knowledge Base tables:",
    len(
        candidate_knowledge_table_names
    )
)

print(
    "Referenced devresults table variables:",
    len(
        referenced_table_rows
    )
)

print(
    "Namespace inspected:",
    KNOWLEDGE_BASE_DISCOVERY_NAMESPACE
)

print("=" * 80)

print(
    "No existing Knowledge Base objects or tables were modified."
)


# COMMAND ----------

# ============================================================================
# RCAIP KNOWLEDGE BASE 001
# Compact Existing Knowledge Base Map
#
# Purpose:
#   Print the exact durable table names, row counts, schemas, and referenced
#   table variables needed to safely integrate the centralized configuration.
#
# Safety:
#   - No tables are created.
#   - No tables are replaced.
#   - No data is modified.
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Confirm that the earlier inventory products are available
# ---------------------------------------------------------------------------

required_inventory_objects = [
    "rcaip_existing_knowledge_tables_df",
    "rcaip_referenced_tables_df",
    "rcaip_existing_knowledge_objects_df"
]


missing_inventory_objects = [
    object_name

    for object_name
    in required_inventory_objects

    if object_name not in globals()
]


if missing_inventory_objects:

    raise RuntimeError(
        "Run the previous Knowledge Base inventory cell first. "
        "Missing objects: "
        +
        " | ".join(
            missing_inventory_objects
        )
    )


# ---------------------------------------------------------------------------
# 2. Collect the durable Knowledge Base table inventory
# ---------------------------------------------------------------------------

durable_knowledge_tables = (
    rcaip_existing_knowledge_tables_df
    .orderBy(
        "table_name"
    )
    .collect()
)


print("=" * 100)
print("DURABLE KNOWLEDGE BASE TABLES")
print("=" * 100)


if not durable_knowledge_tables:

    print(
        "No candidate durable Knowledge Base tables were identified."
    )


for table_number, table_record in enumerate(
    durable_knowledge_tables,
    start=1
):

    print()

    print(
        f"TABLE {table_number}:",
        table_record[
            "table_name"
        ]
    )

    print(
        "STATUS:",
        table_record[
            "table_status"
        ]
    )

    print(
        "ROW COUNT:",
        table_record[
            "row_count"
        ]
    )

    print(
        "SCHEMA:"
    )

    print(
        table_record[
            "schema"
        ]
        or
        "SCHEMA NOT AVAILABLE"
    )


    if table_record[
        "error"
    ]:

        print(
            "ERROR:",
            table_record[
                "error"
            ]
        )


# ---------------------------------------------------------------------------
# 3. Collect table names referenced by current notebook variables
# ---------------------------------------------------------------------------

referenced_knowledge_tables = (
    rcaip_referenced_tables_df
    .orderBy(
        "variable_name"
    )
    .collect()
)


print()
print("=" * 100)
print("TABLE VARIABLES REFERENCED BY THE KNOWLEDGE BASE NOTEBOOK")
print("=" * 100)


if not referenced_knowledge_tables:

    print(
        "No devresults table variables were identified."
    )


for reference_number, reference_record in enumerate(
    referenced_knowledge_tables,
    start=1
):

    print()

    print(
        f"REFERENCE {reference_number}:",
        reference_record[
            "variable_name"
        ]
    )

    print(
        "TABLE:",
        reference_record[
            "referenced_table"
        ]
    )

    print(
        "AVAILABLE:",
        reference_record[
            "table_available"
        ]
    )


# ---------------------------------------------------------------------------
# 4. Print existing Knowledge Base-related Python objects
# ---------------------------------------------------------------------------

knowledge_python_objects = (
    rcaip_existing_knowledge_objects_df
    .orderBy(
        "object_name"
    )
    .collect()
)


print()
print("=" * 100)
print("KNOWLEDGE BASE PYTHON OBJECTS")
print("=" * 100)


for object_number, object_record in enumerate(
    knowledge_python_objects,
    start=1
):

    print()

    print(
        f"OBJECT {object_number}:",
        object_record[
            "object_name"
        ]
    )

    print(
        "TYPE:",
        object_record[
            "object_type"
        ]
    )

    print(
        "SIZE:",
        object_record[
            "object_size"
        ]
    )

    print(
        "DATAFRAME COLUMNS:",
        object_record[
            "dataframe_columns"
        ]
        or
        "NOT A DATAFRAME"
    )


# ---------------------------------------------------------------------------
# 5. Identify exact duplicate table references
#
# This shows whether several variables point to the same durable table.
# ---------------------------------------------------------------------------

reference_target_counts = {}


for reference_record in referenced_knowledge_tables:

    referenced_table_name = reference_record[
        "referenced_table"
    ]


    reference_target_counts[
        referenced_table_name
    ] = (
        reference_target_counts.get(
            referenced_table_name,
            0
        )
        +
        1
    )


duplicate_reference_targets = {
    table_name: reference_count

    for (
        table_name,
        reference_count
    ) in reference_target_counts.items()

    if reference_count > 1
}


print()
print("=" * 100)
print("DUPLICATE TABLE REFERENCE TARGETS")
print("=" * 100)


if not duplicate_reference_targets:

    print(
        "None"
    )


else:

    for (
        table_name,
        reference_count
    ) in sorted(
        duplicate_reference_targets.items()
    ):

        print(
            table_name,
            "referenced by",
            reference_count,
            "variables"
        )


# ---------------------------------------------------------------------------
# 6. Final status
# ---------------------------------------------------------------------------

print()
print("=" * 100)

print(
    "Status:",
    "PASS"
)

print(
    "Durable Knowledge Base tables:",
    len(
        durable_knowledge_tables
    )
)

print(
    "Referenced table variables:",
    len(
        referenced_knowledge_tables
    )
)

print(
    "Knowledge Base Python objects:",
    len(
        knowledge_python_objects
    )
)

print("=" * 100)


# COMMAND ----------

# ============================================================================
# RCAIP KNOWLEDGE BASE 001
# Phase 2 - Knowledge Base V2 Expansion
#
# Purpose:
#   Extend the current RCAIP Knowledge Base without modifying or replacing
#   the original example_post_d-validated Knowledge Base tables.
#
# This phase creates:
#
#   1. Asset naming patterns V2
#   2. Observation rules V2
#   3. Required-tool expectations V2
#   4. Mobile-computer device profiles
#   5. Token-specific tooling expectations
#   6. Guarded naming-token rules
#   7. Enterprise source registry
#   8. Terminology rules
#   9. Knowledge Base release manifest
#
# Guardrails:
#
#   - Existing Knowledge Base tables are read but not modified.
#   - Device-name tokens provide analytical evidence, not definitive proof.
#   - HU, MED, and HR are retained as candidate naming tokens only until
#     their authoritative meanings and valid positions are confirmed.
#   - GoMobile is a management or registration source, not a device type.
#   - TFL, EMD, MID, SAKIO, and SUKIO represent mobile computers,
#     generally laptops.
# ============================================================================


# ---------------------------------------------------------------------------
# 1. Imports
# ---------------------------------------------------------------------------

import json
import os

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 2. Confirm that the central configuration has been loaded
# ---------------------------------------------------------------------------

required_configuration_objects = [
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "RCAIP_RELEASE_VERSION",
    "RCAIP_ASSESSMENT_START_DATE",
    "RCAIP_DEVICE_TYPE_RULES",
    "RCAIP_RESOLVED_SOURCE_TABLES",
    "RCAIP_WORKSPACE_MANIFEST_ROOT"
]


missing_configuration_objects = [
    object_name

    for object_name
    in required_configuration_objects

    if object_name not in globals()
]


if missing_configuration_objects:

    raise RuntimeError(
        "The central RCAIP configuration has not been loaded. "
        "Run this cell at the top of the notebook first:\n\n"
        "%run ./RCAIP_Config_000_Post_Run_Config\n\n"
        "Missing configuration objects: "
        +
        " | ".join(
            missing_configuration_objects
        )
    )


# ---------------------------------------------------------------------------
# 3. Define the original example_post_d-validated Knowledge Base tables
# ---------------------------------------------------------------------------

KB_ASSET_PATTERNS_V1_TABLE = (
    "devresults.rcaip_kb_asset_patterns"
)


KB_OBSERVATION_RULES_V1_TABLE = (
    "devresults.rcaip_kb_observation_rules"
)


KB_REQUIRED_TOOLS_V1_TABLE = (
    "devresults.rcaip_kb_required_tools"
)


# ---------------------------------------------------------------------------
# 4. Define the new V2 and supplemental Knowledge Base tables
# ---------------------------------------------------------------------------

KB_ASSET_PATTERNS_V2_TABLE = (
    "devresults.rcaip_kb_asset_patterns_v2"
)


KB_OBSERVATION_RULES_V2_TABLE = (
    "devresults.rcaip_kb_observation_rules_v2"
)


KB_REQUIRED_TOOLS_V2_TABLE = (
    "devresults.rcaip_kb_required_tools_v2"
)


KB_DEVICE_PROFILES_TABLE = (
    "devresults.rcaip_kb_device_profiles"
)


KB_DEVICE_TOOL_EXPECTATIONS_TABLE = (
    "devresults.rcaip_kb_device_tool_expectations"
)


KB_NAMING_TOKEN_RULES_TABLE = (
    "devresults.rcaip_kb_naming_token_rules"
)


KB_SOURCE_REGISTRY_TABLE = (
    "devresults.rcaip_kb_source_registry"
)


KB_TERMINOLOGY_RULES_TABLE = (
    "devresults.rcaip_kb_terminology_rules"
)


KB_RELEASE_MANIFEST_TABLE = (
    "devresults.rcaip_kb_release_manifest"
)


# ---------------------------------------------------------------------------
# 5. Validate the three original tables required for this expansion
# ---------------------------------------------------------------------------

required_original_tables = [
    KB_ASSET_PATTERNS_V1_TABLE,
    KB_OBSERVATION_RULES_V1_TABLE,
    KB_REQUIRED_TOOLS_V1_TABLE
]


missing_original_tables = [
    table_name

    for table_name
    in required_original_tables

    if not spark.catalog.tableExists(
        table_name
    )
]


if missing_original_tables:

    raise RuntimeError(
        "Required original Knowledge Base tables are unavailable: "
        +
        " | ".join(
            missing_original_tables
        )
    )


kb_asset_patterns_v1_df = spark.table(
    KB_ASSET_PATTERNS_V1_TABLE
)


kb_observation_rules_v1_df = spark.table(
    KB_OBSERVATION_RULES_V1_TABLE
)


kb_required_tools_v1_df = spark.table(
    KB_REQUIRED_TOOLS_V1_TABLE
)


kb_asset_patterns_v1_count = (
    kb_asset_patterns_v1_df.count()
)


kb_observation_rules_v1_count = (
    kb_observation_rules_v1_df.count()
)


kb_required_tools_v1_count = (
    kb_required_tools_v1_df.count()
)


# ---------------------------------------------------------------------------
# 6. Create the authoritative mobile-computer device profiles
#
# There are four canonical classifications and five recognized tokens:
#
#   TFL
#   EMD
#   MID
#   SAKIO
#   SUKIO
#
# SAKIO and SUKIO currently map to the same canonical shared or kiosk-style
# mobile-computer class.
# ---------------------------------------------------------------------------

device_profile_rows = []


for device_rule in RCAIP_DEVICE_TYPE_RULES:

    for observed_token in device_rule[
        "observed_tokens"
    ]:

        normalized_token = (
            str(
                observed_token
            )
            .strip()
            .upper()
        )


        device_profile_id = (
            "DEVICE-PROFILE-"
            +
            normalized_token
        )


        device_profile_rows.append(
            (
                device_profile_id,

                normalized_token,

                device_rule[
                    "canonical_device_type"
                ],

                device_rule[
                    "device_form"
                ],

                device_rule[
                    "issuance_model"
                ],

                device_rule[
                    "assignment_model"
                ],

                device_rule[
                    "management_model"
                ],

                device_rule[
                    "opennet_expectation"
                ],

                " | ".join(
                    device_rule[
                        "required_tools"
                    ]
                ),

                " | ".join(
                    device_rule[
                        "possible_additional_sources"
                    ]
                ),

                device_rule[
                    "rule_confidence"
                ],

                device_rule[
                    "rule_guardrail"
                ],

                "example_post_d beta field validation and RCSO operational knowledge",

                RCAIP_RELEASE_VERSION,

                RCAIP_ASSESSMENT_START_DATE,

                True,

                datetime.utcnow()
            )
        )


device_profile_schema = T.StructType([
    T.StructField(
        "device_profile_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "observed_token",
        T.StringType(),
        False
    ),

    T.StructField(
        "canonical_device_type",
        T.StringType(),
        False
    ),

    T.StructField(
        "device_form",
        T.StringType(),
        False
    ),

    T.StructField(
        "issuance_model",
        T.StringType(),
        False
    ),

    T.StructField(
        "assignment_model",
        T.StringType(),
        False
    ),

    T.StructField(
        "management_model",
        T.StringType(),
        False
    ),

    T.StructField(
        "opennet_expectation",
        T.StringType(),
        False
    ),

    T.StructField(
        "required_tools",
        T.StringType(),
        False
    ),

    T.StructField(
        "possible_additional_sources",
        T.StringType(),
        False
    ),

    T.StructField(
        "rule_confidence",
        T.StringType(),
        False
    ),

    T.StructField(
        "rule_guardrail",
        T.StringType(),
        False
    ),

    T.StructField(
        "source_basis",
        T.StringType(),
        False
    ),

    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "effective_date",
        T.DateType(),
        False
    ),

    T.StructField(
        "active_indicator",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "created_at",
        T.TimestampType(),
        False
    )
])


kb_device_profiles_df = (
    spark.createDataFrame(
        device_profile_rows,
        schema=device_profile_schema
    )

    .dropDuplicates([
        "observed_token"
    ])
)


# ---------------------------------------------------------------------------
# 7. Create token-specific device tooling expectations
#
# Confirmed expectations:
#
#   TFL:
#       MDE
#
#   EMD:
#       MDE
#
#   MID:
#       MDE
#       Intune
#       Qualys
#
#   SAKIO:
#       MDE
#       Intune
#       Qualys
#
#   SUKIO:
#       MDE
#       Intune
#       Qualys
#
# GoMobile may also contain TFL and EMD registrations, but GoMobile is not
# currently treated as required tooling for those classifications.
# ---------------------------------------------------------------------------

device_tool_expectation_rows = []


for device_rule in RCAIP_DEVICE_TYPE_RULES:

    for observed_token in device_rule[
        "observed_tokens"
    ]:

        normalized_token = (
            str(
                observed_token
            )
            .strip()
            .upper()
        )


        for tool_name in device_rule[
            "required_tools"
        ]:

            expectation_id = (
                "TOOL-EXPECTATION-"
                +
                normalized_token
                +
                "-"
                +
                tool_name.upper()
                .replace(
                    " ",
                    "_"
                )
            )


            expectation_reason = (
                f"{normalized_token} is classified as "
                f"{device_rule['device_form']}. "
                f"{tool_name} evidence is expected under the current "
                f"RCAIP device-management rule."
            )


            device_tool_expectation_rows.append(
                (
                    expectation_id,

                    normalized_token,

                    device_rule[
                        "canonical_device_type"
                    ],

                    tool_name,

                    "Required",

                    expectation_reason,

                    (
                        "Tool presence and current reporting require "
                        "independent source validation."
                    ),

                    RCAIP_RELEASE_VERSION,

                    RCAIP_ASSESSMENT_START_DATE,

                    True,

                    datetime.utcnow()
                )
            )


device_tool_expectation_schema = T.StructType([
    T.StructField(
        "expectation_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "observed_token",
        T.StringType(),
        False
    ),

    T.StructField(
        "canonical_device_type",
        T.StringType(),
        False
    ),

    T.StructField(
        "tool_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "expected_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "expectation_reason",
        T.StringType(),
        False
    ),

    T.StructField(
        "validation_guardrail",
        T.StringType(),
        False
    ),

    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "effective_date",
        T.DateType(),
        False
    ),

    T.StructField(
        "active_indicator",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "created_at",
        T.TimestampType(),
        False
    )
])


kb_device_tool_expectations_df = (
    spark.createDataFrame(
        device_tool_expectation_rows,
        schema=device_tool_expectation_schema
    )

    .dropDuplicates([
        "observed_token",
        "tool_name"
    ])
)


# ---------------------------------------------------------------------------
# 8. Create V2 asset-name patterns
#
# These patterns require a non-alphanumeric boundary around each token.
#
# This intentionally favors precision over recall. A token found in an
# unexpected location or concatenated without boundaries should remain a
# candidate for manual review rather than receiving an automatic high-
# confidence classification.
# ---------------------------------------------------------------------------

mobile_pattern_rows = [
    (
        "PATTERN-MOBILE-TFL-001",
        r"(?i)(^|[^A-Z0-9])TFL([^A-Z0-9]|$)",
        "Mobile Computer - Generally Laptop",
        "TFL",
        "Enterprise Managed Endpoint",
        "High",
        90,
        (
            "Delimited TFL token indicates an enterprise-issued mobile "
            "computer, generally a laptop."
        ),
        (
            "Use as device-type evidence and corroborate with current "
            "enterprise management sources."
        )
    ),

    (
        "PATTERN-MOBILE-EMD-001",
        r"(?i)(^|[^A-Z0-9])EMD([^A-Z0-9]|$)",
        "Mobile Computer - Generally Laptop",
        "EMD",
        "Enterprise Managed Endpoint",
        "High",
        90,
        (
            "Delimited EMD token indicates an enterprise-managed mobile "
            "computer, generally a laptop."
        ),
        (
            "Use as device-type evidence and corroborate with current "
            "enterprise management sources."
        )
    ),

    (
        "PATTERN-MOBILE-MID-001",
        r"(?i)(^|[^A-Z0-9])MID([^A-Z0-9]|$)",
        "Mobile Computer - Generally Laptop",
        "MID",
        "Post-Issued Managed Endpoint",
        "High",
        90,
        (
            "Delimited MID token indicates a Post-issued mobile computer, "
            "generally a laptop."
        ),
        (
            "Do not require OpenNet evidence by itself. Validate MDE, Intune, "
            "and Qualys coverage."
        )
    ),

    (
        "PATTERN-MOBILE-SAKIO-001",
        r"(?i)(^|[^A-Z0-9])SAKIO([^A-Z0-9]|$)",
        "Mobile Computer - Generally Laptop",
        "SHARED_KIOSK_MOBILE_COMPUTER",
        "Shared or Kiosk-Style Endpoint",
        "Medium",
        80,
        (
            "Delimited SAKIO token indicates a shared or kiosk-style mobile "
            "computer, generally a laptop."
        ),
        (
            "Validate device use with Post and confirm MDE, Intune, and "
            "Qualys coverage."
        )
    ),

    (
        "PATTERN-MOBILE-SUKIO-001",
        r"(?i)(^|[^A-Z0-9])SUKIO([^A-Z0-9]|$)",
        "Mobile Computer - Generally Laptop",
        "SHARED_KIOSK_MOBILE_COMPUTER",
        "Shared or Kiosk-Style Endpoint",
        "Medium",
        80,
        (
            "Delimited SUKIO token indicates a shared or kiosk-style mobile "
            "computer, generally a laptop."
        ),
        (
            "Validate device use with Post and confirm MDE, Intune, and "
            "Qualys coverage."
        )
    )
]


mobile_pattern_schema = T.StructType([
    T.StructField(
        "pattern_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "pattern_regex",
        T.StringType(),
        False
    ),

    T.StructField(
        "likely_asset_type",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_family",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_domain_hint",
        T.StringType(),
        False
    ),

    T.StructField(
        "pattern_confidence",
        T.StringType(),
        False
    ),

    T.StructField(
        "confidence_weight",
        T.LongType(),
        False
    ),

    T.StructField(
        "pattern_reason",
        T.StringType(),
        False
    ),

    T.StructField(
        "recommended_use",
        T.StringType(),
        False
    )
])


kb_mobile_patterns_df = spark.createDataFrame(
    mobile_pattern_rows,
    schema=mobile_pattern_schema
)


new_mobile_pattern_ids = [
    row[
        "pattern_id"
    ]

    for row
    in kb_mobile_patterns_df.select(
        "pattern_id"
    ).collect()
]


kb_asset_patterns_v2_df = (
    kb_asset_patterns_v1_df
    .filter(
        ~F.col(
            "pattern_id"
        ).isin(
            new_mobile_pattern_ids
        )
    )

    .unionByName(
        kb_mobile_patterns_df
    )

    .dropDuplicates([
        "pattern_id"
    ])
)


# ---------------------------------------------------------------------------
# 9. Create V2 required-tool rules
#
# These rows operate at the canonical device-family level. Token-specific
# expectations remain available in rcaip_kb_device_tool_expectations.
# ---------------------------------------------------------------------------

new_required_tool_rows = [
    (
        "Mobile Computer",
        "TFL",
        "MDE",
        "Required",
        (
            "TFL is an enterprise-managed mobile-computer classification. "
            "Current MDE reporting is expected."
        )
    ),

    (
        "Mobile Computer",
        "EMD",
        "MDE",
        "Required",
        (
            "EMD is an enterprise-managed mobile-computer classification. "
            "Current MDE reporting is expected."
        )
    ),

    (
        "Mobile Computer",
        "MID",
        "MDE",
        "Required",
        (
            "MID is a Post-issued mobile-computer classification. "
            "Current MDE reporting is expected."
        )
    ),

    (
        "Mobile Computer",
        "MID",
        "Intune",
        "Required",
        (
            "MID is a Post-issued mobile-computer classification. "
            "Current Intune management evidence is expected."
        )
    ),

    (
        "Mobile Computer",
        "MID",
        "Qualys",
        "Required",
        (
            "MID is a Post-issued mobile-computer classification. "
            "Current Qualys coverage is expected."
        )
    ),

    (
        "Mobile Computer",
        "SHARED_KIOSK_MOBILE_COMPUTER",
        "MDE",
        "Required",
        (
            "SAKIO and SUKIO mobile computers are expected to report to MDE."
        )
    ),

    (
        "Mobile Computer",
        "SHARED_KIOSK_MOBILE_COMPUTER",
        "Intune",
        "Required",
        (
            "SAKIO and SUKIO mobile computers are expected to have current "
            "Intune management evidence."
        )
    ),

    (
        "Mobile Computer",
        "SHARED_KIOSK_MOBILE_COMPUTER",
        "Qualys",
        "Required",
        (
            "SAKIO and SUKIO mobile computers are expected to have current "
            "Qualys coverage."
        )
    )
]


new_required_tool_schema = T.StructType([
    T.StructField(
        "asset_domain",
        T.StringType(),
        False
    ),

    T.StructField(
        "asset_family",
        T.StringType(),
        False
    ),

    T.StructField(
        "tool_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "expected_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "expectation_reason",
        T.StringType(),
        False
    )
])


kb_new_required_tools_df = spark.createDataFrame(
    new_required_tool_rows,
    schema=new_required_tool_schema
)


new_required_tool_keys_df = (
    kb_new_required_tools_df
    .select(
        "asset_domain",
        "asset_family",
        "tool_name"
    )
    .distinct()
)


kb_existing_required_tools_without_replacements_df = (
    kb_required_tools_v1_df.alias(
        "existing"
    )

    .join(
        new_required_tool_keys_df.alias(
            "new_keys"
        ),

        on=[
            "asset_domain",
            "asset_family",
            "tool_name"
        ],

        how="left_anti"
    )
)


kb_required_tools_v2_df = (
    kb_existing_required_tools_without_replacements_df
    .unionByName(
        kb_new_required_tools_df
    )

    .dropDuplicates([
        "asset_domain",
        "asset_family",
        "tool_name"
    ])
)


# ---------------------------------------------------------------------------
# 10. Create V2 observation rules
# ---------------------------------------------------------------------------

new_observation_rule_rows = [
    (
        "OBS-MOBILE-TOOLING-MISSING",
        "Required Mobile-Computer Tooling Missing",
        (
            "A device classified as TFL, EMD, MID, SAKIO, or SUKIO is missing "
            "one or more management or security tools expected for its "
            "classification."
        ),
        "High",
        "Post DT and Enterprise Tool Owner",
        (
            "Validate device identity and lifecycle, confirm source freshness, "
            "and restore or document the required tooling."
        )
    ),

    (
        "OBS-MID-NO-OPENNET",
        "MID Without OpenNet Evidence",
        (
            "MID is a Post-issued mobile-computer classification and may not "
            "normally appear in OpenNet-focused evidence. OpenNet absence alone "
            "is not sufficient to create a management finding."
        ),
        "Review",
        "Post DT",
        (
            "Confirm MID classification and validate current MDE, Intune, and "
            "Qualys evidence before determining disposition."
        )
    ),

    (
        "OBS-GOMOBILE-REGISTRATION",
        "GoMobile Registration Evidence",
        (
            "GoMobile presence represents management or registration evidence. "
            "It does not independently establish a unique device type or prove "
            "that the record represents one distinct physical asset."
        ),
        "Informational",
        "Post DT and Enterprise GoMobile Owner",
        (
            "Reconcile GoMobile identifiers with serial number, device name, "
            "user, platform, and current management timestamps before merging "
            "the record into the asset plane."
        )
    )
]


new_observation_rule_schema = T.StructType([
    T.StructField(
        "rule_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "rule_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "interpretation",
        T.StringType(),
        False
    ),

    T.StructField(
        "default_priority",
        T.StringType(),
        False
    ),

    T.StructField(
        "recommended_owner",
        T.StringType(),
        False
    ),

    T.StructField(
        "recommended_action",
        T.StringType(),
        False
    )
])


kb_new_observation_rules_df = spark.createDataFrame(
    new_observation_rule_rows,
    schema=new_observation_rule_schema
)


new_observation_rule_ids = [
    row[
        "rule_id"
    ]

    for row
    in kb_new_observation_rules_df.select(
        "rule_id"
    ).collect()
]


kb_observation_rules_v2_df = (
    kb_observation_rules_v1_df
    .filter(
        ~F.col(
            "rule_id"
        ).isin(
            new_observation_rule_ids
        )
    )

    .unionByName(
        kb_new_observation_rules_df
    )

    .dropDuplicates([
        "rule_id"
    ])
)


# ---------------------------------------------------------------------------
# 11. Create guarded HU, MED, and HR naming-token rules
#
# Their exact authoritative meanings are not yet confirmed.
#
# Therefore:
#
#   - They may support candidate attribution.
#   - They may not independently drive automated Post attribution.
#   - They may not independently establish device type.
#   - They require field and enterprise validation.
# ---------------------------------------------------------------------------

naming_token_rule_rows = [
    (
        "NAMING-TOKEN-HU-001",
        "HU",
        "Candidate Post or Organizational Naming Token",
        None,
        "Candidate Evidence Only",
        "Low",
        "FIELD_VALIDATION_REQUIRED",
        (
            "Do not use HU as authoritative Post attribution until its formal "
            "meaning, valid position, collision risk, and naming scope are "
            "confirmed."
        ),
        RCAIP_RELEASE_VERSION,
        RCAIP_ASSESSMENT_START_DATE,
        True,
        datetime.utcnow()
    ),

    (
        "NAMING-TOKEN-MED-001",
        "MED",
        "Candidate Post, Program, or Organizational Naming Token",
        None,
        "Candidate Evidence Only",
        "Low",
        "FIELD_VALIDATION_REQUIRED",
        (
            "Do not use MED as authoritative attribution or device-type proof "
            "until its formal meaning and valid naming context are confirmed."
        ),
        RCAIP_RELEASE_VERSION,
        RCAIP_ASSESSMENT_START_DATE,
        True,
        datetime.utcnow()
    ),

    (
        "NAMING-TOKEN-HR-001",
        "HR",
        "Candidate Post or Organizational Naming Token",
        None,
        "Candidate Evidence Only",
        "Low",
        "FIELD_VALIDATION_REQUIRED",
        (
            "Do not use HR as authoritative Post attribution until its formal "
            "meaning, valid position, collision risk, and naming scope are "
            "confirmed."
        ),
        RCAIP_RELEASE_VERSION,
        RCAIP_ASSESSMENT_START_DATE,
        True,
        datetime.utcnow()
    )
]


naming_token_rule_schema = T.StructType([
    T.StructField(
        "token_rule_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "observed_token",
        T.StringType(),
        False
    ),

    T.StructField(
        "token_category",
        T.StringType(),
        False
    ),

    T.StructField(
        "authoritative_meaning",
        T.StringType(),
        True
    ),

    T.StructField(
        "automation_use",
        T.StringType(),
        False
    ),

    T.StructField(
        "rule_confidence",
        T.StringType(),
        False
    ),

    T.StructField(
        "validation_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "rule_guardrail",
        T.StringType(),
        False
    ),

    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "effective_date",
        T.DateType(),
        False
    ),

    T.StructField(
        "active_indicator",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "created_at",
        T.TimestampType(),
        False
    )
])


kb_naming_token_rules_df = spark.createDataFrame(
    naming_token_rule_rows,
    schema=naming_token_rule_schema
)


# ---------------------------------------------------------------------------
# 12. Create the initial source registry
#
# The registry centralizes logical source meaning separately from physical
# table names.
#
# Source paths for MDE, Intune, Qualys, CMDB, and AD will be migrated into the
# configuration engine as their consuming notebooks are hardened.
# ---------------------------------------------------------------------------

resolved_aad_audit_table = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "o365_aad_audit"
    )
)


source_registry_rows = [
    (
        "CMDB",
        "Enterprise Asset Management",
        None,
        None,
        "Authoritative enterprise asset registration and ownership evidence",
        "Asset or configuration item",
        True,
        "NOTEBOOK_CONFIGURATION_MIGRATION_PENDING",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "MDE",
        "Endpoint Security",
        None,
        None,
        "Endpoint detection, security telemetry, and recent device visibility",
        "Managed endpoint",
        True,
        "NOTEBOOK_CONFIGURATION_MIGRATION_PENDING",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "INTUNE",
        "Endpoint Management",
        None,
        None,
        "Endpoint enrollment, management, platform, and compliance evidence",
        "Enrollment or managed device",
        True,
        "NOTEBOOK_CONFIGURATION_MIGRATION_PENDING",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "QUALYS",
        "Vulnerability Management",
        None,
        None,
        "Vulnerability-management coverage and observed asset evidence",
        "Scanned or managed asset",
        True,
        "NOTEBOOK_CONFIGURATION_MIGRATION_PENDING",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "ACTIVE_DIRECTORY",
        "Identity and Directory Services",
        None,
        None,
        "Computer, user, group, membership, and directory lifecycle evidence",
        "Directory object",
        True,
        "NOTEBOOK_CONFIGURATION_MIGRATION_PENDING",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "NMAP",
        "Assessment-Collected Network Evidence",
        None,
        None,
        (
            "Point-in-time network response, port, protocol, and service "
            "evidence collected during an authorized assessment"
        ),
        "Observed network host and scan result",
        False,
        "FILE_INPUT",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "GOMOBILE",
        "Mobile Device Registration and Management",
        None,
        None,
        (
            "Mobile-device registration or management evidence. The physical "
            "unit represented by each row must be confirmed during discovery."
        ),
        "Unknown until Databricks source discovery is completed",
        False,
        "DISCOVERY_REQUIRED",
        None,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    ),

    (
        "O365_AAD_AUDIT",
        "Identity Audit",
        "example_sources.o365_aad_audit_1",
        "example_sources.o365_aad_audit_2",
        "Azure Active Directory audit-event evidence",
        "Audit event",
        False,
        (
            "RESOLVED"
            if resolved_aad_audit_table is not None
            else
            "NOT_AVAILABLE"
        ),
        resolved_aad_audit_table,
        RCAIP_RELEASE_VERSION,
        datetime.utcnow()
    )
]


source_registry_schema = T.StructType([
    T.StructField(
        "logical_source_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "source_category",
        T.StringType(),
        False
    ),

    T.StructField(
        "preferred_table",
        T.StringType(),
        True
    ),

    T.StructField(
        "fallback_tables",
        T.StringType(),
        True
    ),

    T.StructField(
        "management_role",
        T.StringType(),
        False
    ),

    T.StructField(
        "unit_of_analysis",
        T.StringType(),
        False
    ),

    T.StructField(
        "required_for_assessment",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "source_resolution_status",
        T.StringType(),
        False
    ),

    T.StructField(
        "resolved_table",
        T.StringType(),
        True
    ),

    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "updated_at",
        T.TimestampType(),
        False
    )
])


kb_source_registry_df = spark.createDataFrame(
    source_registry_rows,
    schema=source_registry_schema
)


# ---------------------------------------------------------------------------
# 13. Create terminology rules
#
# These rules apply only to generated narrative and report text.
#
# Historical table names, source fields, immutable identifiers, and direct
# quotations must not be silently rewritten.
# ---------------------------------------------------------------------------

terminology_rule_rows = [
    (
        "TERM-DT-IRM-001",
        "IRM",
        "DT",
        "Generated narrative and report text",
        (
            "Do not alter historical table names, source-system values, "
            "immutable identifiers, or direct quotations."
        ),
        RCAIP_RELEASE_VERSION,
        True,
        datetime.utcnow()
    ),

    (
        "TERM-DT-FULL-001",
        "Information Resource Management",
        "Diplomatic Technology",
        "Generated narrative and report text",
        (
            "Do not alter historical table names, source-system values, "
            "immutable identifiers, or direct quotations."
        ),
        RCAIP_RELEASE_VERSION,
        True,
        datetime.utcnow()
    )
]


terminology_rule_schema = T.StructType([
    T.StructField(
        "terminology_rule_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "deprecated_term",
        T.StringType(),
        False
    ),

    T.StructField(
        "preferred_term",
        T.StringType(),
        False
    ),

    T.StructField(
        "replacement_scope",
        T.StringType(),
        False
    ),

    T.StructField(
        "exclusions",
        T.StringType(),
        False
    ),

    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "active_indicator",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "updated_at",
        T.TimestampType(),
        False
    )
])


kb_terminology_rules_df = spark.createDataFrame(
    terminology_rule_rows,
    schema=terminology_rule_schema
)


# ---------------------------------------------------------------------------
# 14. Create the Knowledge Base release manifest
# ---------------------------------------------------------------------------

release_manifest_rows = [
    (
        RCAIP_RELEASE_VERSION,
        "RCAIP Knowledge Base V2 Hardening Release",
        RCAIP_ASSESSMENT_START_DATE,
        RCAIP_POST_NAME,
        RCAIP_RUN_ID,
        (
            "Preserved the original example_post_d Knowledge Base and added mobile-"
            "computer classifications, required-tool expectations, guarded "
            "naming tokens, source registry, terminology rules, and V2 "
            "pattern and observation tables."
        ),
        True,
        datetime.utcnow()
    )
]


release_manifest_schema = T.StructType([
    T.StructField(
        "kb_version",
        T.StringType(),
        False
    ),

    T.StructField(
        "release_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "release_date",
        T.DateType(),
        False
    ),

    T.StructField(
        "validation_post",
        T.StringType(),
        False
    ),

    T.StructField(
        "validation_run_id",
        T.StringType(),
        False
    ),

    T.StructField(
        "change_summary",
        T.StringType(),
        False
    ),

    T.StructField(
        "current_indicator",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "created_at",
        T.TimestampType(),
        False
    )
])


kb_release_manifest_df = spark.createDataFrame(
    release_manifest_rows,
    schema=release_manifest_schema
)


# ---------------------------------------------------------------------------
# 15. Write the V2 and supplemental Knowledge Base tables
#
# These are new table names. The original V1 tables remain unchanged.
#
# Overwrite is intentional for these generated current-state tables so that
# rerunning this phase produces a deterministic result.
# ---------------------------------------------------------------------------

knowledge_base_write_contract = [
    (
        KB_ASSET_PATTERNS_V2_TABLE,
        kb_asset_patterns_v2_df
    ),

    (
        KB_OBSERVATION_RULES_V2_TABLE,
        kb_observation_rules_v2_df
    ),

    (
        KB_REQUIRED_TOOLS_V2_TABLE,
        kb_required_tools_v2_df
    ),

    (
        KB_DEVICE_PROFILES_TABLE,
        kb_device_profiles_df
    ),

    (
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE,
        kb_device_tool_expectations_df
    ),

    (
        KB_NAMING_TOKEN_RULES_TABLE,
        kb_naming_token_rules_df
    ),

    (
        KB_SOURCE_REGISTRY_TABLE,
        kb_source_registry_df
    ),

    (
        KB_TERMINOLOGY_RULES_TABLE,
        kb_terminology_rules_df
    ),

    (
        KB_RELEASE_MANIFEST_TABLE,
        kb_release_manifest_df
    )
]


for (
    destination_table,
    destination_df
) in knowledge_base_write_contract:

    (
        destination_df.write
        .format(
            "delta"
        )
        .mode(
            "overwrite"
        )
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            destination_table
        )
    )


# ---------------------------------------------------------------------------
# 16. Reload all written tables for independent validation
# ---------------------------------------------------------------------------

written_table_counts = {}


for (
    destination_table,
    destination_df
) in knowledge_base_write_contract:

    if not spark.catalog.tableExists(
        destination_table
    ):

        raise RuntimeError(
            "Expected Knowledge Base table was not created: "
            +
            destination_table
        )


    written_table_counts[
        destination_table
    ] = spark.table(
        destination_table
    ).count()


# ---------------------------------------------------------------------------
# 17. Validate required token and tooling contracts
# ---------------------------------------------------------------------------

required_device_tokens = {
    "TFL",
    "EMD",
    "MID",
    "SAKIO",
    "SUKIO"
}


actual_device_tokens = {
    row[
        "observed_token"
    ]

    for row
    in spark.table(
        KB_DEVICE_PROFILES_TABLE
    )
    .select(
        "observed_token"
    )
    .distinct()
    .collect()
}


missing_device_tokens = sorted(
    required_device_tokens
    -
    actual_device_tokens
)


actual_tool_expectations = {}


for tool_row in (
    spark.table(
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE
    )
    .select(
        "observed_token",
        "tool_name"
    )
    .distinct()
    .collect()
):

    actual_tool_expectations.setdefault(
        tool_row[
            "observed_token"
        ],
        set()
    ).add(
        tool_row[
            "tool_name"
        ]
    )


expected_tool_expectations = {
    "TFL": {
        "MDE"
    },

    "EMD": {
        "MDE"
    },

    "MID": {
        "MDE",
        "Intune",
        "Qualys"
    },

    "SAKIO": {
        "MDE",
        "Intune",
        "Qualys"
    },

    "SUKIO": {
        "MDE",
        "Intune",
        "Qualys"
    }
}


tool_expectation_errors = []


for (
    device_token,
    expected_tools
) in expected_tool_expectations.items():

    actual_tools = actual_tool_expectations.get(
        device_token,
        set()
    )


    if actual_tools != expected_tools:

        tool_expectation_errors.append(
            (
                device_token
                +
                " expected "
                +
                " | ".join(
                    sorted(
                        expected_tools
                    )
                )
                +
                " but received "
                +
                " | ".join(
                    sorted(
                        actual_tools
                    )
                )
            )
        )


# ---------------------------------------------------------------------------
# 18. Validate duplicate keys
# ---------------------------------------------------------------------------

duplicate_validation_contract = [
    (
        KB_ASSET_PATTERNS_V2_TABLE,
        [
            "pattern_id"
        ]
    ),

    (
        KB_OBSERVATION_RULES_V2_TABLE,
        [
            "rule_id"
        ]
    ),

    (
        KB_REQUIRED_TOOLS_V2_TABLE,
        [
            "asset_domain",
            "asset_family",
            "tool_name"
        ]
    ),

    (
        KB_DEVICE_PROFILES_TABLE,
        [
            "observed_token"
        ]
    ),

    (
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE,
        [
            "observed_token",
            "tool_name"
        ]
    ),

    (
        KB_NAMING_TOKEN_RULES_TABLE,
        [
            "observed_token"
        ]
    ),

    (
        KB_SOURCE_REGISTRY_TABLE,
        [
            "logical_source_name"
        ]
    ),

    (
        KB_TERMINOLOGY_RULES_TABLE,
        [
            "terminology_rule_id"
        ]
    )
]


duplicate_key_results = []


for (
    validation_table,
    key_columns
) in duplicate_validation_contract:

    duplicate_key_count = (
        spark.table(
            validation_table
        )

        .groupBy(
            *key_columns
        )

        .count()

        .filter(
            F.col(
                "count"
            ) > 1
        )

        .count()
    )


    duplicate_key_results.append(
        (
            validation_table,
            " | ".join(
                key_columns
            ),
            duplicate_key_count,
            duplicate_key_count == 0
        )
    )


duplicate_key_schema = T.StructType([
    T.StructField(
        "table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "key_columns",
        T.StringType(),
        False
    ),

    T.StructField(
        "duplicate_key_count",
        T.LongType(),
        False
    ),

    T.StructField(
        "check_passed",
        T.BooleanType(),
        False
    )
])


kb_duplicate_key_validation_df = spark.createDataFrame(
    duplicate_key_results,
    schema=duplicate_key_schema
)


# ---------------------------------------------------------------------------
# 19. Validate that the original V1 tables were not overwritten
# ---------------------------------------------------------------------------

original_table_post_write_counts = {
    KB_ASSET_PATTERNS_V1_TABLE:
        spark.table(
            KB_ASSET_PATTERNS_V1_TABLE
        ).count(),

    KB_OBSERVATION_RULES_V1_TABLE:
        spark.table(
            KB_OBSERVATION_RULES_V1_TABLE
        ).count(),

    KB_REQUIRED_TOOLS_V1_TABLE:
        spark.table(
            KB_REQUIRED_TOOLS_V1_TABLE
        ).count()
}


original_table_preservation_checks = [
    (
        KB_ASSET_PATTERNS_V1_TABLE,
        kb_asset_patterns_v1_count,
        original_table_post_write_counts[
            KB_ASSET_PATTERNS_V1_TABLE
        ],
        (
            kb_asset_patterns_v1_count
            ==
            original_table_post_write_counts[
                KB_ASSET_PATTERNS_V1_TABLE
            ]
        )
    ),

    (
        KB_OBSERVATION_RULES_V1_TABLE,
        kb_observation_rules_v1_count,
        original_table_post_write_counts[
            KB_OBSERVATION_RULES_V1_TABLE
        ],
        (
            kb_observation_rules_v1_count
            ==
            original_table_post_write_counts[
                KB_OBSERVATION_RULES_V1_TABLE
            ]
        )
    ),

    (
        KB_REQUIRED_TOOLS_V1_TABLE,
        kb_required_tools_v1_count,
        original_table_post_write_counts[
            KB_REQUIRED_TOOLS_V1_TABLE
        ],
        (
            kb_required_tools_v1_count
            ==
            original_table_post_write_counts[
                KB_REQUIRED_TOOLS_V1_TABLE
            ]
        )
    )
]


original_table_preservation_schema = T.StructType([
    T.StructField(
        "original_table",
        T.StringType(),
        False
    ),

    T.StructField(
        "count_before_phase_2",
        T.LongType(),
        False
    ),

    T.StructField(
        "count_after_phase_2",
        T.LongType(),
        False
    ),

    T.StructField(
        "table_preserved",
        T.BooleanType(),
        False
    )
])


kb_original_table_preservation_df = spark.createDataFrame(
    original_table_preservation_checks,
    schema=original_table_preservation_schema
)


# ---------------------------------------------------------------------------
# 20. Create the phase summary
# ---------------------------------------------------------------------------

phase_summary_rows = [
    (
        KB_ASSET_PATTERNS_V1_TABLE,
        kb_asset_patterns_v1_count,
        "Original preserved"
    ),

    (
        KB_ASSET_PATTERNS_V2_TABLE,
        written_table_counts[
            KB_ASSET_PATTERNS_V2_TABLE
        ],
        "V2 current"
    ),

    (
        KB_OBSERVATION_RULES_V1_TABLE,
        kb_observation_rules_v1_count,
        "Original preserved"
    ),

    (
        KB_OBSERVATION_RULES_V2_TABLE,
        written_table_counts[
            KB_OBSERVATION_RULES_V2_TABLE
        ],
        "V2 current"
    ),

    (
        KB_REQUIRED_TOOLS_V1_TABLE,
        kb_required_tools_v1_count,
        "Original preserved"
    ),

    (
        KB_REQUIRED_TOOLS_V2_TABLE,
        written_table_counts[
            KB_REQUIRED_TOOLS_V2_TABLE
        ],
        "V2 current"
    ),

    (
        KB_DEVICE_PROFILES_TABLE,
        written_table_counts[
            KB_DEVICE_PROFILES_TABLE
        ],
        "New current"
    ),

    (
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE,
        written_table_counts[
            KB_DEVICE_TOOL_EXPECTATIONS_TABLE
        ],
        "New current"
    ),

    (
        KB_NAMING_TOKEN_RULES_TABLE,
        written_table_counts[
            KB_NAMING_TOKEN_RULES_TABLE
        ],
        "New current"
    ),

    (
        KB_SOURCE_REGISTRY_TABLE,
        written_table_counts[
            KB_SOURCE_REGISTRY_TABLE
        ],
        "New current"
    ),

    (
        KB_TERMINOLOGY_RULES_TABLE,
        written_table_counts[
            KB_TERMINOLOGY_RULES_TABLE
        ],
        "New current"
    ),

    (
        KB_RELEASE_MANIFEST_TABLE,
        written_table_counts[
            KB_RELEASE_MANIFEST_TABLE
        ],
        "New current"
    )
]


phase_summary_schema = T.StructType([
    T.StructField(
        "table_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "row_count",
        T.LongType(),
        False
    ),

    T.StructField(
        "table_role",
        T.StringType(),
        False
    )
])


kb_phase_2_summary_df = spark.createDataFrame(
    phase_summary_rows,
    schema=phase_summary_schema
)


# ---------------------------------------------------------------------------
# 21. Write a human-readable run manifest into the Post Workspace folder
#
# This begins the permanent RCSO run-record practice. Registered Delta tables
# stay in the catalog. The Workspace folder receives a manifest describing
# what was created and validated.
# ---------------------------------------------------------------------------

phase_2_manifest_path = os.path.join(
    RCAIP_WORKSPACE_MANIFEST_ROOT,
    "RCAIP_KB_Phase_2_Manifest.json"
)


phase_2_manifest = {
    "status":
        "PASS_PENDING_FINAL_VALIDATION",

    "run_id":
        RCAIP_RUN_ID,

    "post_name":
        RCAIP_POST_NAME,

    "post_slug":
        RCAIP_POST_SLUG,

    "release_version":
        RCAIP_RELEASE_VERSION,

    "original_tables_preserved": [
        KB_ASSET_PATTERNS_V1_TABLE,
        KB_OBSERVATION_RULES_V1_TABLE,
        KB_REQUIRED_TOOLS_V1_TABLE
    ],

    "new_or_versioned_tables": [
        table_name

        for (
            table_name,
            destination_df
        ) in knowledge_base_write_contract
    ],

    "recognized_device_tokens":
        sorted(
            actual_device_tokens
        ),

    "tool_expectations": {
        device_token:
            sorted(
                tool_names
            )

        for (
            device_token,
            tool_names
        ) in actual_tool_expectations.items()
    },

    "guarded_naming_tokens": [
        "HU",
        "MED",
        "HR"
    ],

    "gomobile_status":
        "DISCOVERY_REQUIRED",

    "aad_audit_table":
        resolved_aad_audit_table,

    "created_at_utc":
        datetime.utcnow().isoformat()
}


with open(
    phase_2_manifest_path,
    "w",
    encoding="utf-8"
) as manifest_file:

    json.dump(
        phase_2_manifest,
        manifest_file,
        indent=2,
        sort_keys=True,
        default=str
    )


# ---------------------------------------------------------------------------
# 22. Display validation products
# ---------------------------------------------------------------------------

print(
    "RCAIP Knowledge Base Phase 2 table summary"
)


display(
    kb_phase_2_summary_df
    .orderBy(
        "table_role",
        "table_name"
    )
)


print(
    "Original Knowledge Base table preservation"
)


display(
    kb_original_table_preservation_df
)


print(
    "Duplicate-key validation"
)


display(
    kb_duplicate_key_validation_df
)


print(
    "Mobile-computer device profiles"
)


display(
    spark.table(
        KB_DEVICE_PROFILES_TABLE
    )
    .orderBy(
        "observed_token"
    )
)


print(
    "Mobile-computer tooling expectations"
)


display(
    spark.table(
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE
    )
    .orderBy(
        "observed_token",
        "tool_name"
    )
)


print(
    "Guarded naming-token rules"
)


display(
    spark.table(
        KB_NAMING_TOKEN_RULES_TABLE
    )
    .orderBy(
        "observed_token"
    )
)


print(
    "Source registry"
)


display(
    spark.table(
        KB_SOURCE_REGISTRY_TABLE
    )
    .orderBy(
        "logical_source_name"
    )
)


# ---------------------------------------------------------------------------
# 23. Final validation
# ---------------------------------------------------------------------------

failed_preservation_checks = [
    table_name

    for (
        table_name,
        count_before,
        count_after,
        table_preserved
    ) in original_table_preservation_checks

    if not table_preserved
]


failed_duplicate_checks = [
    table_name

    for (
        table_name,
        key_columns,
        duplicate_key_count,
        check_passed
    ) in duplicate_key_results

    if not check_passed
]


final_validation_errors = []


if missing_device_tokens:

    final_validation_errors.append(
        "Missing device tokens: "
        +
        " | ".join(
            missing_device_tokens
        )
    )


if tool_expectation_errors:

    final_validation_errors.append(
        "Tool expectation errors: "
        +
        " || ".join(
            tool_expectation_errors
        )
    )


if failed_preservation_checks:

    final_validation_errors.append(
        "Original tables changed unexpectedly: "
        +
        " | ".join(
            failed_preservation_checks
        )
    )


if failed_duplicate_checks:

    final_validation_errors.append(
        "Duplicate keys detected in: "
        +
        " | ".join(
            failed_duplicate_checks
        )
    )


if not os.path.exists(
    phase_2_manifest_path
):

    final_validation_errors.append(
        "Workspace Phase 2 manifest was not created."
    )


if final_validation_errors:

    raise ValueError(
        "RCAIP Knowledge Base Phase 2 validation failed:\n- "
        +
        "\n- ".join(
            final_validation_errors
        )
    )


# Update the manifest from pending to final PASS.
phase_2_manifest[
    "status"
] = "PASS"


with open(
    phase_2_manifest_path,
    "w",
    encoding="utf-8"
) as manifest_file:

    json.dump(
        phase_2_manifest,
        manifest_file,
        indent=2,
        sort_keys=True,
        default=str
    )


print("=" * 100)

print(
    "Status:",
    "PASS"
)

print(
    "Run ID:",
    RCAIP_RUN_ID
)

print(
    "Original asset patterns:",
    kb_asset_patterns_v1_count
)

print(
    "V2 asset patterns:",
    written_table_counts[
        KB_ASSET_PATTERNS_V2_TABLE
    ]
)

print(
    "Original observation rules:",
    kb_observation_rules_v1_count
)

print(
    "V2 observation rules:",
    written_table_counts[
        KB_OBSERVATION_RULES_V2_TABLE
    ]
)

print(
    "Original required-tool rules:",
    kb_required_tools_v1_count
)

print(
    "V2 required-tool rules:",
    written_table_counts[
        KB_REQUIRED_TOOLS_V2_TABLE
    ]
)

print(
    "Device profile rows:",
    written_table_counts[
        KB_DEVICE_PROFILES_TABLE
    ]
)

print(
    "Device tooling expectation rows:",
    written_table_counts[
        KB_DEVICE_TOOL_EXPECTATIONS_TABLE
    ]
)

print(
    "Guarded naming-token rows:",
    written_table_counts[
        KB_NAMING_TOKEN_RULES_TABLE
    ]
)

print(
    "Source registry rows:",
    written_table_counts[
        KB_SOURCE_REGISTRY_TABLE
    ]
)

print(
    "Terminology-rule rows:",
    written_table_counts[
        KB_TERMINOLOGY_RULES_TABLE
    ]
)

print(
    "Workspace manifest:",
    phase_2_manifest_path
)

print(
    "GoMobile status:",
    "DISCOVERY_REQUIRED"
)

print("=" * 100)
