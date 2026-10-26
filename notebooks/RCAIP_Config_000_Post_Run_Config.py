# Databricks notebook source
# ============================================================================
# RCAIP CONFIGURATION 000
# Single-Cell Certified-Intake Runtime Resolver
#
# ENTERPRISE OPERATING MODEL
#   1. RCAIP_RUN_000_Post_Intake_and_Readiness collects, validates, and
#      publishes the Post profile and CSA run.
#   2. RCAIP_Config_001_Table_Registry executes this notebook automatically.
#   3. This notebook reads the certified intake registries and publishes one
#      complete runtime contract.
#   4. Field RCSOs do not edit this code and do not enter Config 000 widgets.
#
# NOTEBOOK STRUCTURE
#   - This is the only code cell permitted in Config 000.
#   - Delete or archive every older Config 000 cell.
#   - Do not add Post-specific dictionaries, dates, CIDRs, or filenames here.
#
# WRITES
#   - No Delta table writes.
#   - Creates only the user's Workspace run directories.
# ============================================================================

import copy
import ipaddress
import json
import os
import re

from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# 1. Generic helpers
# ---------------------------------------------------------------------------

def normalize_profile_key(raw_value):

    normalized = re.sub(
        r"[^a-z0-9_]+",
        "_",
        str(
            raw_value or ""
        ).strip().lower()
    )

    return re.sub(
        r"_+",
        "_",
        normalized
    ).strip("_")


def normalize_identifier_token(raw_value):

    normalized = re.sub(
        r"[^a-z0-9_]+",
        "_",
        str(
            raw_value or ""
        ).strip().lower()
    )

    return re.sub(
        r"_+",
        "_",
        normalized
    ).strip("_")


def ordered_unique_strings(
    values,
    lowercase=False,
    uppercase=False
):

    output = []

    for value in values:

        text = str(
            value or ""
        ).strip()

        if lowercase:
            text = text.lower()

        if uppercase:
            text = text.upper()

        if text and text not in output:
            output.append(
                text
            )

    return output


def parse_json_list(
    raw_value,
    field_name
):

    if raw_value is None or not str(
        raw_value
    ).strip():
        return []

    try:
        parsed_value = json.loads(
            str(
                raw_value
            )
        )

    except Exception as parse_error:
        raise ValueError(
            f"Registry field {field_name} is not valid JSON: "
            f"{parse_error}"
        )

    if not isinstance(
        parsed_value,
        list
    ):
        raise ValueError(
            f"Registry field {field_name} must contain a JSON list."
        )

    return parsed_value


def table_exists_safe(
    table_name
):

    try:
        return spark.catalog.tableExists(
            table_name
        )

    except Exception:
        return False


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


def dbfs_path_exists(
    path_value
):

    try:
        dbutils.fs.ls(
            path_value
        )

        return True

    except Exception:
        return False


def coerce_registry_date(
    raw_value,
    field_name
):

    if raw_value is None:
        raise ValueError(
            f"Registry field {field_name} is blank."
        )

    if isinstance(
        raw_value,
        datetime
    ):
        return raw_value.date()

    if isinstance(
        raw_value,
        date
    ):
        return raw_value

    try:
        return datetime.strptime(
            str(
                raw_value
            ).strip(),
            "%Y-%m-%d"
        ).date()

    except ValueError:
        raise ValueError(
            f"Registry field {field_name} must use YYYY-MM-DD."
        )


def resolve_first_available_table(
    candidate_tables,
    required=False
):

    for candidate_table in candidate_tables:

        if table_exists_safe(
            candidate_table
        ):
            return candidate_table

    if required:
        raise RuntimeError(
            "No available table was found from: "
            +
            " | ".join(
                candidate_tables
            )
        )

    return None


# ---------------------------------------------------------------------------
# 2. Locate the certified intake registries
# ---------------------------------------------------------------------------

RCAIP_CONTROL_NAMESPACE = "devresults"

RCAIP_PROFILE_REGISTRY_TABLE = (
    f"{RCAIP_CONTROL_NAMESPACE}."
    "rcaip_post_profile_registry"
)

RCAIP_RUN_REGISTRY_TABLE = (
    f"{RCAIP_CONTROL_NAMESPACE}."
    "rcaip_assessment_run_registry"
)

missing_control_tables = [
    table_name

    for table_name
    in [
        RCAIP_PROFILE_REGISTRY_TABLE,
        RCAIP_RUN_REGISTRY_TABLE
    ]

    if not table_exists_safe(
        table_name
    )
]

if missing_control_tables:

    raise RuntimeError(
        "Certified intake registries are unavailable. Run "
        "RCAIP_RUN_000_Post_Intake_and_Readiness through its final "
        "registry-publication cell. Missing: "
        +
        " | ".join(
            missing_control_tables
        )
    )


# ---------------------------------------------------------------------------
# 3. Select the active certified CSA run
#
# Selection precedence:
#   1. Most recent active certified run created by the current user.
#   2. If the current user has no active run, use the only active enterprise
#      run when exactly one exists.
#   3. Never guess when several unrelated enterprise runs are active.
#
# Config 000 deliberately has no Post, date, country, CIDR, filename, release,
# namespace, run, or run-mode widgets.
# ---------------------------------------------------------------------------

RCAIP_DATABRICKS_USER = str(
    spark.sql(
        "SELECT current_user() AS current_user"
    ).first()[
        "current_user"
    ]
).strip()

active_profiles_df = (
    spark.table(
        RCAIP_PROFILE_REGISTRY_TABLE
    )
    .filter(
        F.col(
            "active"
        )
        &
        (
            F.upper(
                F.col(
                    "profile_status"
                )
            )
            ==
            F.lit(
                "CERTIFIED"
            )
        )
    )
)

active_runs_df = (
    spark.table(
        RCAIP_RUN_REGISTRY_TABLE
    )
    .filter(
        F.col(
            "active"
        )
        &
        (
            F.upper(
                F.col(
                    "run_status"
                )
            )
            ==
            F.lit(
                "CERTIFIED_INTAKE"
            )
        )
    )
)

runnable_runs_df = (
    active_runs_df.alias(
        "run"
    )
    .join(
        active_profiles_df
        .select(
            "post_profile_key",
            F.col(
                "profile_hash"
            ).alias(
                "profile_registry_hash"
            )
        )
        .alias(
            "profile"
        ),
        on="post_profile_key",
        how="inner"
    )
    .filter(
        F.col(
            "run.profile_hash"
        )
        ==
        F.col(
            "profile.profile_registry_hash"
        )
    )
    .select(
        "run.*"
    )
)

current_user_run_rows = (
    runnable_runs_df
    .filter(
        F.col(
            "certified_by"
        )
        ==
        F.lit(
            RCAIP_DATABRICKS_USER
        )
    )
    .orderBy(
        F.col(
            "certified_at"
        ).desc(),
        F.col(
            "run_id"
        ).desc()
    )
    .limit(
        2
    )
    .collect()
)

if current_user_run_rows:

    selected_run_row = current_user_run_rows[
        0
    ]

    RCAIP_RUNTIME_SELECTION_SOURCE = (
        "LATEST_CURRENT_USER_CERTIFIED_RUN"
    )

else:

    enterprise_run_rows = (
        runnable_runs_df
        .orderBy(
            F.col(
                "certified_at"
            ).desc(),
            F.col(
                "run_id"
            ).desc()
        )
        .limit(
            2
        )
        .collect()
    )

    if not enterprise_run_rows:

        raise RuntimeError(
            "No runnable certified CSA run exists. Run "
            "RCAIP_RUN_000_Post_Intake_and_Readiness and publish the intake."
        )

    if len(
        enterprise_run_rows
    ) > 1:

        raise RuntimeError(
            "No active certified run belongs to the current Databricks user, "
            "and multiple enterprise runs are active. Config 000 will not "
            "guess. Run the intake under the current user account."
        )

    selected_run_row = enterprise_run_rows[
        0
    ]

    RCAIP_RUNTIME_SELECTION_SOURCE = (
        "ONLY_ACTIVE_ENTERPRISE_CERTIFIED_RUN"
    )

RCAIP_SELECTED_RUN_ID = str(
    selected_run_row[
        "run_id"
    ]
).strip()

RCAIP_SELECTED_POST_PROFILE = normalize_profile_key(
    selected_run_row[
        "post_profile_key"
    ]
)

profile_matches = (
    active_profiles_df
    .filter(
        F.col(
            "post_profile_key"
        )
        ==
        F.lit(
            RCAIP_SELECTED_POST_PROFILE
        )
    )
    .collect()
)

if len(
    profile_matches
) != 1:

    raise RuntimeError(
        "Config 000 requires exactly one active certified Post profile for "
        f"{RCAIP_SELECTED_POST_PROFILE}; found {len(profile_matches)}."
    )

run_matches = (
    runnable_runs_df
    .filter(
        (
            F.col(
                "post_profile_key"
            )
            ==
            F.lit(
                RCAIP_SELECTED_POST_PROFILE
            )
        )
        &
        (
            F.col(
                "run_id"
            )
            ==
            F.lit(
                RCAIP_SELECTED_RUN_ID
            )
        )
    )
    .collect()
)

if len(
    run_matches
) != 1:

    raise RuntimeError(
        "Config 000 requires exactly one active certified run for "
        f"{RCAIP_SELECTED_RUN_ID}; found {len(run_matches)}."
    )

profile_row = profile_matches[
    0
]

run_row = run_matches[
    0
]


# ---------------------------------------------------------------------------
# 4. Publish the authoritative registry-derived runtime
# ---------------------------------------------------------------------------

RCAIP_POST_NAME = str(
    profile_row[
        "post_name"
    ]
).strip()

RCAIP_POST_SLUG = normalize_profile_key(
    profile_row[
        "post_slug"
    ]
)

RCAIP_COUNTRY_NAME = str(
    profile_row[
        "country_name"
    ]
).strip()

RCAIP_POST_DEVICE_PREFIX = str(
    profile_row[
        "post_device_prefix"
    ]
).strip().upper()

RCAIP_POST_TOKENS = ordered_unique_strings(
    [
        RCAIP_POST_SLUG,
        RCAIP_POST_NAME,
        RCAIP_POST_DEVICE_PREFIX,
        *parse_json_list(
            profile_row[
                "post_tokens_json"
            ],
            "post_tokens_json"
        )
    ],
    lowercase=True
)

RCAIP_DNS_TOKENS = ordered_unique_strings(
    [
        *parse_json_list(
            profile_row[
                "dns_tokens_json"
            ],
            "dns_tokens_json"
        ),
        *RCAIP_POST_TOKENS
    ],
    lowercase=True
)

RCAIP_USER_LOCATION_TOKENS = ordered_unique_strings(
    [
        *parse_json_list(
            profile_row[
                "user_location_tokens_json"
            ],
            "user_location_tokens_json"
        ),
        *RCAIP_POST_TOKENS
    ],
    lowercase=True
)

RCAIP_MDE_TOKENS = ordered_unique_strings(
    [
        *parse_json_list(
            profile_row[
                "mde_tokens_json"
            ],
            "mde_tokens_json"
        ),
        *RCAIP_POST_TOKENS
    ],
    lowercase=True
)

RCAIP_ENTERPRISE_CIDRS = ordered_unique_strings(
    parse_json_list(
        profile_row[
            "enterprise_cidrs_json"
        ],
        "enterprise_cidrs_json"
    )
)

RCAIP_NEN_PUBLIC_TARGETS = ordered_unique_strings(
    parse_json_list(
        profile_row[
            "nen_public_targets_json"
        ],
        "nen_public_targets_json"
    )
)

RCAIP_ENTWIFI_PUBLIC_TARGETS = ordered_unique_strings(
    parse_json_list(
        profile_row[
            "entwifi_public_targets_json"
        ],
        "entwifi_public_targets_json"
    )
)

RCAIP_INPUT_BASE_DIR = str(
    profile_row[
        "input_base_dir"
    ]
).strip()

RCAIP_CMDB_FILE = str(
    profile_row[
        "cmdb_file"
    ]
).strip()

# ---------------------------------------------------------------------------
# Run-scoped optional Qualys evidence authority
#
# D0030:
#   Supplemental evidence belongs to the assessment run and must not require
#   mutation of the stable Post profile. Preserve the mature profile field as
#   a compatibility fallback, but prefer exactly one active certified QUALYS
#   artifact attached to the current certified run when such evidence exists.
# ---------------------------------------------------------------------------

raw_qualys_file = profile_row[
    "qualys_easm_file"
]

_base_profile_qualys_file = (
    str(
        raw_qualys_file
    ).strip()

    if raw_qualys_file

    else
    None
)

RCAIP_QUALYS_EASM_FILE = (
    _base_profile_qualys_file
)

RCAIP_QUALYS_EASM_SOURCE_FILE_PATH = None

RCAIP_QUALYS_EASM_RUNTIME_SOURCE = (
    "BASE_PROFILE"
    if _base_profile_qualys_file
    else
    "NOT_PROVIDED"
)

_run_results_namespace = str(
    run_row[
        "results_namespace"
    ]
).strip()

_run_scoped_artifact_table = (
    f"{_run_results_namespace}."
    "rcaip_assessment_run_v3_artifact_evidence"
)

if spark.catalog.tableExists(
    _run_scoped_artifact_table
):

    _artifact_columns = set(
        spark.table(
            _run_scoped_artifact_table
        ).columns
    )

    _required_artifact_columns = {
        "run_id",
        "post_profile_key",
        "base_profile_hash",
        "file_name",
        "source_file_path",
        "evidence_family",
        "evidence_status",
        "active",
    }

    _missing_artifact_columns = sorted(
        _required_artifact_columns
        -
        _artifact_columns
    )

    if _missing_artifact_columns:

        raise RuntimeError(
            "Run-scoped artifact-evidence table is missing required "
            "Qualys runtime columns: "
            +
            " | ".join(
                _missing_artifact_columns
            )
        )


    _current_run_qualys_rows = (
        spark.table(
            _run_scoped_artifact_table
        )

        .filter(
            (
                F.col(
                    "run_id"
                ).cast(
                    "string"
                )
                ==
                F.lit(
                    str(
                        run_row[
                            "run_id"
                        ]
                    ).strip()
                )
            )
            &
            (
                F.col(
                    "post_profile_key"
                ).cast(
                    "string"
                )
                ==
                F.lit(
                    str(
                        run_row[
                            "post_profile_key"
                        ]
                    ).strip()
                )
            )
            &
            (
                F.col(
                    "base_profile_hash"
                ).cast(
                    "string"
                )
                ==
                F.lit(
                    str(
                        run_row[
                            "profile_hash"
                        ]
                    ).strip()
                )
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col(
                            "evidence_family"
                        ).cast(
                            "string"
                        )
                    )
                )
                ==
                F.lit(
                    "QUALYS"
                )
            )
            &
            (
                F.upper(
                    F.trim(
                        F.col(
                            "evidence_status"
                        ).cast(
                            "string"
                        )
                    )
                )
                ==
                F.lit(
                    "CERTIFIED"
                )
            )
            &
            (
                F.col(
                    "active"
                )
                ==
                F.lit(
                    True
                )
            )
        )

        .select(
            "file_name",
            "source_file_path",
        )

        .dropDuplicates()
        .collect()
    )


    if len(
        _current_run_qualys_rows
    ) > 1:

        raise RuntimeError(
            "Current certified run contains multiple active QUALYS "
            "artifacts, but the mature Asset Engine compatibility contract "
            "publishes only one Qualys file. Refusing ambiguous selection."
        )


    if len(
        _current_run_qualys_rows
    ) == 1:

        _run_qualys_file = str(
            _current_run_qualys_rows[
                0
            ][
                "file_name"
            ]
            or
            ""
        ).strip()

        _run_qualys_path = str(
            _current_run_qualys_rows[
                0
            ][
                "source_file_path"
            ]
            or
            ""
        ).strip()

        if not _run_qualys_file:

            raise RuntimeError(
                "Current certified QUALYS artifact has a blank file_name."
            )

        if not _run_qualys_path:

            raise RuntimeError(
                "Current certified QUALYS artifact has a blank "
                "source_file_path."
            )

        RCAIP_QUALYS_EASM_FILE = (
            _run_qualys_file
        )

        RCAIP_QUALYS_EASM_SOURCE_FILE_PATH = (
            _run_qualys_path
        )

        RCAIP_QUALYS_EASM_RUNTIME_SOURCE = (
            "RUN_SCOPED_CERTIFIED_ARTIFACT"
        )

RCAIP_NMAP_SCANS = parse_json_list(
    profile_row[
        "nmap_scans_json"
    ],
    "nmap_scans_json"
)

RCAIP_ASSESSMENT_START_DATE = coerce_registry_date(
    run_row[
        "assessment_start_date"
    ],
    "assessment_start_date"
)

RCAIP_ASSESSMENT_END_DATE = coerce_registry_date(
    run_row[
        "assessment_end_date"
    ],
    "assessment_end_date"
)

RCAIP_ASSESSMENT_START_DATE_TEXT = (
    RCAIP_ASSESSMENT_START_DATE.strftime(
        "%Y-%m-%d"
    )
)

RCAIP_ASSESSMENT_END_DATE_TEXT = (
    RCAIP_ASSESSMENT_END_DATE.strftime(
        "%Y-%m-%d"
    )
)

RCAIP_RESULTS_NAMESPACE = str(
    run_row[
        "results_namespace"
    ]
).strip()

RCAIP_RELEASE_VERSION = str(
    run_row[
        "release_version"
    ]
).strip()

RCAIP_RUN_MODE = str(
    run_row[
        "run_mode"
    ]
).strip().upper()

RCAIP_RUN_ID = RCAIP_SELECTED_RUN_ID

RCAIP_PROFILE_HASH = str(
    profile_row[
        "profile_hash"
    ]
).strip()

RCAIP_POST_PROFILE_SOURCE = (
    "CERTIFIED_INTAKE_REGISTRY"
)

RCAIP_CONFIG_000_REGISTRY_MODE = (
    "READ_ONLY_CERTIFIED_INTAKE_CONSUMER"
)

RCAIP_POST_LOOKUP_TABLE = (
    str(
        profile_row[
            "lookup_table"
        ]
    ).strip()

    if profile_row[
        "lookup_table"
    ]

    else
    None
)

RCAIP_POST_LOOKUP_STATUS = str(
    profile_row[
        "lookup_status"
    ]
).strip()

RCAIP_POST_LOOKUP_MATCH_COUNT = (
    1

    if RCAIP_POST_LOOKUP_STATUS
    ==
    "MATCHED"

    else
    0
)

RCAIP_POST_LOOKUP_COLUMNS = {}

RCAIP_POST_LOOKUP_TOKENS = list(
    RCAIP_POST_TOKENS
)

RCAIP_POST_LOOKUP_CIDRS = list(
    RCAIP_ENTERPRISE_CIDRS
)

RCAIP_REPORT_POST_NAME = RCAIP_POST_NAME
RCAIP_REPORT_POST_SLUG = RCAIP_POST_SLUG

RCAIP_REPORT_POST_CODE = (
    RCAIP_POST_DEVICE_PREFIX
    or
    RCAIP_POST_SLUG.upper()
)


# ---------------------------------------------------------------------------
# 5. Validate the certified input contract
# ---------------------------------------------------------------------------

configuration_errors = []

allowed_run_modes = {
    "DISCOVERY",
    "DEVELOPMENT",
    "VALIDATION",
    "ASSESSMENT",
    "PRODUCTION"
}

if RCAIP_RUN_MODE not in allowed_run_modes:

    configuration_errors.append(
        "Run Mode must be one of: "
        +
        " | ".join(
            sorted(
                allowed_run_modes
            )
        )
    )

if not RCAIP_POST_NAME:

    configuration_errors.append(
        "Post Name cannot be blank."
    )

if not re.fullmatch(
    r"[a-z0-9_]+",
    RCAIP_POST_SLUG
):

    configuration_errors.append(
        "Post Slug contains unsupported characters."
    )

if (
    not RCAIP_COUNTRY_NAME
    or
    RCAIP_COUNTRY_NAME.upper()
    ==
    "UNKNOWN"
):

    configuration_errors.append(
        "Country must be populated by the certified intake."
    )

if (
    RCAIP_ASSESSMENT_END_DATE
    <
    RCAIP_ASSESSMENT_START_DATE
):

    configuration_errors.append(
        "Assessment End Date precedes Assessment Start Date."
    )

if not re.fullmatch(
    r"[A-Za-z0-9_.]+",
    RCAIP_RESULTS_NAMESPACE
):

    configuration_errors.append(
        "Results Namespace contains unsupported characters."
    )

invalid_enterprise_cidrs = []

for enterprise_cidr in RCAIP_ENTERPRISE_CIDRS:

    try:
        ipaddress.ip_network(
            enterprise_cidr,
            strict=False
        )

    except ValueError:
        invalid_enterprise_cidrs.append(
            enterprise_cidr
        )

if invalid_enterprise_cidrs:

    configuration_errors.append(
        "Invalid OpenNet CIDRs: "
        +
        " | ".join(
            invalid_enterprise_cidrs
        )
    )

required_nmap_scan_fields = {
    "file",
    "asset_domain",
    "scan_scope",
    "scan_location",
    "target_cidr",
    "notes"
}

invalid_nmap_scan_indexes = []

for (
    scan_index,
    scan_record
) in enumerate(
    RCAIP_NMAP_SCANS
):

    if not isinstance(
        scan_record,
        dict
    ):

        invalid_nmap_scan_indexes.append(
            scan_index
        )

        continue

    missing_scan_fields = (
        required_nmap_scan_fields
        -
        set(
            scan_record.keys()
        )
    )

    if missing_scan_fields:

        invalid_nmap_scan_indexes.append(
            scan_index
        )

if invalid_nmap_scan_indexes:

    configuration_errors.append(
        "Invalid Nmap manifest records: "
        +
        " | ".join(
            str(
                scan_index
            )

            for scan_index
            in invalid_nmap_scan_indexes
        )
    )

RCAIP_CMDB_PATH = dbfs_path_join(
    RCAIP_INPUT_BASE_DIR,
    RCAIP_CMDB_FILE
)

RCAIP_QUALYS_EASM_PATH = (
    RCAIP_QUALYS_EASM_SOURCE_FILE_PATH

    if RCAIP_QUALYS_EASM_SOURCE_FILE_PATH

    else
    (
        dbfs_path_join(
            RCAIP_INPUT_BASE_DIR,
            RCAIP_QUALYS_EASM_FILE
        )

        if RCAIP_QUALYS_EASM_FILE

        else
        None
    )
)

RCAIP_NMAP_PATHS = [
    dbfs_path_join(
        RCAIP_INPUT_BASE_DIR,
        scan_record[
            "file"
        ]
    )

    for scan_record
    in RCAIP_NMAP_SCANS
]

RCAIP_CMDB_FILE_AVAILABLE = dbfs_path_exists(
    RCAIP_CMDB_PATH
)

RCAIP_QUALYS_FILE_AVAILABLE = (
    dbfs_path_exists(
        RCAIP_QUALYS_EASM_PATH
    )

    if RCAIP_QUALYS_EASM_PATH

    else
    True
)

RCAIP_NMAP_FILE_AVAILABILITY = {
    scan_path:
        dbfs_path_exists(
            scan_path
        )

    for scan_path
    in RCAIP_NMAP_PATHS
}

missing_nmap_paths = [
    scan_path

    for (
        scan_path,
        file_available
    )
    in RCAIP_NMAP_FILE_AVAILABILITY.items()

    if not file_available
]

if not RCAIP_CMDB_FILE_AVAILABLE:

    configuration_errors.append(
        "CMDB file was not found: "
        +
        RCAIP_CMDB_PATH
    )

if not RCAIP_QUALYS_FILE_AVAILABLE:

    configuration_errors.append(
        "Configured Qualys file was not found: "
        +
        str(
            RCAIP_QUALYS_EASM_PATH
        )
    )

if missing_nmap_paths:

    configuration_errors.append(
        "Configured Nmap files were not found: "
        +
        " | ".join(
            missing_nmap_paths
        )
    )

RCAIP_POST_INPUTS_READY = bool(
    RCAIP_INPUT_BASE_DIR
    and
    RCAIP_CMDB_FILE
    and
    RCAIP_NMAP_SCANS
    and
    RCAIP_CMDB_FILE_AVAILABLE
    and
    not missing_nmap_paths
)

release_token = normalize_identifier_token(
    RCAIP_RELEASE_VERSION
)

# D0025 / D0030:
# The active certified assessment-run registry row is the authoritative
# expected run identity. Do not reconstruct a child run ID from naming rules.
from pyspark.sql import functions as _rcaip_config000_F

_rcaip_active_run_id_rows = (
    spark.table(
        f"{RCAIP_RESULTS_NAMESPACE}.rcaip_assessment_run_registry"
    )
    .filter(
        (
            _rcaip_config000_F.col("post_profile_key")
            ==
            _rcaip_config000_F.lit(RCAIP_POST_SLUG)
        )
        &
        (
            _rcaip_config000_F.col("run_status")
            ==
            _rcaip_config000_F.lit("CERTIFIED_INTAKE")
        )
        &
        (
            _rcaip_config000_F.col("active")
            ==
            _rcaip_config000_F.lit(True)
        )
    )
    .select("run_id")
    .collect()
)

if len(_rcaip_active_run_id_rows) != 1:
    raise RuntimeError(
        "Config 000 integrity check requires exactly one active "
        "CERTIFIED_INTAKE run for Post "
        f"{RCAIP_POST_SLUG}; "
        f"found {len(_rcaip_active_run_id_rows)}."
    )

expected_run_id = str(
    _rcaip_active_run_id_rows[0]["run_id"]
).strip()

if RCAIP_RUN_ID != expected_run_id:

    configuration_errors.append(
        "Registry Run ID does not match the standard RCAIP run identity. "
        f"Expected {expected_run_id}; observed {RCAIP_RUN_ID}."
    )

RCAIP_RUN_DATE_TOKEN = (
    RCAIP_ASSESSMENT_START_DATE.strftime(
        "%Y%m%d"
    )
)

RCAIP_RUN_YEAR = (
    RCAIP_ASSESSMENT_START_DATE.year
)

RCAIP_WORKSPACE_USER_ROOT = (
    f"/Workspace/Users/"
    f"{RCAIP_DATABRICKS_USER}"
)

RCAIP_WORKSPACE_RUN_ROOT = (
    f"{RCAIP_WORKSPACE_USER_ROOT}/"
    f"RCAIP_RUNS/"
    f"{RCAIP_POST_SLUG.upper()}_"
    f"{RCAIP_RUN_YEAR}"
)

RCAIP_WORKSPACE_EXPORT_ROOT = (
    f"{RCAIP_WORKSPACE_RUN_ROOT}/"
    "Exports"
)

RCAIP_WORKSPACE_MANIFEST_ROOT = (
    f"{RCAIP_WORKSPACE_RUN_ROOT}/"
    "Manifest"
)

RCAIP_WORKSPACE_LOG_ROOT = (
    f"{RCAIP_WORKSPACE_RUN_ROOT}/"
    "Run_Log"
)

for workspace_directory in [
    RCAIP_WORKSPACE_RUN_ROOT,
    RCAIP_WORKSPACE_EXPORT_ROOT,
    RCAIP_WORKSPACE_MANIFEST_ROOT,
    RCAIP_WORKSPACE_LOG_ROOT
]:

    os.makedirs(
        workspace_directory,
        exist_ok=True
    )


def rcaip_table(
    table_stem,
    post_slug=None,
    results_namespace=None
):

    effective_post_slug = (
        post_slug
        or
        RCAIP_POST_SLUG
    )

    effective_namespace = (
        results_namespace
        or
        RCAIP_RESULTS_NAMESPACE
    )

    normalized_table_stem = normalize_identifier_token(
        table_stem
    )

    if not normalized_table_stem:

        raise ValueError(
            "RCAIP table stem cannot be blank."
        )

    return (
        f"{effective_namespace}."
        f"{effective_post_slug}_"
        f"{normalized_table_stem}"
    )


# ---------------------------------------------------------------------------
# 6. Resolve enterprise source aliases
# ---------------------------------------------------------------------------

RCAIP_POST_LOOKUP_CANDIDATES = [
    (
        f"{RCAIP_CONTROL_NAMESPACE}."
        "rcaip_kb_isso_subnet_lookup"
    ),
    "example_sources.binding_a97541d9"
]

RCAIP_SOURCE_ALIASES = {
    "o365_aad_audit": [
        "example_sources.o365_aad_audit_1",
        "example_sources.o365_aad_audit_2"
    ],

    "tenable_assets": [
        "example_sources.tenable_assets_1"
    ],

    "intune_devices": [
        "example_sources.intune_devices_1"
    ],

    "mde_device_info": [
        "example_sources.mde_device_info_1"
    ],

    "mde_device_network_info": [
        "example_sources.mde_device_network_info_1",
        "example_sources.mde_device_network_info_2"
    ],

    "ad_users": [
        "example_sources.ad_users_1",
        "example_sources.ad_users_2"
    ],

    "ad_computers": [
        "example_sources.ad_computers_1"
    ],

    "ad_groups": [
        "example_sources.ad_groups_1"
    ],

    "post_subnet_lookup":
        RCAIP_POST_LOOKUP_CANDIDATES
}

RCAIP_RESOLVED_SOURCE_TABLES = {}

source_alias_status_rows = []

for (
    logical_source_name,
    candidate_tables
) in RCAIP_SOURCE_ALIASES.items():

    resolved_table = resolve_first_available_table(
        candidate_tables,
        required=False
    )

    RCAIP_RESOLVED_SOURCE_TABLES[
        logical_source_name
    ] = resolved_table

    source_alias_status_rows.append(
        (
            logical_source_name,
            candidate_tables[
                0
            ],
            (
                " | ".join(
                    candidate_tables[
                        1:
                    ]
                )

                if len(
                    candidate_tables
                ) > 1

                else
                None
            ),
            resolved_table,
            resolved_table is not None,
            (
                "Preferred Table"

                if resolved_table
                ==
                candidate_tables[
                    0
                ]

                else
                (
                    "Legacy Fallback"

                    if resolved_table

                    else
                    "Not Available"
                )
            )
        )
    )

source_alias_status_schema = T.StructType([
    T.StructField(
        "logical_source_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "preferred_table",
        T.StringType(),
        False
    ),

    T.StructField(
        "legacy_fallback_tables",
        T.StringType(),
        True
    ),

    T.StructField(
        "resolved_table",
        T.StringType(),
        True
    ),

    T.StructField(
        "table_available",
        T.BooleanType(),
        False
    ),

    T.StructField(
        "resolution_status",
        T.StringType(),
        False
    )
])

rcaip_source_alias_status_df = (
    spark.createDataFrame(
        source_alias_status_rows,
        schema=source_alias_status_schema
    )
)

RCAIP_TENABLE_TABLE = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "tenable_assets"
    )
    or
    RCAIP_SOURCE_ALIASES[
        "tenable_assets"
    ][
        0
    ]
)

RCAIP_INTUNE_TABLE = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "intune_devices"
    )
    or
    RCAIP_SOURCE_ALIASES[
        "intune_devices"
    ][
        0
    ]
)

RCAIP_MDE_TABLE = (
    RCAIP_RESOLVED_SOURCE_TABLES.get(
        "mde_device_info"
    )
    or
    RCAIP_SOURCE_ALIASES[
        "mde_device_info"
    ][
        0
    ]
)


# ---------------------------------------------------------------------------
# 7. Publish authoritative and legacy-compatible variables
# ---------------------------------------------------------------------------

RCAIP_POST_CONFIG = {
    "post_name":
        RCAIP_POST_NAME,

    "post_slug":
        RCAIP_POST_SLUG,

    "country_name":
        RCAIP_COUNTRY_NAME,

    "post_device_prefix":
        RCAIP_POST_DEVICE_PREFIX,

    "post_tokens":
        list(
            RCAIP_POST_TOKENS
        ),

    "dns_tokens":
        list(
            RCAIP_DNS_TOKENS
        ),

    "user_location_tokens":
        list(
            RCAIP_USER_LOCATION_TOKENS
        ),

    "mde_tokens":
        list(
            RCAIP_MDE_TOKENS
        ),

    "enterprise_cidrs":
        list(
            RCAIP_ENTERPRISE_CIDRS
        ),

    "nen_public_targets":
        list(
            RCAIP_NEN_PUBLIC_TARGETS
        ),

    "entwifi_public_targets":
        list(
            RCAIP_ENTWIFI_PUBLIC_TARGETS
        ),

    "base_dir":
        RCAIP_INPUT_BASE_DIR,

    "cmdb_file":
        RCAIP_CMDB_FILE,

    "qualys_easm_file":
        RCAIP_QUALYS_EASM_FILE,

    "nmap_scans":
        copy.deepcopy(
            RCAIP_NMAP_SCANS
        ),

    "output_schema":
        RCAIP_RESULTS_NAMESPACE,

    "tenable_table":
        RCAIP_TENABLE_TABLE,

    "intune_table":
        RCAIP_INTUNE_TABLE,

    "mde_table":
        RCAIP_MDE_TABLE
}

RCAIP_CERTIFIED_POST_PROFILE = copy.deepcopy(
    RCAIP_POST_CONFIG
)

# Compatibility only. Config 000 never reads a hard-coded fallback profile.
RCAIP_FALLBACK_POST_PROFILES = {
    RCAIP_POST_SLUG:
        copy.deepcopy(
            RCAIP_CERTIFIED_POST_PROFILE
        )
}

fallback_profile = copy.deepcopy(
    RCAIP_CERTIFIED_POST_PROFILE
)

post_config = copy.deepcopy(
    RCAIP_POST_CONFIG
)

post_name = RCAIP_POST_NAME
post_slug = RCAIP_POST_SLUG
post_device_prefix = RCAIP_POST_DEVICE_PREFIX

dns_tokens = list(
    RCAIP_DNS_TOKENS
)

user_location_tokens = list(
    RCAIP_USER_LOCATION_TOKENS
)

mde_tokens = list(
    RCAIP_MDE_TOKENS
)

opennet_cidrs = list(
    RCAIP_ENTERPRISE_CIDRS
)

nen_public_targets = list(
    RCAIP_NEN_PUBLIC_TARGETS
)

entwifi_public_targets = list(
    RCAIP_ENTWIFI_PUBLIC_TARGETS
)

base_dir = RCAIP_INPUT_BASE_DIR
cmdb_file = RCAIP_CMDB_FILE
qualys_easm_file = RCAIP_QUALYS_EASM_FILE

nmap_scans = copy.deepcopy(
    RCAIP_NMAP_SCANS
)

nmap_file = (
    nmap_scans[
        0
    ][
        "file"
    ]

    if nmap_scans

    else
    None
)

output_schema = RCAIP_RESULTS_NAMESPACE
tenable_table = RCAIP_TENABLE_TABLE
intune_table = RCAIP_INTUNE_TABLE
mde_table = RCAIP_MDE_TABLE


# ---------------------------------------------------------------------------
# 8. Shared rules and Nmap provenance defaults
# ---------------------------------------------------------------------------

RCAIP_EVIDENCE_FRESHNESS_THRESHOLDS = {
    "current_max_days": 30,
    "recent_max_days": 90
}


def classify_evidence_freshness(
    age_days
):

    if age_days is None:
        return "Unknown Scan Date"

    if age_days < 0:
        return "Invalid Future Date"

    if (
        age_days
        <=
        RCAIP_EVIDENCE_FRESHNESS_THRESHOLDS[
            "current_max_days"
        ]
    ):
        return "Current"

    if (
        age_days
        <=
        RCAIP_EVIDENCE_FRESHNESS_THRESHOLDS[
            "recent_max_days"
        ]
    ):
        return "Recent"

    return "Stale Historical"


RCAIP_SCANNER_TRUST_LEVELS = {
    "ENTERPRISE_MANAGED": {
        "display_name":
            "Enterprise Managed Scanner",

        "evidence_confidence":
            "High"
    },

    "POST_MANAGED": {
        "display_name":
            "Post Managed Scanner",

        "evidence_confidence":
            "High"
    },

    "AUTHORIZED_ASSESSMENT": {
        "display_name":
            "Authorized Assessment Computer",

        "evidence_confidence":
            "Medium"
    },

    "UNVERIFIED": {
        "display_name":
            "Unmanaged or Unverified Scanner",

        "evidence_confidence":
            "Low"
    },

    "UNKNOWN": {
        "display_name":
            "Unknown Scanner",

        "evidence_confidence":
            "Unknown"
    }
}

RCAIP_NMAP_SCANNER_NAME = "UNKNOWN"
RCAIP_NMAP_SCANNER_IP = "UNKNOWN"
RCAIP_NMAP_SCANNER_TRUST = "AUTHORIZED_ASSESSMENT"
RCAIP_NMAP_COLLECTION_METHOD = "XML_UPLOAD"
RCAIP_NMAP_COLLECTOR = "RCSO"
RCAIP_NMAP_AUTHORIZATION_REFERENCE = "CSA_ASSESSMENT"

scanner_trust_configuration = (
    RCAIP_SCANNER_TRUST_LEVELS[
        RCAIP_NMAP_SCANNER_TRUST
    ]
)

RCAIP_NMAP_SCANNER_TRUST_DISPLAY = (
    scanner_trust_configuration[
        "display_name"
    ]
)

RCAIP_NMAP_EVIDENCE_CONFIDENCE = (
    scanner_trust_configuration[
        "evidence_confidence"
    ]
)

RCAIP_NMAP_PROVENANCE_COLUMNS = [
    "scan_id",
    "scan_start_timestamp",
    "scan_end_timestamp",
    "scan_duration_seconds",
    "scan_age_days",
    "scan_freshness_status",
    "scan_arguments",
    "scan_target_specification",
    "nmap_version",
    "nmap_xml_output_version",
    "scanner_name",
    "scanner_ip",
    "scanner_trust_class",
    "scanner_trust_display",
    "evidence_confidence",
    "collection_method",
    "collector",
    "authorization_reference",
    "source_file_name",
    "source_file_path",
    "source_file_modified_timestamp",
    "source_file_size_bytes",
    "source_file_sha256",
    "scan_hosts_up",
    "scan_hosts_down",
    "scan_hosts_total"
]

RCAIP_TERMINOLOGY_RULES = {
    "IRM":
        "DT",

    "Information Resource Management":
        "Diplomatic Technology"
}


def apply_rcaip_terminology(
    text_value
):

    if text_value is None:
        return None

    updated_text = str(
        text_value
    )

    for (
        deprecated_term,
        preferred_term
    ) in RCAIP_TERMINOLOGY_RULES.items():

        updated_text = re.sub(
            rf"\b{re.escape(deprecated_term)}\b",
            preferred_term,
            updated_text,
            flags=re.IGNORECASE
        )

    return updated_text


RCAIP_DEVICE_TYPE_RULES = [
    {
        "canonical_device_type":
            "TFL",

        "observed_tokens": [
            "TFL"
        ],

        "device_form":
            "Mobile Computer - Generally Laptop",

        "issuance_model":
            "Enterprise Issued",

        "assignment_model":
            "Normally Assigned to an Individual",

        "management_model":
            "Enterprise Managed",

        "opennet_expectation":
            "Likely Expected",

        "required_tools": [
            "MDE"
        ],

        "possible_additional_sources": [
            "GoMobile"
        ],

        "rule_confidence":
            "High",

        "rule_guardrail":
            "The TFL token does not by itself prove current user assignment, "
            "lifecycle, ownership, or management health."
    },

    {
        "canonical_device_type":
            "EMD",

        "observed_tokens": [
            "EMD"
        ],

        "device_form":
            "Mobile Computer - Generally Laptop",

        "issuance_model":
            "Enterprise Managed Device",

        "assignment_model":
            "May Be Individual or Shared",

        "management_model":
            "Enterprise Managed",

        "opennet_expectation":
            "Likely Expected",

        "required_tools": [
            "MDE"
        ],

        "possible_additional_sources": [
            "GoMobile"
        ],

        "rule_confidence":
            "High",

        "rule_guardrail":
            "The EMD token does not prove assignment to a specific individual."
    },

    {
        "canonical_device_type":
            "MID",

        "observed_tokens": [
            "MID"
        ],

        "device_form":
            "Mobile Computer - Generally Laptop",

        "issuance_model":
            "Post Issued",

        "assignment_model":
            "Post Determined",

        "management_model":
            "Post Issued with Required Enterprise Security Tooling",

        "opennet_expectation":
            "Not Necessarily Expected",

        "required_tools": [
            "MDE",
            "Intune",
            "Qualys"
        ],

        "possible_additional_sources": [
            "GoMobile"
        ],

        "rule_confidence":
            "High",

        "rule_guardrail":
            "Absence from OpenNet-oriented evidence is not independently a "
            "finding. Missing MDE, Intune, or Qualys evidence requires review."
    },

    {
        "canonical_device_type":
            "SHARED_KIOSK_MOBILE_COMPUTER",

        "observed_tokens": [
            "SAKIO",
            "SUKIO"
        ],

        "device_form":
            "Mobile Computer - Generally Laptop",

        "issuance_model":
            "Post or Program Issued",

        "assignment_model":
            "Shared or Kiosk-Style Multi-User Device",

        "management_model":
            "Managed Mobile Computer",

        "opennet_expectation":
            "Validate with Post",

        "required_tools": [
            "MDE",
            "Intune",
            "Qualys"
        ],

        "possible_additional_sources": [],

        "rule_confidence":
            "Medium",

        "rule_guardrail":
            "SAKIO and SUKIO remain observed aliases until DT confirms whether "
            "they represent separate formal classes."
    }
]

device_rule_rows = [
    (
        device_rule[
            "canonical_device_type"
        ],
        " | ".join(
            device_rule[
                "observed_tokens"
            ]
        ),
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
        ]
    )

    for device_rule
    in RCAIP_DEVICE_TYPE_RULES
]

device_rule_schema = T.StructType([
    T.StructField(
        "canonical_device_type",
        T.StringType(),
        False
    ),

    T.StructField(
        "observed_tokens",
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
    )
])

rcaip_device_type_rules_df = (
    spark.createDataFrame(
        device_rule_rows,
        schema=device_rule_schema
    )
)

RCAIP_NOTEBOOK_SEQUENCE = [
    {
        "run_order": -1,

        "notebook_name":
            "RCAIP_RUN_000_Post_Intake_and_Readiness",

        "purpose":
            "Field intake, evidence readiness, and registry publication"
    },

    {
        "run_order": 0,

        "notebook_name":
            "RCAIP_Config_001_Table_Registry",

        "purpose":
            "Entry point after intake; automatically executes Config 000 and "
            "publishes the shared table registry"
    },

    {
        "run_order": 1,

        "notebook_name":
            "RCAIP_Discovery_001_Management_and_Control_Data_Catalog",

        "purpose":
            "Enterprise source discovery and source-contract inspection"
    },

    {
        "run_order": 2,

        "notebook_name":
            "RCAIP_IP_001_Open_Port_Intelligence v2.0",

        "purpose":
            "Nmap and open-port evidence ingestion and interpretation"
    },

    {
        "run_order": 3,

        "notebook_name":
            "RCAIP_Asset_Engine_v3",

        "purpose":
            "Source normalization and observed-asset reconciliation"
    },

    {
        "run_order": 4,

        "notebook_name":
            "RCAIP_IP_002_Asset_Boundary_and_Coverage_Work_Package",

        "purpose":
            "Asset plane, management boundary, and coverage analysis"
    },

    {
        "run_order": 5,

        "notebook_name":
            "RCAIP-IP-003_Asset_Lifecycle_and_Hygiene",

        "purpose":
            "Lifecycle, freshness, stale evidence, and hygiene analysis"
    },

    {
        "run_order": 6,

        "notebook_name":
            "RCAIP_IP_004_Identity_and_AD_Hygiene",

        "purpose":
            "AD identity, account, group, and privilege review"
    },

    {
        "run_order": 7,

        "notebook_name":
            "RCAIP_Engine_009_Operational_Management",

        "purpose":
            "Operational reconciliation and validation workflow"
    },

    {
        "run_order": 8,

        "notebook_name":
            "RCAIP_Report_001_DT_Validation_Package",

        "purpose":
            "Post, enterprise, and ServiceNow-ready reporting"
    }
]

notebook_sequence_schema = T.StructType([
    T.StructField(
        "run_order",
        T.IntegerType(),
        False
    ),

    T.StructField(
        "notebook_name",
        T.StringType(),
        False
    ),

    T.StructField(
        "purpose",
        T.StringType(),
        False
    )
])

rcaip_notebook_sequence_df = (
    spark.createDataFrame(
        [
            (
                notebook_record[
                    "run_order"
                ],
                notebook_record[
                    "notebook_name"
                ],
                notebook_record[
                    "purpose"
                ]
            )

            for notebook_record
            in RCAIP_NOTEBOOK_SEQUENCE
        ],
        schema=notebook_sequence_schema
    )
    .orderBy(
        "run_order"
    )
)


# ---------------------------------------------------------------------------
# 9. Certify the complete Config 000 contract
# ---------------------------------------------------------------------------

expected_sample_table = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    f"{RCAIP_POST_SLUG}_"
    "asset_reconciliation_scored"
)

actual_sample_table = rcaip_table(
    "asset_reconciliation_scored"
)

validation_checks = [
    (
        "Certified profile registry record",
        "Exactly one active certified profile",
        str(
            len(
                profile_matches
            )
        ),
        len(
            profile_matches
        ) == 1
    ),

    (
        "Certified run registry record",
        "Exactly one active certified run",
        str(
            len(
                run_matches
            )
        ),
        len(
            run_matches
        ) == 1
    ),

    (
        "Post profile status",
        "CERTIFIED",
        str(
            profile_row[
                "profile_status"
            ]
        ),
        str(
            profile_row[
                "profile_status"
            ]
        ).upper()
        ==
        "CERTIFIED"
    ),

    (
        "Run status",
        "CERTIFIED_INTAKE",
        str(
            run_row[
                "run_status"
            ]
        ),
        str(
            run_row[
                "run_status"
            ]
        ).upper()
        ==
        "CERTIFIED_INTAKE"
    ),

    (
        "Country",
        "Nonblank and not UNKNOWN",
        RCAIP_COUNTRY_NAME,
        bool(
            RCAIP_COUNTRY_NAME
        )
        and
        RCAIP_COUNTRY_NAME.upper()
        !=
        "UNKNOWN"
    ),

    (
        "Assessment dates",
        "Valid ordered dates",
        (
            f"{RCAIP_ASSESSMENT_START_DATE_TEXT} to "
            f"{RCAIP_ASSESSMENT_END_DATE_TEXT}"
        ),
        RCAIP_ASSESSMENT_END_DATE
        >=
        RCAIP_ASSESSMENT_START_DATE
    ),

    (
        "Run ID",
        expected_run_id,
        RCAIP_RUN_ID,
        RCAIP_RUN_ID
        ==
        expected_run_id
    ),

    (
        "Post table naming contract",
        expected_sample_table,
        actual_sample_table,
        expected_sample_table
        ==
        actual_sample_table
    ),

    (
        "Post tokens include slug",
        RCAIP_POST_SLUG,
        " | ".join(
            RCAIP_POST_TOKENS
        ),
        RCAIP_POST_SLUG
        in
        RCAIP_POST_TOKENS
    ),

    (
        "Enterprise CIDR syntax",
        "No invalid CIDRs",
        (
            "PASS"

            if not invalid_enterprise_cidrs

            else
            " | ".join(
                invalid_enterprise_cidrs
            )
        ),
        not invalid_enterprise_cidrs
    ),

    (
        "CMDB file",
        RCAIP_CMDB_PATH,
        str(
            RCAIP_CMDB_FILE_AVAILABLE
        ),
        RCAIP_CMDB_FILE_AVAILABLE
    ),

    (
        "Qualys file",
        RCAIP_QUALYS_EASM_PATH
        or
        "NOT CONFIGURED",
        str(
            RCAIP_QUALYS_FILE_AVAILABLE
        ),
        RCAIP_QUALYS_FILE_AVAILABLE
    ),

    (
        "Nmap files",
        "All configured files available",
        (
            "PASS"

            if not missing_nmap_paths

            else
            " | ".join(
                missing_nmap_paths
            )
        ),
        not missing_nmap_paths
    ),

    (
        "Nmap manifest",
        "All records contain required fields",
        (
            "PASS"

            if not invalid_nmap_scan_indexes

            else
            " | ".join(
                str(
                    scan_index
                )

                for scan_index
                in invalid_nmap_scan_indexes
            )
        ),
        not invalid_nmap_scan_indexes
    ),

    (
        "Assessment input manifest",
        "Complete",
        str(
            RCAIP_POST_INPUTS_READY
        ),
        RCAIP_POST_INPUTS_READY
    ),

    (
        "AAD audit source",
        "Preferred or legacy source available",
        (
            RCAIP_RESOLVED_SOURCE_TABLES.get(
                "o365_aad_audit"
            )
            or
            "NOT AVAILABLE"
        ),
        RCAIP_RESOLVED_SOURCE_TABLES.get(
            "o365_aad_audit"
        )
        is not None
    ),

    (
        "Workspace run directory",
        "Directory exists",
        RCAIP_WORKSPACE_RUN_ROOT,
        os.path.isdir(
            RCAIP_WORKSPACE_RUN_ROOT
        )
    ),

    (
        "Legacy Post configuration alias",
        "Matches authoritative configuration",
        str(
            post_config
            ==
            RCAIP_POST_CONFIG
        ),
        post_config
        ==
        RCAIP_POST_CONFIG
    ),

    (
        "Legacy Post identity aliases",
        "Match authoritative identity",
        (
            f"{post_name} | "
            f"{post_slug}"
        ),
        post_name
        ==
        RCAIP_POST_NAME
        and
        post_slug
        ==
        RCAIP_POST_SLUG
    ),

    (
        "Legacy output schema alias",
        RCAIP_RESULTS_NAMESPACE,
        output_schema,
        output_schema
        ==
        RCAIP_RESULTS_NAMESPACE
    ),

    (
        "Configuration errors",
        "None",
        (
            "NONE"

            if not configuration_errors

            else
            " | ".join(
                configuration_errors
            )
        ),
        not configuration_errors
    )
]

validation_schema = T.StructType([
    T.StructField(
        "validation_check",
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

rcaip_configuration_integrity_df = (
    spark.createDataFrame(
        validation_checks,
        schema=validation_schema
    )
)

failed_configuration_checks = [
    validation_check

    for validation_check
    in validation_checks

    if not validation_check[
        3
    ]
]

RCAIP_CONFIG_000_CERTIFIED = (
    not failed_configuration_checks
)

RCAIP_CONFIG_000_KB_MODE = (
    "READ_ONLY_CONSUMER"
)


# ---------------------------------------------------------------------------
# 10. Display and publish final ready status
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP CONFIG 000 - CERTIFIED INTAKE RUNTIME RESOLUTION")
print("=" * 100)
print(
    "Status:",
    (
        "PASS"

        if RCAIP_CONFIG_000_CERTIFIED

        else
        "FAIL"
    )
)
print(
    "Selection source:",
    RCAIP_RUNTIME_SELECTION_SOURCE
)
print(
    "Registry mode:",
    RCAIP_CONFIG_000_REGISTRY_MODE
)
print(
    "Selected Post:",
    RCAIP_SELECTED_POST_PROFILE
)
print(
    "Selected Run:",
    RCAIP_RUN_ID
)
print(
    "Profile source:",
    RCAIP_POST_PROFILE_SOURCE
)
print(
    "Profile hash:",
    RCAIP_PROFILE_HASH
)
print("=" * 100)

print("=" * 100)
print("RCAIP CONFIG 000 - PHASE 4H INTEGRITY CHECK")
print("=" * 100)
print(
    "Status:",
    (
        "PASS"

        if RCAIP_CONFIG_000_CERTIFIED

        else
        "FAIL"
    )
)
print(
    "Post:",
    RCAIP_POST_NAME
)
print(
    "Post slug:",
    RCAIP_POST_SLUG
)
print(
    "Country:",
    RCAIP_COUNTRY_NAME
)
print(
    "Post lookup table:",
    RCAIP_POST_LOOKUP_TABLE
    or
    "NOT AVAILABLE"
)
print(
    "Post lookup status:",
    RCAIP_POST_LOOKUP_STATUS
)
print(
    "Post tokens:",
    " | ".join(
        RCAIP_POST_TOKENS
    )
)
print(
    "Enterprise CIDRs:",
    " | ".join(
        RCAIP_ENTERPRISE_CIDRS
    )
)
print(
    "Assessment inputs ready:",
    RCAIP_POST_INPUTS_READY
)
print(
    "Failed checks:",
    len(
        failed_configuration_checks
    )
)
print("=" * 100)

display(
    rcaip_configuration_integrity_df
    .orderBy(
        F.col(
            "check_passed"
        ).asc(),
        "validation_check"
    )
)

display(
    rcaip_source_alias_status_df
)

display(
    rcaip_device_type_rules_df
)

display(
    rcaip_notebook_sequence_df
)

if failed_configuration_checks:

    raise RuntimeError(
        "Config 000 integrity check failed: "
        +
        " | ".join(
            failed_check[
                0
            ]

            for failed_check
            in failed_configuration_checks
        )
    )

required_final_config_objects = [
    "RCAIP_CONFIG_000_CERTIFIED",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_POST_TOKENS",
    "RCAIP_RESULTS_NAMESPACE",
    "RCAIP_POST_CONFIG",
    "post_config",
    "post_name",
    "post_slug",
    "post_device_prefix",
    "dns_tokens",
    "user_location_tokens",
    "mde_tokens",
    "opennet_cidrs",
    "nen_public_targets",
    "entwifi_public_targets",
    "base_dir",
    "cmdb_file",
    "nmap_scans",
    "output_schema",
    "tenable_table",
    "intune_table",
    "mde_table",
    "rcaip_table"
]

missing_final_config_objects = [
    object_name

    for object_name
    in required_final_config_objects

    if object_name not in globals()
]

if missing_final_config_objects:

    raise RuntimeError(
        "The final Config 000 contract is incomplete: "
        +
        " | ".join(
            missing_final_config_objects
        )
    )

RCAIP_CONFIG_000_READY = True

print("=" * 100)
print("RCAIP CONFIG 000 FINAL RUNTIME CONTRACT")
print("=" * 100)
print("Status: PASS")
print(
    "Ready:",
    RCAIP_CONFIG_000_READY
)
print(
    "Configured Post:",
    RCAIP_POST_NAME
)
print(
    "Configured Post slug:",
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
    "Assessment dates:",
    RCAIP_ASSESSMENT_START_DATE_TEXT,
    "to",
    RCAIP_ASSESSMENT_END_DATE_TEXT
)
print(
    "Profile source:",
    RCAIP_POST_PROFILE_SOURCE
)
print(
    "Post tokens:",
    " | ".join(
        RCAIP_POST_TOKENS
    )
)
print(
    "OpenNet CIDRs:",
    " | ".join(
        RCAIP_ENTERPRISE_CIDRS
    )
)
print(
    "Results namespace:",
    RCAIP_RESULTS_NAMESPACE
)
print(
    "Release version:",
    RCAIP_RELEASE_VERSION
)
print(
    "Run mode:",
    RCAIP_RUN_MODE
)
print(
    "Input directory:",
    RCAIP_INPUT_BASE_DIR
)
print(
    "CMDB file:",
    RCAIP_CMDB_FILE
)
print(
    "Nmap scans configured:",
    len(
        RCAIP_NMAP_SCANS
    )
)
print(
    "Sample table:",
    rcaip_table(
        "asset_reconciliation_scored"
    )
)
print(
    "Knowledge Base mode:",
    RCAIP_CONFIG_000_KB_MODE
)
print("Delta writes from Config 000: NONE")
print("Next notebook: RCAIP_Config_001_Table_Registry")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 000 - V3
# Certified Extension Runtime Consumer
#
# Purpose:
#   Add RCAIP V3 Post-context and run-evidence extensions to the mature
#   certified Config 000 runtime without modifying the base intake contract.
#
# Supported modes:
#   BASE_ONLY
#   BASE_PLUS_V3_POST_CONTEXT
#   BASE_PLUS_V3_POST_CONTEXT_AND_RUN_EVIDENCE
#
# Safety:
#   - READ ONLY.
#   - No Delta writes.
#   - Existing certified Post/run registries remain authoritative.
#   - V3 extensions must match the active certified base profile hash.
# ============================================================================

import hashlib
import ipaddress
import json

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# 1. Validate mature Config 000 runtime
# ---------------------------------------------------------------------------

required_base_runtime = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_NAME",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "RCAIP_NMAP_SCANS",
]

missing_base_runtime = [
    object_name
    for object_name in required_base_runtime
    if object_name not in globals()
]

if missing_base_runtime:
    raise RuntimeError(
        "V3 extension consumer requires the mature Config 000 runtime. Missing: "
        + " | ".join(missing_base_runtime)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Mature Config 000 runtime is not ready."
    )


# ---------------------------------------------------------------------------
# 2. Shared certified-intake registry namespace
#
# This is intentionally separate from a Post analytical-output namespace.
# ---------------------------------------------------------------------------

RCAIP_CERTIFIED_INTAKE_REGISTRY_NAMESPACE = "devresults"

RCAIP_BASE_PROFILE_REGISTRY_TABLE = (
    f"{RCAIP_CERTIFIED_INTAKE_REGISTRY_NAMESPACE}."
    "rcaip_post_profile_registry"
)

RCAIP_BASE_RUN_REGISTRY_TABLE = (
    f"{RCAIP_CERTIFIED_INTAKE_REGISTRY_NAMESPACE}."
    "rcaip_assessment_run_registry"
)

RCAIP_V3_POST_CONTEXT_REGISTRY_TABLE = (
    f"{RCAIP_CERTIFIED_INTAKE_REGISTRY_NAMESPACE}."
    "rcaip_post_profile_v3_extension"
)

RCAIP_V3_RUN_EVIDENCE_REGISTRY_TABLE = (
    f"{RCAIP_CERTIFIED_INTAKE_REGISTRY_NAMESPACE}."
    "rcaip_assessment_run_v3_evidence"
)


for required_table in [
    RCAIP_BASE_PROFILE_REGISTRY_TABLE,
    RCAIP_BASE_RUN_REGISTRY_TABLE,
]:
    if not spark.catalog.tableExists(
        required_table
    ):
        raise RuntimeError(
            "Required certified intake registry is unavailable: "
            + required_table
        )


# ---------------------------------------------------------------------------
# 3. Resolve the exact active certified base profile
# ---------------------------------------------------------------------------

active_base_profile_rows = (
    spark.table(
        RCAIP_BASE_PROFILE_REGISTRY_TABLE
    )
    .filter(
        (
            F.lower(
                F.trim(
                    F.col("post_slug")
                )
            )
            ==
            F.lit(
                str(
                    RCAIP_POST_SLUG
                ).strip().lower()
            )
        )
        &
        (
            F.col("active")
            ==
            F.lit(True)
        )
        &
        (
            F.upper(
                F.trim(
                    F.col("profile_status")
                )
            )
            ==
            F.lit("CERTIFIED")
        )
    )
    .collect()
)

if len(active_base_profile_rows) != 1:
    raise RuntimeError(
        "Expected exactly one active certified base profile for "
        f"{RCAIP_POST_SLUG}; found {len(active_base_profile_rows)}."
    )

active_base_profile_row = (
    active_base_profile_rows[0]
)

RCAIP_POST_PROFILE_KEY = str(
    active_base_profile_row[
        "post_profile_key"
    ]
).strip()

RCAIP_BASE_PROFILE_HASH = str(
    active_base_profile_row[
        "profile_hash"
    ]
).strip()

if not RCAIP_POST_PROFILE_KEY:
    raise RuntimeError(
        "Active certified profile has a blank post_profile_key."
    )

if not RCAIP_BASE_PROFILE_HASH:
    raise RuntimeError(
        "Active certified profile has a blank profile_hash."
    )


# ---------------------------------------------------------------------------
# 4. Resolve and verify the exact active assessment run
# ---------------------------------------------------------------------------

active_base_run_rows = (
    spark.table(
        RCAIP_BASE_RUN_REGISTRY_TABLE
    )
    .filter(
        (
            F.col("run_id")
            ==
            F.lit(
                RCAIP_RUN_ID
            )
        )
        &
        (
            F.col("active")
            ==
            F.lit(True)
        )
    )
    .collect()
)

if len(active_base_run_rows) != 1:
    raise RuntimeError(
        "Expected exactly one active certified assessment run for "
        f"{RCAIP_RUN_ID}; found {len(active_base_run_rows)}."
    )

active_base_run_row = (
    active_base_run_rows[0]
)

run_post_profile_key = str(
    active_base_run_row[
        "post_profile_key"
    ]
).strip()

run_base_profile_hash = str(
    active_base_run_row[
        "profile_hash"
    ]
).strip()

if (
    run_post_profile_key
    !=
    RCAIP_POST_PROFILE_KEY
):
    raise RuntimeError(
        "Certified run Post-profile key does not match the active Post profile. "
        f"Profile={RCAIP_POST_PROFILE_KEY}; run={run_post_profile_key}"
    )

if (
    run_base_profile_hash
    !=
    RCAIP_BASE_PROFILE_HASH
):
    raise RuntimeError(
        "Certified run profile hash does not match the active Post profile hash. "
        f"Profile={RCAIP_BASE_PROFILE_HASH}; run={run_base_profile_hash}"
    )


# ---------------------------------------------------------------------------
# 5. Helpers
# ---------------------------------------------------------------------------

def _rcaip_v3_canonical_json(
    value
):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _rcaip_v3_sha256_text(
    value
):
    return hashlib.sha256(
        str(value).encode(
            "utf-8"
        )
    ).hexdigest()


def _rcaip_v3_parse_json(
    raw_value,
    expected_type,
    field_name,
):
    try:
        parsed_value = json.loads(
            str(
                raw_value
                if raw_value is not None
                else ""
            )
        )
    except Exception as exc:
        raise RuntimeError(
            f"Certified V3 field {field_name} contains invalid JSON."
        ) from exc

    if not isinstance(
        parsed_value,
        expected_type
    ):
        raise RuntimeError(
            f"Certified V3 field {field_name} expected "
            f"{expected_type.__name__}; found "
            f"{type(parsed_value).__name__}."
        )

    return parsed_value


def _rcaip_v3_normalize_string_list(
    values
):
    normalized = []

    for value in values:
        text_value = str(
            value
        ).strip()

        if (
            text_value
            and
            text_value not in normalized
        ):
            normalized.append(
                text_value
            )

    return normalized


def _rcaip_v3_scan_text(
    scan_record
):
    if not isinstance(
        scan_record,
        dict
    ):
        return ""

    return " ".join([
        str(
            scan_record.get(
                field_name,
                ""
            )
            or
            ""
        )

        for field_name
        in [
            "file",
            "asset_domain",
            "scan_scope",
            "scan_location",
            "target_cidr",
            "notes",
        ]
    ]).lower()


# ---------------------------------------------------------------------------
# 6. Build safe BASE_ONLY defaults
#
# These defaults create context states, never findings.
# ---------------------------------------------------------------------------

RCAIP_ENTWIFI_INTERNAL_CIDRS = []

RCAIP_UMBRELLA_EDGE_DEPLOYED = (
    "UNKNOWN"
)

RCAIP_UMBRELLA_EDGE_CONFIG_REVIEWED = (
    "NOT_CHECKED"
)

RCAIP_UMBRELLA_ENDPOINT_FILES = []


_base_nmap_scan_texts = [
    _rcaip_v3_scan_text(
        scan_record
    )
    for scan_record
    in (
        RCAIP_NMAP_SCANS
        or
        []
    )
]


_base_has_entwifi_nmap = any(
    (
        "entwifi" in scan_text
        or
        "enterprise wifi" in scan_text
        or
        "enterprise_wifi" in scan_text
        or
        "wifi" in scan_text
    )
    for scan_text
    in _base_nmap_scan_texts
)


_base_has_nen_nmap = any(
    (
        "nen" in scan_text
        and
        not (
            "entwifi" in scan_text
            or
            "enterprise wifi" in scan_text
            or
            "enterprise_wifi" in scan_text
            or
            "wifi" in scan_text
        )
    )
    for scan_text
    in _base_nmap_scan_texts
)


_base_qualys_file = globals().get(
    "RCAIP_QUALYS_EASM_FILE"
)


RCAIP_OPTIONAL_EVIDENCE_STATUS = {
    "QUALYS":
        (
            "PROVIDED"
            if _base_qualys_file
            else
            "NOT_PROVIDED"
        ),

    "NEN_NMAP":
        (
            "PROVIDED"
            if _base_has_nen_nmap
            else
            "NOT_PROVIDED"
        ),

    "ENTERPRISE_WIFI_NMAP":
        (
            "PROVIDED"
            if _base_has_entwifi_nmap
            else
            "NOT_PROVIDED"
        ),

    "ENTERPRISE_WIFI_INTERNAL_CIDRS":
        "NOT_PROVIDED",

    "UMBRELLA_ENDPOINT":
        "NOT_PROVIDED",

    "UMBRELLA_EDGE_DEPLOYMENT":
        "UNKNOWN",

    "UMBRELLA_EDGE_CONFIG_REVIEW":
        "NOT_CHECKED",
}


RCAIP_V3_EXTENSION_MODE = (
    "BASE_ONLY"
)

RCAIP_V3_POST_CONTEXT_AVAILABLE = False
RCAIP_V3_RUN_EVIDENCE_AVAILABLE = False

RCAIP_V3_POST_CONTEXT_EXTENSION_HASH = None
RCAIP_V3_RUN_EVIDENCE_EXTENSION_HASH = None


# ---------------------------------------------------------------------------
# 7. Consume certified V3 Post context if available
# ---------------------------------------------------------------------------

post_context_table_exists = (
    spark.catalog.tableExists(
        RCAIP_V3_POST_CONTEXT_REGISTRY_TABLE
    )
)

matching_post_context_rows = []

if post_context_table_exists:

    matching_post_context_rows = (
        spark.table(
            RCAIP_V3_POST_CONTEXT_REGISTRY_TABLE
        )
        .filter(
            (
                F.col("post_profile_key")
                ==
                F.lit(
                    RCAIP_POST_PROFILE_KEY
                )
            )
            &
            (
                F.col("base_profile_hash")
                ==
                F.lit(
                    RCAIP_BASE_PROFILE_HASH
                )
            )
            &
            (
                F.col("active")
                ==
                F.lit(True)
            )
        )
        .collect()
    )


if len(
    matching_post_context_rows
) > 1:
    raise RuntimeError(
        "More than one active V3 Post-context extension matches "
        "the current certified base profile."
    )


if matching_post_context_rows:

    post_context_row = (
        matching_post_context_rows[0]
    )

    if (
        str(
            post_context_row[
                "extension_status"
            ]
        ).strip().upper()
        !=
        "CERTIFIED"
    ):
        raise RuntimeError(
            "Active V3 Post-context extension is not CERTIFIED."
        )

    _post_entwifi_raw = (
        _rcaip_v3_parse_json(
            post_context_row[
                "entwifi_internal_cidrs_json"
            ],
            list,
            "entwifi_internal_cidrs_json",
        )
    )

    _post_edge_deployed_raw = str(
        post_context_row[
            "umbrella_edge_deployed"
        ]
    ).strip().upper()

    _post_contract_version = str(
        post_context_row[
            "contract_version"
        ]
    ).strip()

    _post_extension_hash = str(
        post_context_row[
            "extension_hash"
        ]
    ).strip()

    _expected_post_payload = {
        "contract_version":
            _post_contract_version,

        "post_profile_key":
            RCAIP_POST_PROFILE_KEY,

        "base_profile_hash":
            RCAIP_BASE_PROFILE_HASH,

        "entwifi_internal_cidrs":
            _post_entwifi_raw,

        "umbrella_edge_deployed":
            _post_edge_deployed_raw,
    }

    _recomputed_post_extension_hash = (
        _rcaip_v3_sha256_text(
            _rcaip_v3_canonical_json(
                _expected_post_payload
            )
        )
    )

    if (
        _recomputed_post_extension_hash
        !=
        _post_extension_hash
    ):
        raise RuntimeError(
            "V3 Post-context extension hash verification failed."
        )


    # Validate and normalize internal EntWiFi CIDRs.
    _validated_internal_cidrs = []

    for cidr_value in _post_entwifi_raw:

        cidr_text = str(
            cidr_value
        ).strip()

        try:
            normalized_cidr = str(
                ipaddress.ip_network(
                    cidr_text,
                    strict=False
                )
            )
        except Exception as exc:
            raise RuntimeError(
                "Certified V3 Enterprise WiFi internal CIDR is invalid: "
                + cidr_text
            ) from exc

        if (
            normalized_cidr
            not in _validated_internal_cidrs
        ):
            _validated_internal_cidrs.append(
                normalized_cidr
            )


    allowed_edge_deployment_states = {
        "YES",
        "NO",
        "UNKNOWN",
        "NOT_APPLICABLE",
    }

    if (
        _post_edge_deployed_raw
        not in allowed_edge_deployment_states
    ):
        raise RuntimeError(
            "Certified V3 Umbrella edge deployment state is invalid: "
            + _post_edge_deployed_raw
        )


    RCAIP_ENTWIFI_INTERNAL_CIDRS = list(
        _validated_internal_cidrs
    )

    RCAIP_UMBRELLA_EDGE_DEPLOYED = (
        _post_edge_deployed_raw
    )

    RCAIP_V3_POST_CONTEXT_EXTENSION_HASH = (
        _post_extension_hash
    )

    RCAIP_V3_POST_CONTEXT_AVAILABLE = True

    RCAIP_OPTIONAL_EVIDENCE_STATUS[
        "ENTERPRISE_WIFI_INTERNAL_CIDRS"
    ] = (
        "PROVIDED"
        if RCAIP_ENTWIFI_INTERNAL_CIDRS
        else
        "NOT_PROVIDED"
    )

    RCAIP_OPTIONAL_EVIDENCE_STATUS[
        "UMBRELLA_EDGE_DEPLOYMENT"
    ] = RCAIP_UMBRELLA_EDGE_DEPLOYED

    RCAIP_V3_EXTENSION_MODE = (
        "BASE_PLUS_V3_POST_CONTEXT"
    )


# ---------------------------------------------------------------------------
# 8. Consume certified V3 run evidence if available
# ---------------------------------------------------------------------------

run_evidence_table_exists = (
    spark.catalog.tableExists(
        RCAIP_V3_RUN_EVIDENCE_REGISTRY_TABLE
    )
)

matching_run_evidence_rows = []

if run_evidence_table_exists:

    matching_run_evidence_rows = (
        spark.table(
            RCAIP_V3_RUN_EVIDENCE_REGISTRY_TABLE
        )
        .filter(
            (
                F.col("run_id")
                ==
                F.lit(
                    RCAIP_RUN_ID
                )
            )
            &
            (
                F.col("post_profile_key")
                ==
                F.lit(
                    RCAIP_POST_PROFILE_KEY
                )
            )
            &
            (
                F.col("base_profile_hash")
                ==
                F.lit(
                    RCAIP_BASE_PROFILE_HASH
                )
            )
            &
            (
                F.col("active")
                ==
                F.lit(True)
            )
        )
        .collect()
    )


if len(
    matching_run_evidence_rows
) > 1:
    raise RuntimeError(
        "More than one active V3 run-evidence extension matches "
        "the current certified run."
    )


if (
    matching_run_evidence_rows
    and
    not RCAIP_V3_POST_CONTEXT_AVAILABLE
):
    raise RuntimeError(
        "V3 run evidence exists without a matching active V3 Post-context "
        "extension for the same certified base profile."
    )


if matching_run_evidence_rows:

    run_evidence_row = (
        matching_run_evidence_rows[0]
    )

    if (
        str(
            run_evidence_row[
                "extension_status"
            ]
        ).strip().upper()
        !=
        "CERTIFIED"
    ):
        raise RuntimeError(
            "Active V3 run-evidence extension is not CERTIFIED."
        )


    _run_umbrella_files_raw = (
        _rcaip_v3_parse_json(
            run_evidence_row[
                "umbrella_endpoint_files_json"
            ],
            list,
            "umbrella_endpoint_files_json",
        )
    )

    _run_optional_status_raw = (
        _rcaip_v3_parse_json(
            run_evidence_row[
                "optional_evidence_status_json"
            ],
            dict,
            "optional_evidence_status_json",
        )
    )

    _run_edge_review_raw = str(
        run_evidence_row[
            "umbrella_edge_config_reviewed"
        ]
    ).strip().upper()

    _run_contract_version = str(
        run_evidence_row[
            "contract_version"
        ]
    ).strip()

    _run_extension_hash = str(
        run_evidence_row[
            "extension_hash"
        ]
    ).strip()


    _expected_run_payload = {
        "contract_version":
            _run_contract_version,

        "run_id":
            RCAIP_RUN_ID,

        "post_profile_key":
            RCAIP_POST_PROFILE_KEY,

        "base_profile_hash":
            RCAIP_BASE_PROFILE_HASH,

        "umbrella_edge_config_reviewed":
            _run_edge_review_raw,

        "umbrella_endpoint_files":
            _run_umbrella_files_raw,

        "optional_evidence_status":
            _run_optional_status_raw,
    }


    _recomputed_run_extension_hash = (
        _rcaip_v3_sha256_text(
            _rcaip_v3_canonical_json(
                _expected_run_payload
            )
        )
    )

    if (
        _recomputed_run_extension_hash
        !=
        _run_extension_hash
    ):
        raise RuntimeError(
            "V3 run-evidence extension hash verification failed."
        )


    allowed_edge_review_states = {
        "YES",
        "NO",
        "NOT_CHECKED",
        "NOT_APPLICABLE",
    }

    if (
        _run_edge_review_raw
        not in allowed_edge_review_states
    ):
        raise RuntimeError(
            "Certified V3 Umbrella edge review state is invalid: "
            + _run_edge_review_raw
        )


    if (
        RCAIP_UMBRELLA_EDGE_DEPLOYED
        ==
        "NO"
        and
        _run_edge_review_raw
        ==
        "YES"
    ):
        raise RuntimeError(
            "Certified V3 context is contradictory: Umbrella edge deployment "
            "is NO but configuration review is YES."
        )


    RCAIP_UMBRELLA_EDGE_CONFIG_REVIEWED = (
        _run_edge_review_raw
    )

    RCAIP_UMBRELLA_ENDPOINT_FILES = (
        _rcaip_v3_normalize_string_list(
            _run_umbrella_files_raw
        )
    )

    RCAIP_OPTIONAL_EVIDENCE_STATUS = {
        str(key):
            str(value)

        for (
            key,
            value
        )
        in _run_optional_status_raw.items()
    }

    RCAIP_V3_RUN_EVIDENCE_EXTENSION_HASH = (
        _run_extension_hash
    )

    RCAIP_V3_RUN_EVIDENCE_AVAILABLE = True

    RCAIP_V3_EXTENSION_MODE = (
        "BASE_PLUS_V3_POST_CONTEXT_AND_RUN_EVIDENCE"
    )


# ---------------------------------------------------------------------------
# 9. Publish a compact V3 runtime contract
# ---------------------------------------------------------------------------

RCAIP_V3_RUNTIME = {
    "mode":
        RCAIP_V3_EXTENSION_MODE,

    "post_profile_key":
        RCAIP_POST_PROFILE_KEY,

    "base_profile_hash":
        RCAIP_BASE_PROFILE_HASH,

    "post_context_available":
        RCAIP_V3_POST_CONTEXT_AVAILABLE,

    "run_evidence_available":
        RCAIP_V3_RUN_EVIDENCE_AVAILABLE,

    "entwifi_internal_cidrs":
        list(
            RCAIP_ENTWIFI_INTERNAL_CIDRS
        ),

    "umbrella_edge_deployed":
        RCAIP_UMBRELLA_EDGE_DEPLOYED,

    "umbrella_edge_config_reviewed":
        RCAIP_UMBRELLA_EDGE_CONFIG_REVIEWED,

    "umbrella_endpoint_files":
        list(
            RCAIP_UMBRELLA_ENDPOINT_FILES
        ),

    "optional_evidence_status":
        dict(
            RCAIP_OPTIONAL_EVIDENCE_STATUS
        ),
}


# ---------------------------------------------------------------------------
# 10. Final V3 certification
# ---------------------------------------------------------------------------

print("=" * 100)
print("RCAIP CONFIG 000 - V3 EXTENSION RUNTIME CONTRACT")
print("=" * 100)

print("Status: PASS")

print(
    "Mode:",
    RCAIP_V3_EXTENSION_MODE
)

print(
    "Post:",
    RCAIP_POST_NAME
)

print(
    "Post profile key:",
    RCAIP_POST_PROFILE_KEY
)

print(
    "Run ID:",
    RCAIP_RUN_ID
)

print(
    "Base profile hash:",
    RCAIP_BASE_PROFILE_HASH
)

print()

print(
    "V3 Post context:",
    (
        "CERTIFIED"
        if RCAIP_V3_POST_CONTEXT_AVAILABLE
        else
        "NOT AVAILABLE - BASE DEFAULTS"
    )
)

print(
    "V3 run evidence:",
    (
        "CERTIFIED"
        if RCAIP_V3_RUN_EVIDENCE_AVAILABLE
        else
        "NOT AVAILABLE - BASE DEFAULTS"
    )
)

print()

print(
    "Enterprise WiFi internal CIDRs:",
    (
        " | ".join(
            RCAIP_ENTWIFI_INTERNAL_CIDRS
        )
        or
        "NOT PROVIDED"
    )
)

print(
    "Umbrella edge deployed:",
    RCAIP_UMBRELLA_EDGE_DEPLOYED
)

print(
    "Umbrella edge config reviewed:",
    RCAIP_UMBRELLA_EDGE_CONFIG_REVIEWED
)

print(
    "Umbrella endpoint files:",
    len(
        RCAIP_UMBRELLA_ENDPOINT_FILES
    )
)

print()

print("OPTIONAL EVIDENCE STATUS")
print("------------------------")

for (
    evidence_name,
    evidence_state
) in sorted(
    RCAIP_OPTIONAL_EVIDENCE_STATUS.items()
):
    print(
        evidence_name + ":",
        evidence_state
    )

print()

print("Delta writes from V3 consumer: NONE")
print("Findings created: NONE")
print("=" * 100)

# ============================================================================
# CONFIG 000 - CERTIFIED V3 NMAP RUNTIME PUBLICATION
#
# Purpose:
#   Rehydrate the current run's certified Nmap evidence from the durable
#   V3 evidence table and publish the runtime objects required downstream.
#
# Architecture:
#   - Certified registry/evidence is runtime authority.
#   - One XML remains one durable parent evidence record.
#   - D0024 multi-CIDR scope is expanded only into runtime target lists.
#   - target_cidrs_json is authoritative for multi-CIDR collections.
#   - target_cidr is compatibility-only; legacy MULTI_TARGET is not a CIDR.
#   - No Delta writes.
# ============================================================================

import ipaddress
import json

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# 1. Required certified runtime
# ---------------------------------------------------------------------------

_required_v3_nmap_runtime_inputs = [
    "RCAIP_CONFIG_000_READY",
    "RCAIP_POST_SLUG",
    "RCAIP_RUN_ID",
    "RCAIP_RESULTS_NAMESPACE",
]

_missing_v3_nmap_runtime_inputs = [
    object_name
    for object_name in _required_v3_nmap_runtime_inputs
    if object_name not in globals()
]

if _missing_v3_nmap_runtime_inputs:
    raise RuntimeError(
        "Config 000 cannot publish the certified V3 Nmap runtime. Missing: "
        + " | ".join(_missing_v3_nmap_runtime_inputs)
    )

if not RCAIP_CONFIG_000_READY:
    raise RuntimeError(
        "Config 000 certified runtime is not ready."
    )


# ---------------------------------------------------------------------------
# 2. Resolve the durable certified Nmap evidence table
# ---------------------------------------------------------------------------

RCAIP_V3_NMAP_RUN_EVIDENCE_TABLE = (
    f"{RCAIP_RESULTS_NAMESPACE}."
    "rcaip_assessment_run_v3_nmap_evidence"
)

if not spark.catalog.tableExists(
    RCAIP_V3_NMAP_RUN_EVIDENCE_TABLE
):
    raise RuntimeError(
        "Certified V3 Nmap evidence table does not exist: "
        + RCAIP_V3_NMAP_RUN_EVIDENCE_TABLE
    )


_required_v3_nmap_columns = {
    "run_id",
    "post_profile_key",
    "base_profile_hash",
    "manifest_sha256",
    "record_sha256",
    "file",
    "source_file_path",
    "asset_domain",
    "scan_scope",
    "scan_location",
    "target_cidr",
    "target_cidrs_json",
    "target_cidr_count",
    "target_source",
    "xml_args_status",
    "notes",
    "scan_perspective",
    "network_role",
    "service_owner",
    "perspective_basis",
    "collection_capability",
    "scan_start_raw",
    "mature_contract_role",
    "v3_semantic_authority",
    "evidence_type",
    "evidence_status",
    "active",
}

_v3_nmap_source_df = spark.table(
    RCAIP_V3_NMAP_RUN_EVIDENCE_TABLE
)

_missing_v3_nmap_columns = sorted(
    _required_v3_nmap_columns
    -
    set(_v3_nmap_source_df.columns)
)

if _missing_v3_nmap_columns:
    raise RuntimeError(
        "Certified V3 Nmap evidence table is missing required columns: "
        + " | ".join(_missing_v3_nmap_columns)
    )


# ---------------------------------------------------------------------------
# 3. Isolate this exact certified Post/run
# ---------------------------------------------------------------------------

_v3_nmap_current_df = (
    _v3_nmap_source_df

    .filter(
        (
            F.lower(
                F.trim(
                    F.col("post_profile_key")
                )
            )
            ==
            F.lit(
                str(RCAIP_POST_SLUG).strip().lower()
            )
        )
        &
        (
            F.col("run_id")
            ==
            F.lit(
                str(RCAIP_RUN_ID).strip()
            )
        )
        &
        (
            F.col("active")
            ==
            F.lit(True)
        )
        &
        (
            F.upper(
                F.trim(
                    F.col("evidence_status")
                )
            )
            ==
            F.lit("CERTIFIED")
        )
    )
)

_v3_nmap_rows = (
    _v3_nmap_current_df
    .orderBy("file")
    .collect()
)

if not _v3_nmap_rows:
    raise RuntimeError(
        "No active certified V3 Nmap evidence exists for "
        f"{RCAIP_POST_SLUG} / {RCAIP_RUN_ID}."
    )


# ---------------------------------------------------------------------------
# 4. D0024-safe CIDR helpers
# ---------------------------------------------------------------------------

def _rcaip_normalize_runtime_cidr(raw_value):
    return str(
        ipaddress.ip_network(
            str(raw_value).strip(),
            strict=False
        )
    )


def _rcaip_parse_certified_target_cidrs(record):
    """
    Return the complete target list for one durable XML parent record.

    D0024:
      - target_cidrs_json is authoritative for multi-target scope.
      - target_cidr remains a valid single-CIDR compatibility field.
      - MULTI_TARGET is accepted only for legacy read compatibility.
      - singular target_cidr is fallback support for older certified rows.
    """

    raw_json = record.get(
        "target_cidrs_json"
    )

    parsed_values = []

    if raw_json is not None and str(raw_json).strip():
        try:
            parsed = json.loads(
                str(raw_json)
            )
        except Exception as exc:
            raise RuntimeError(
                "Certified target_cidrs_json is unreadable for "
                f"{record.get('file')}: {exc}"
            )

        if isinstance(parsed, str):
            parsed = [parsed]

        if not isinstance(parsed, list):
            raise RuntimeError(
                "Certified target_cidrs_json is not a list for "
                f"{record.get('file')}."
            )

        parsed_values = parsed

    # Compatibility fallback for pre-D0024 singular records.
    if not parsed_values:
        compatibility_target = str(
            record.get("target_cidr")
            or ""
        ).strip()

        if compatibility_target.upper() not in {
            "",
            "UNKNOWN",
            "MULTI_TARGET",
        }:
            parsed_values = [
                compatibility_target
            ]

    normalized_values = []

    for raw_target in parsed_values:
        normalized_target = (
            _rcaip_normalize_runtime_cidr(
                raw_target
            )
        )

        if normalized_target not in normalized_values:
            normalized_values.append(
                normalized_target
            )

    return normalized_values


def _rcaip_append_unique(target_list, values):
    for value in values:
        if value not in target_list:
            target_list.append(value)


# ---------------------------------------------------------------------------
# 5. Rehydrate one runtime record per certified XML parent
# ---------------------------------------------------------------------------

_v3_nmap_runtime_errors = []
_v3_nmap_runtime_scans = []

_v3_nmap_manifest_hashes = set()
_v3_nmap_base_profile_hashes = set()
_v3_nmap_files = []

_rcaip_opennet_scan_targets = []
_rcaip_entwifi_internal_observed_targets = []
_rcaip_public_facing_targets = []

_rcaip_nen_public_targets = []
_rcaip_entwifi_public_targets = []

_allowed_scan_perspectives = {
    "INTERNAL_OPENNET",
    "INTERNAL_ENTERPRISE_WIFI",
    "PUBLIC_FACING",
}

for source_row in _v3_nmap_rows:

    record = source_row.asDict(
        recursive=True
    )

    file_name = str(
        record.get("file")
        or ""
    ).strip()

    if not file_name:
        _v3_nmap_runtime_errors.append(
            "Certified Nmap record contains a blank file name."
        )

    _v3_nmap_files.append(
        file_name
    )

    manifest_hash = str(
        record.get("manifest_sha256")
        or ""
    ).strip().lower()

    if manifest_hash:
        _v3_nmap_manifest_hashes.add(
            manifest_hash
        )

    base_profile_hash = str(
        record.get("base_profile_hash")
        or ""
    ).strip().lower()

    if base_profile_hash:
        _v3_nmap_base_profile_hashes.add(
            base_profile_hash
        )

    try:
        target_cidrs = (
            _rcaip_parse_certified_target_cidrs(
                record
            )
        )
    except Exception as exc:
        _v3_nmap_runtime_errors.append(
            str(exc)
        )
        target_cidrs = []

    declared_target_count = (
        record.get(
            "target_cidr_count"
        )
    )

    if (
        declared_target_count is not None
        and
        int(declared_target_count)
        !=
        len(target_cidrs)
    ):
        _v3_nmap_runtime_errors.append(
            "Certified target count mismatch for "
            f"{file_name}: declared="
            f"{declared_target_count}, parsed="
            f"{len(target_cidrs)}."
        )

    if not target_cidrs:
        _v3_nmap_runtime_errors.append(
            "Certified Nmap evidence contains no usable target CIDRs for "
            + file_name
        )

    compatibility_target = str(
        record.get("target_cidr")
        or ""
    ).strip()

    if len(target_cidrs) > 1:
        # D0024: target_cidrs_json is authoritative for the complete
        # multi-CIDR collection. target_cidr remains a single-CIDR
        # compatibility field. Historical MULTI_TARGET rows remain readable,
        # but new certified evidence must not require a non-CIDR sentinel.
        if compatibility_target.upper() != "MULTI_TARGET":
            try:
                normalized_compatibility_target = (
                    _rcaip_normalize_runtime_cidr(
                        compatibility_target
                    )
                )
            except Exception:
                normalized_compatibility_target = None

            if normalized_compatibility_target not in target_cidrs:
                _v3_nmap_runtime_errors.append(
                    "D0024 multi-CIDR compatibility target is not a valid "
                    "member of the certified target set for "
                    f"{file_name}: "
                    f"{compatibility_target or '<BLANK>'}."
                )

    elif len(target_cidrs) == 1:
        try:
            normalized_compatibility_target = (
                _rcaip_normalize_runtime_cidr(
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
            _v3_nmap_runtime_errors.append(
                "Singular compatibility target does not match the "
                f"certified target list for {file_name}."
            )

    scan_perspective = str(
        record.get("scan_perspective")
        or ""
    ).strip().upper()

    if (
        scan_perspective
        not in
        _allowed_scan_perspectives
    ):
        _v3_nmap_runtime_errors.append(
            "Unsupported certified scan perspective for "
            f"{file_name}: {scan_perspective or '<BLANK>'}."
        )

    network_role = str(
        record.get("network_role")
        or ""
    ).strip().upper()

    if scan_perspective == "INTERNAL_OPENNET":
        _rcaip_append_unique(
            _rcaip_opennet_scan_targets,
            target_cidrs
        )

    elif scan_perspective == "INTERNAL_ENTERPRISE_WIFI":
        _rcaip_append_unique(
            _rcaip_entwifi_internal_observed_targets,
            target_cidrs
        )

    elif scan_perspective == "PUBLIC_FACING":
        _rcaip_append_unique(
            _rcaip_public_facing_targets,
            target_cidrs
        )

        if network_role == "NEN_PUBLIC":
            _rcaip_append_unique(
                _rcaip_nen_public_targets,
                target_cidrs
            )

        if network_role == "ENTERPRISE_WIFI_PUBLIC":
            _rcaip_append_unique(
                _rcaip_entwifi_public_targets,
                target_cidrs
            )

    # Preserve one runtime record per XML parent.
    # Add the D0024 list without replacing the mature compatibility field.
    runtime_record = dict(
        record
    )

    runtime_record[
        "target_cidrs"
    ] = list(
        target_cidrs
    )

    _v3_nmap_runtime_scans.append(
        runtime_record
    )


# ---------------------------------------------------------------------------
# 6. Parent-record and binding validation
# ---------------------------------------------------------------------------

if len(set(_v3_nmap_files)) != len(_v3_nmap_files):
    _v3_nmap_runtime_errors.append(
        "Certified V3 Nmap evidence contains duplicate XML parent records."
    )

if len(_v3_nmap_manifest_hashes) != 1:
    _v3_nmap_runtime_errors.append(
        "Current certified Nmap rows do not resolve to exactly one "
        "manifest SHA-256."
    )

if len(_v3_nmap_base_profile_hashes) != 1:
    _v3_nmap_runtime_errors.append(
        "Current certified Nmap rows do not resolve to exactly one "
        "base-profile hash."
    )


# ---------------------------------------------------------------------------
# 7. Refuse partial runtime publication
# ---------------------------------------------------------------------------

if _v3_nmap_runtime_errors:

    print("=" * 100)
    print("RCAIP CONFIG 000 - CERTIFIED V3 NMAP RUNTIME")
    print("=" * 100)
    print("Status: REVIEW_REQUIRED")

    for error_text in _v3_nmap_runtime_errors:
        print("-", error_text)

    print("=" * 100)

    raise RuntimeError(
        "Certified V3 Nmap runtime publication failed: "
        + " | ".join(
            _v3_nmap_runtime_errors
        )
    )


# ---------------------------------------------------------------------------
# 8. Publish authoritative runtime objects
# ---------------------------------------------------------------------------

RCAIP_NMAP_SCANS = list(
    _v3_nmap_runtime_scans
)

RCAIP_V3_NMAP_MANIFEST_SHA256 = next(
    iter(
        _v3_nmap_manifest_hashes
    )
)

RCAIP_OPENNET_SCAN_TARGETS = list(
    _rcaip_opennet_scan_targets
)

RCAIP_ENTWIFI_INTERNAL_OBSERVED_TARGETS = list(
    _rcaip_entwifi_internal_observed_targets
)

RCAIP_PUBLIC_FACING_TARGETS = list(
    _rcaip_public_facing_targets
)

# D0020 compatibility projections from certified run evidence.
RCAIP_NEN_PUBLIC_TARGETS = list(
    _rcaip_nen_public_targets
)

RCAIP_ENTWIFI_PUBLIC_TARGETS = list(
    _rcaip_entwifi_public_targets
)

# Preserve mature lowercase compatibility aliases where downstream code
# still consumes them.
nmap_scans = RCAIP_NMAP_SCANS
nen_public_targets = RCAIP_NEN_PUBLIC_TARGETS
entwifi_public_targets = RCAIP_ENTWIFI_PUBLIC_TARGETS

RCAIP_V3_NMAP_RUN_EVIDENCE_READY = True
RCAIP_V3_NMAP_RUNTIME_READY = True


# ---------------------------------------------------------------------------
# 9. Runtime certification summary
# ---------------------------------------------------------------------------

_v3_multi_target_parent_count = sum(
    1
    for record in RCAIP_NMAP_SCANS
    if len(
        record.get(
            "target_cidrs",
            []
        )
    ) > 1
)

print("=" * 100)
print("RCAIP CONFIG 000 - CERTIFIED V3 NMAP RUNTIME")
print("=" * 100)
print("Status: PASS")
print("Post:", RCAIP_POST_NAME)
print("Post slug:", RCAIP_POST_SLUG)
print("Run ID:", RCAIP_RUN_ID)
print(
    "Certified XML parent records:",
    len(RCAIP_NMAP_SCANS)
)
print(
    "Multi-target XML parent records:",
    _v3_multi_target_parent_count
)
print(
    "OpenNet observed target CIDRs:",
    len(RCAIP_OPENNET_SCAN_TARGETS)
)
print(
    "Enterprise WiFi internal observed target CIDRs:",
    len(RCAIP_ENTWIFI_INTERNAL_OBSERVED_TARGETS)
)
print(
    "Public-facing observed target CIDRs:",
    len(RCAIP_PUBLIC_FACING_TARGETS)
)
print(
    "NEN public compatibility targets:",
    len(RCAIP_NEN_PUBLIC_TARGETS)
)
print(
    "Enterprise WiFi public compatibility targets:",
    len(RCAIP_ENTWIFI_PUBLIC_TARGETS)
)
print(
    "Manifest SHA-256:",
    RCAIP_V3_NMAP_MANIFEST_SHA256
)
print(
    "Runtime ready:",
    RCAIP_V3_NMAP_RUNTIME_READY
)
print("Delta writes: NONE")
print("=" * 100)

# COMMAND ----------

# ============================================================================
# RCAIP CONFIG 000
# Cell 3 - Knowledge Base Consumer Contract
#
# Purpose:
#   Preserve the downstream KB-mode contract without repeating lookup discovery
#   already certified by Cell 1.
#
# Writes: NONE
# ============================================================================

if not globals().get("RCAIP_CONFIG_000_READY", False):
    raise RuntimeError(
        "Config 000 certified runtime is not ready."
    )

RCAIP_CONFIG_000_KB_MODE = "READ_ONLY_CONSUMER"

print("=" * 100)
print("RCAIP CONFIG 000 - KNOWLEDGE BASE CONSUMER CONTRACT")
print("=" * 100)
print("Status: PASS")
print("Post:", RCAIP_POST_NAME)
print("Run ID:", RCAIP_RUN_ID)
print("Mode:", RCAIP_CONFIG_000_KB_MODE)
print("Delta writes: NONE")
print("=" * 100)