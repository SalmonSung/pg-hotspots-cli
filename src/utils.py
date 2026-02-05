import shutil
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
import json
import subprocess
import click
import logging
import google.auth
from google.auth.exceptions import DefaultCredentialsError
import config


def bytes_to_unit(value_bytes: float, unit: str = "GiB") -> float:
    """
    Convert raw bytes to the requested unit.

    Notes
    -----
    - Returns 0.0 for None to keep Plotly traces and sunbursts stable.
    """
    if value_bytes is None:
        return 0.0

    unit = unit.lower()

    if unit in ("b", "bytes"):
        return float(value_bytes)
    if unit == "mib":
        return float(value_bytes) / (1024.0 ** 2)
    if unit == "gib":
        return float(value_bytes) / (1024.0 ** 3)

    # Safe fallback: return bytes unchanged
    return float(value_bytes)


def ensure_adc_login():
    """
    Ensures Google Application Default Credentials (ADC) are available.
    Runs `gcloud auth application-default login` only if needed.
    """
    try:
        credentials, project = google.auth.default()
        logging.info('Application Default Credentials already configured.')
        if project:
            logging.info(f'Project: {project}')
        return True

    except DefaultCredentialsError:
        logging.info('ADC not found. Launching gcloud login...')

        gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd")
        if not gcloud:
            logging.error(f'gcloud command not found')
            raise RuntimeError(
                "gcloud command not installed: Please check: https://docs.cloud.google.com/sdk/docs/install-sdk")

        try:
            subprocess.run(
                [gcloud, "auth", "application-default", "login"],
                check=True,
            )
        except Exception as e:
            logging.error(e)
            return False
        logging.info("ADC login successful.")
        return True


# Todo: that means your Monitoring API auth + project are correct, and Cloud SQL metrics are visible.
def check_project_endpoints():
    from datetime import datetime, timedelta, timezone
    from google.cloud import logging_v2

    PROJECT_ID = "psql-hotspots"
    client = logging_v2.Client(project=PROJECT_ID)

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)

    start_s = start.isoformat().replace("+00:00", "Z")
    end_s = end.isoformat().replace("+00:00", "Z")

    log_filter = f'''
    resource.type="cloudsql_database"
    timestamp>="{start_s}"
    timestamp<="{end_s}"
    '''

    it = client.list_entries(filter_=log_filter, order_by=logging_v2.DESCENDING, page_size=50)

    found = 0
    for entry in it:
        found += 1
        # Print raw-ish content to see where text lives
        print("logName:", entry.log_name)
        print("labels:", dict(entry.resource.labels))
        print("payload type:", type(entry.payload))
        print("payload:", entry.payload)
        print("----")
        if found >= 5:
            break

    if found == 0:
        print(
            "No Cloud SQL log entries found in this project/time window (wrong project, no permissions, or logs not in this project).")


def load_db_secret_list(path: str) -> list[dict]:
    path = Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        return []

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_utc_minute(value: Optional[str]) -> Optional[datetime]:
    """Parse 'YYYY-MM-DDTHH:MM' (UTC), no seconds. Returns tz-aware UTC datetime."""
    if value is None:
        return None

    s = value.strip()
    # allow space separator
    s = s.replace(" ", "T")
    # allow trailing Z (we always treat as UTC anyway)
    if s.endswith("Z"):
        s = s[:-1]

    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M")
    except ValueError as e:
        raise click.BadParameter(
            "Invalid datetime. Use UTC format: YYYY-MM-DDTHH:MM (no seconds), "
            "e.g. 2026-01-29T10:15"
        ) from e

    return dt.replace(tzinfo=timezone.utc)


def write_table_txt(columns: list[str], rows: list[dict], filename: str) -> None:
    # Determine column widths (max of header vs values)
    widths = {}
    for col in columns:
        max_len = len(col)
        for row in rows:
            val = str(row.get(col, "")) if row.get(col) is not None else ""
            max_len = max(max_len, len(val))
        widths[col] = max_len

    def format_row(values):
        return " | ".join(f"{v:<{widths[c]}}" for v, c in zip(values, columns))

    file_path = config.OUTPUT_DIR_PATH / filename

    with open(file_path, "w", encoding="utf-8") as f:
        # Header
        f.write(format_row(columns) + "\n")

        # Separator
        f.write(
            " | ".join("-" * widths[col] for col in columns) + "\n"
        )

        # Rows
        for row in rows:
            values = [str(row.get(col, "")) if row.get(col) is not None else "" for col in columns]
            f.write(format_row(values) + "\n")


def get_disk_iops_tp(tier_str: str, availability: str) -> dict:
    """
    Parses a Cloud SQL tier string and maps it to performance metrics.

    Args:
        tier_str (str): e.g., "db-custom-8-32768"
        availability (str): "REGIONAL" or "ZONAL"

    Returns:
        dict: The mapped IOPS and throughput values.
    """
    if tier_str == "db-f1-micro" or "db-g1-small":
        if availability == "REGIONAL":
            return {
                "max_iops_rw": (15000, 15000),
                "max_throughput_rw": (200, 100)
            }
        else:
            return {
                "max_iops_rw": (15000, 15000),
                "max_throughput_rw": (200, 200)
            }

    parts = tier_str.split("-")


    cpu_count = int(parts[-2])

    # Extract prefix info for the mapping key: (db, custom, availability)
    # parts[0] is 'db', parts[1] is 'custom'
    mapping_key = (parts[0], parts[1], availability.upper())

    # 2. Performance Mapping Table
    # Keys represent the start of the vCPU range
    perf_map = {
        1: {
            "ZONAL": {"iops": (15000, 15000), "tp": (200, 200)},
            "REGIONAL": {"iops": (15000, 15000), "tp": (200, 100)}
        },
        2: {
            "ZONAL": {"iops": (15000, 15000), "tp": (240, 240)},
            "REGIONAL": {"iops": (15000, 15000), "tp": (240, 120)}
        },
        8: {
            "ZONAL": {"iops": (15000, 15000), "tp": (800, 800)},
            "REGIONAL": {"iops": (15000, 15000), "tp": (800, 400)}
        },
        16: {
            "ZONAL": {"iops": (25000, 25000), "tp": (1200, 1200)},
            "REGIONAL": {"iops": (25000, 25000), "tp": (1200, 600)}
        },
        32: {
            "ZONAL": {"iops": (60000, 60000), "tp": (1200, 1200)},
            "REGIONAL": {"iops": (60000, 60000), "tp": (1200, 600)}
        },
        64: {
            "ZONAL": {"iops": (100000, 100000), "tp": (1200, 1200)},
            "REGIONAL": {"iops": (100000, 80000), "tp": (1200, 1000)}
        }
    }

    # 3. Logic to find the correct vCPU bucket
    if cpu_count >= 64:
        bucket = 64
    elif cpu_count >= 32:
        bucket = 32
    elif cpu_count >= 16:
        bucket = 16
    elif cpu_count >= 8:
        bucket = 8
    elif cpu_count >= 2:
        bucket = 2
    else:
        bucket = 1

    # Fetch the results based on the bucket and availability
    res = perf_map[bucket].get(availability.upper(), {})

    # Return as a nested dict including the requested tuple-key structure
    return {
        "max_iops_rw": res.get("iops"),
        "max_throughput_rw": res.get("throughput")
    }


# --- Example Usage ---
# Using your example "db-custom-8-32768" and "REGIONAL"
# output = get_performance_metrics("db-custom-8-32768", "REGIONAL")
# print(output)
