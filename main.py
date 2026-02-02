from flask import (
    Flask,
    request,
    render_template,
    jsonify,
    Response,
    session,
    redirect,
    url_for,
    send_file,
)
from datetime import datetime, timedelta
import pandas as pd
import os
import hashlib
import queue
import json
import sys
import signal
import atexit
import glob
import re
import zipfile
import io
import threading
import logging
import time
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException
import minify_html

# Rust imports
from rustlibs import get_csv_files_from_folder

# Load environment variables from .env file
load_dotenv()

# Import notification service
try:
    from ibu_dashboard.notification_service import notification_service

    NOTIFICATIONS_ENABLED = True
    print("✅ Email notifications enabled")
except ImportError as e:
    NOTIFICATIONS_ENABLED = False
    print(f"⚠️ Email notifications disabled: {e}")

# Use local data folder
DATA_FOLDER = os.getenv("DATA_FOLDER", "Scraped_Team_Info")
# Additional folder for scraped team rankings (top 150 teams with multiple metrics)
TEAMS_POINTS_FOLDER = os.getenv("SCRAPED_TEAMS_POINTS_FOLDER", "Scraped_Teams_Points")

progress_queue = queue.Queue()
layout_height = 700
layout_width = 1000  # aspect_ratio variable removed (unused)

# --- Probation Overrides (external file) ------------------------------------
# Configure overrides file (JSON). Can be changed via env PROBATION_OVERRIDES_FILE
OVERRIDES_FILE = os.getenv(
    "PROBATION_OVERRIDES_FILE", os.path.join("config", "probation_overrides.json")
)

MEMBER_INFO_CACHE_FILE = "./cache/member_info.json"


def load_probation_overrides() -> dict:
    """Load milestone pass overrides from JSON file. Returns {} if missing/invalid.
    JSON shape: { "member_name": {"week_1": true, "month_1": false, "month_3": false } }
    """
    try:
        if os.path.exists(OVERRIDES_FILE):
            with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        return {}
    except Exception as e:
        print(f"Error loading probation overrides from {OVERRIDES_FILE}: {e}")
        return {}


def save_probation_overrides(data: dict) -> bool:
    """Persist the overrides dict atomically. Returns True on success."""
    try:
        # Ensure folder exists
        overrides_dir = os.path.dirname(OVERRIDES_FILE)
        if overrides_dir and not os.path.exists(overrides_dir):
            os.makedirs(overrides_dir, exist_ok=True)

        tmp_path = OVERRIDES_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data or {}, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, OVERRIDES_FILE)
        return True
    except Exception as e:
        print(f"Error saving probation overrides to {OVERRIDES_FILE}: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return False


def name_to_color(name):
    # Hash the name to get a consistent value
    hash_object = hashlib.md5(name.encode())
    hex_color = "#" + hash_object.hexdigest()[:6]
    return hex_color


def _hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 3:
        hex_color = "".join(c * 2 for c in hex_color)
    try:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return r, g, b
    except Exception:
        return 128, 128, 128


def blend_with(color_hex, base_rgb, alpha=0.5):
    """Blend a member color (hex) with a base RGB tuple (e.g. green or red) by alpha.
    alpha: portion of base color; (1-alpha) of member color.
    Returns hex string."""
    mr, mg, mb = _hex_to_rgb(color_hex)
    br, bg, bb = base_rgb
    r = int(mr * (1 - alpha) + br * alpha)
    g = int(mg * (1 - alpha) + bg * alpha)
    b = int(mb * (1 - alpha) + bb * alpha)
    return f"#{r:02x}{g:02x}{b:02x}"


# --- Normalization Helpers -------------------------------------------------
def normalize_member_points_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with unified 'Member' and 'Points' columns.
    Accepts any of: 'Member'/'member'/'name' for member column and
    'Points'/'points' for points column. Leaves original DF otherwise.
    """
    try:
        if df is None or df.empty:
            return df
        # Strip whitespace and work case-insensitively
        df = df.copy()
        df.columns = df.columns.str.strip()
        lower_map = {c.lower(): c for c in df.columns}
        rename_map = {}
        # Member/name
        if "member" in lower_map:
            rename_map[lower_map["member"]] = "Member"
        elif "name" in lower_map:
            rename_map[lower_map["name"]] = "Member"
        # Points
        if "points" in lower_map:
            rename_map[lower_map["points"]] = "Points"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df
    except Exception as e:
        print(f"normalize_member_points_columns error: {e}")
        return df


def get_team_points_files_from_folder():
    """Return list of team rankings CSV files (sheepit_teams_points_YYYY-MM-DD.csv) sorted ascending by date."""
    try:
        if not os.path.exists(TEAMS_POINTS_FOLDER):
            return []
        pattern = os.path.join(TEAMS_POINTS_FOLDER, "sheepit_teams_points_*.csv")
        csv_files = glob.glob(pattern)
        csv_files.sort()
        return csv_files
    except Exception as e:
        print(f"Error getting team rankings files: {e}")
        return []


# --- Team name normalization / sanitization helpers (for robust matching) ----
def _sanitize_team_name(name: str) -> str:
    """Return a simplified version of a team name for fuzzy-ish matching.
    - Lowercase
    - Strip spaces
    - Remove punctuation & emoji / non-word chars
    - Collapse multiple spaces
    This keeps alphanumerics + basic periods removed to avoid subtle differences.
    """
    try:
        if name is None:
            return ""
        # Normalize unicode form
        import unicodedata
        import re as _re

        n = unicodedata.normalize("NFKC", str(name)).lower()
        # Replace non-alphanumeric with space
        n = _re.sub(r"[^a-z0-9]+", " ", n)
        # Collapse spaces
        n = " ".join(n.split())
        return n
    except Exception:
        return str(name).strip().lower()


def get_csv_file_by_index(index: int = 0):
    # index=0 -> latest
    # index=1 -> previous

    try:
        csv_files = get_csv_files_from_folder()

        if not csv_files or index < 0 or index >= len(csv_files):
            return None, None, None

        file_path = csv_files[index]

        filename = os.path.basename(file_path)
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)

        file_timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

        if date_match:
            date_str = date_match.group(1)
        else:
            date_str = "unknown"

        return file_path, date_str, file_timestamp

    except Exception as e:
        print(f"Error getting CSV file by index: {str(e)}")
        return None, None, None


def get_latest_csv_file():
    """
    Get the latest CSV file from the local folder
    """
    try:
        csv_files = get_csv_files_from_folder()

        if not csv_files:
            return None, None, None

        latest_file = csv_files[0]

        # Extract date from filename
        filename = os.path.basename(latest_file)
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)

        if date_match:
            date_str = date_match.group(1)
            # Get file modification time as timestamp
            file_timestamp = datetime.fromtimestamp(os.path.getmtime(latest_file))
            return latest_file, date_str, file_timestamp
        else:
            return (
                latest_file,
                "unknown",
                datetime.fromtimestamp(os.path.getmtime(latest_file)),
            )

    except Exception as e:
        print(f"Error getting latest file from local folder: {str(e)}")
        return None, None, None


def find_csv_file_by_date(date_str):
    """
    Find a CSV file by date string (YYYY-MM-DD format)
    """
    try:
        target_filename = f"sheepit_team_points_{date_str}.csv"
        target_path = os.path.join(DATA_FOLDER, target_filename)

        if os.path.exists(target_path):
            return target_path

        # If exact match not found, look for any file containing the date
        csv_files = get_csv_files_from_folder()
        for file in csv_files:
            if date_str in os.path.basename(file):
                return file

        return None

    except Exception as e:
        print(f"Error finding CSV file by date {date_str}: {str(e)}")
        return None


def get_time_ago_string(file_timestamp):
    """Convert timestamp to 'X time ago' format"""
    if not file_timestamp:
        return "Recently"

    try:
        current_time = datetime.now()
        time_diff = current_time - file_timestamp

        total_seconds = int(time_diff.total_seconds())

        # Calculate time units
        if total_seconds < 60:
            return "Just now" if total_seconds < 10 else f"{total_seconds} seconds ago"

        minutes = total_seconds // 60
        if minutes < 60:
            return "1 minute ago" if minutes == 1 else f"{minutes} minutes ago"

        hours = minutes // 60
        if hours < 24:
            return "1 hour ago" if hours == 1 else f"{hours} hours ago"

        days = hours // 24
        if days < 30:
            return "1 day ago" if days == 1 else f"{days} days ago"

        months = days // 30
        if months < 12:
            return "1 month ago" if months == 1 else f"{months} months ago"

        years = months // 12
        return "1 year ago" if years == 1 else f"{years} years ago"

    except Exception as e:
        print(f"Error calculating time ago: {e}")
        return "Recently"


def get_last_day_data():
    """Get chart data showing point differences for the last day (latest file vs previous day file)"""
    try:
        # Get the latest CSV file
        latest_file_path, latest_date_str, _ = get_latest_csv_file()

        if not latest_file_path or not latest_date_str:
            return {"error": "No CSV files found for last day calculation."}

        # Calculate the previous day
        try:
            latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
            previous_date = latest_date - timedelta(days=1)
            previous_date_str = previous_date.strftime("%Y-%m-%d")
        except ValueError:
            return {"error": f"Invalid date format in latest file: {latest_date_str}"}

        # Find the CSV file for the previous day
        previous_file_path = find_csv_file_by_date(previous_date_str)

        if not previous_file_path:
            return {
                "error": f"Previous day file not found for {previous_date_str}. Cannot calculate last day difference without consecutive day data."
            }

        if not os.path.exists(latest_file_path) or not os.path.exists(
            previous_file_path
        ):
            return {"error": "Required CSV files not found for last day calculation."}

        # Use the existing standardize_range_formats function to calculate differences
        return standardize_range_formats(previous_file_path, latest_file_path)

    except Exception as e:
        return {"error": f"Error calculating last day data: {str(e)}"}


def get_chart_total():
    """Get chart data from the latest CSV file in the local folder"""
    try:
        # Get the latest file from local folder
        file_path, date_str, file_timestamp = get_latest_csv_file()

        if not file_path or not os.path.exists(file_path):
            return {"error": "No CSV files found in the Scraped_Team_Info folder."}

        # Load CSV
        df = pd.read_csv(file_path)

        # Check if file is empty
        if df.empty:
            return {"error": "Data file is empty."}

        df.columns = df.columns.str.strip()
        df = df.rename(columns={"name": "Member", "points": "Points"})

        # Check if required columns exist
        if "Member" not in df.columns or "Points" not in df.columns:
            return {"error": "Data file missing required columns (Member, Points)."}

        # Check if there's any data
        if len(df) == 0:
            return {"error": "No data available in file."}

        member = df["Member"]
        points = df["Points"]
        color = df["Member"].apply(name_to_color)
        return data_for_return(member, points, color)

    except Exception as e:
        return {"error": f"Error processing data file: {str(e)}"}


def get_last_week_range():
    """Get chart data for last week using local CSV files"""
    today = datetime.today().date()
    last_monday = today - timedelta(days=today.weekday() + 7)
    this_monday = last_monday + timedelta(days=6)
    print(last_monday, this_monday)
    end_date = this_monday.strftime("%Y-%m-%d")
    start_date = last_monday.strftime("%Y-%m-%d")

    # Find the required CSV files in local folder
    file_start = find_csv_file_by_date(start_date)
    file_end = find_csv_file_by_date(end_date)

    if not file_start:
        return {
            "error": f"Start date file not found for {start_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    if not file_end:
        return {
            "error": f"End date file not found for {end_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    return standardize_range_formats(file_start, file_end)


def get_last_month_range():
    """Get chart data for last month using local CSV files"""
    today = datetime.today().date()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    end_date = last_month_end.strftime("%Y-%m-%d")
    start_date = last_month_start.strftime("%Y-%m-%d")

    # Find the required CSV files in local folder
    file_start = find_csv_file_by_date(start_date)
    file_end = find_csv_file_by_date(end_date)

    if not file_start:
        return {
            "error": f"Start date file not found for {start_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    if not file_end:
        return {
            "error": f"End date file not found for {end_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    return standardize_range_formats(file_start, file_end)


def get_last_year_range():
    """Get chart data for last year using local CSV files"""
    today = datetime.today().date()
    last_year = today.year - 1
    last_year_start = datetime(last_year, 1, 1).date()
    last_year_end = datetime(last_year, 12, 31).date()
    end_date = last_year_end.strftime("%Y-%m-%d")
    start_date = last_year_start.strftime("%Y-%m-%d")

    # Find the required CSV files in local folder
    file_start = find_csv_file_by_date(start_date)
    file_end = find_csv_file_by_date(end_date)

    if not file_start:
        return {
            "error": f"Start date file not found for {start_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    if not file_end:
        return {
            "error": f"End date file not found for {end_date}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    return standardize_range_formats(file_start, file_end)


def get_last_90_days_range():
    """Get chart data for exactly the last 90 days using local CSV files.
    Requires an exact CSV file on both the computed start date and the end date (latest)."""
    try:
        latest_file_path, latest_date_str, _ = get_latest_csv_file()
        if not latest_file_path or not latest_date_str:
            return {"error": "No CSV files found for last 90 days calculation."}
        try:
            end_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
            start_date = end_date - timedelta(days=90)
            start_date_str = start_date.strftime("%Y-%m-%d")
        except ValueError:
            return {"error": f"Invalid date format in latest file: {latest_date_str}"}

        file_start = find_csv_file_by_date(start_date_str)
        file_end = find_csv_file_by_date(latest_date_str)
        if not file_start:
            return {
                "error": f"Start date file not found for {start_date_str}. Cannot calculate last 90 days without an exact file on the start date."
            }
        if not file_end:
            return {
                "error": f"End date file not found for {latest_date_str}. Cannot calculate last 90 days without an exact file on the end date."
            }
        data = standardize_range_formats(file_start, file_end)
        if isinstance(data, dict):
            data["date_range"] = {"start": start_date_str, "end": latest_date_str}
        return data
    except Exception as e:
        return {"error": f"Error calculating last 90 days data: {str(e)}"}


def get_last_180_days_range():
    """Get chart data for exactly the last 180 days using local CSV files.
    Requires an exact CSV file on both the computed start date and the end date (latest)."""
    try:
        latest_file_path, latest_date_str, _ = get_latest_csv_file()
        if not latest_file_path or not latest_date_str:
            return {"error": "No CSV files found for last 180 days calculation."}
        try:
            end_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
            start_date = end_date - timedelta(days=180)
            start_date_str = start_date.strftime("%Y-%m-%d")
        except ValueError:
            return {"error": f"Invalid date format in latest file: {latest_date_str}"}

        file_start = find_csv_file_by_date(start_date_str)
        file_end = find_csv_file_by_date(latest_date_str)
        if not file_start:
            return {
                "error": f"Start date file not found for {start_date_str}. Cannot calculate last 180 days without an exact file on the start date."
            }
        if not file_end:
            return {
                "error": f"End date file not found for {latest_date_str}. Cannot calculate last 180 days without an exact file on the end date."
            }
        data = standardize_range_formats(file_start, file_end)
        if isinstance(data, dict):
            data["date_range"] = {"start": start_date_str, "end": latest_date_str}
        return data
    except Exception as e:
        return {"error": f"Error calculating last 180 days data: {str(e)}"}


def get_chart_data_for_range(start_date, end_date):
    """Get chart data for a custom date range using local CSV files"""
    # Find the required CSV files in local folder
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    file_start = find_csv_file_by_date(start_date_str)
    file_end = find_csv_file_by_date(end_date_str)

    if not file_start:
        return {
            "error": f"Start date file not found for {start_date_str}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    if not file_end:
        return {
            "error": f"End date file not found for {end_date_str}. Please ensure the CSV file exists in the Scraped_Team_Info folder."
        }

    return standardize_range_formats(file_start, file_end)


def standardize_range_formats(file_start_raw, file_end_raw):
    # Load CSVs
    df_start = pd.read_csv(file_start_raw)
    df_end = pd.read_csv(file_end_raw)

    # Clean and standardize column names
    df_start.columns = df_start.columns.str.strip()
    df_end.columns = df_end.columns.str.strip()

    # Rename columns to match
    df_start = df_start.rename(columns={"name": "Member", "points": "Points"})
    df_end = df_end.rename(columns={"name": "Member", "points": "Points"})

    # Find new members in end that are not in start
    new_members = set(df_end["Member"]) - set(df_start["Member"])
    if new_members:
        # Create DataFrame for new members with 0 points at start
        new_rows = pd.DataFrame(
            {"Member": list(new_members), "Points": [0] * len(new_members)}
        )
        # Append to df_start
        df_start = pd.concat([df_start, new_rows], ignore_index=True)

    # Merge and calculate difference
    merged = pd.merge(df_end, df_start, on="Member", suffixes=("_end", "_start"))
    merged["Delta"] = merged["Points_end"] - merged["Points_start"]
    # merged = merged[merged["Delta"] > 0] # Uncomment to filter out non-positive/0 points members
    color = merged["Member"].apply(name_to_color)
    member = merged["Member"]
    points = merged["Delta"]

    # Return data in pie chart format
    return data_for_return(member, points, color)


def data_for_return(data_member, data_points, color_data):
    return {
        "data": [
            {
                "type": "pie",
                "labels": data_member.tolist(),
                "values": data_points.tolist(),
                "hole": 0.6,
                "text": get_custom_text(data_points, data_member),
                "textinfo": "text",
                "textposition": "inside",
                "hoverinfo": "label+percent+value",
                "hovertemplate": "<b>%{label}</b><br>"
                + "Points: %{value:,}<br>"
                + "Percentage: %{percent}<br>"
                + "<extra></extra>",
                "marker": {
                    "colors": color_data.tolist(),
                    "line": {"color": "rgba(255, 255, 255, 0.2)", "width": 1},
                },
                "automargin": False,
                "domain": {"x": [0, 1], "y": [0, 1]},
            }
        ],
        "layout": {
            "width": layout_width,
            "height": layout_height,
            "margin": {"t": 50, "b": 50, "l": 0, "r": 0},
            "showlegend": True,
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0)",
            "annotations": [
                {
                    "text": f"<b>Total Points</b><br><br><span style='font-size:24px; color:#e06150'>{sum(data_points):,}</span>",
                    "x": 0.5,
                    "y": 0.55,
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {
                        "size": 14,
                        "color": "white",
                        "family": "Inter, Arial, sans-serif",
                    },
                    "align": "center",
                },
                {
                    "text": f"Active Members: {len([x for x in data_points if x > 0])}",
                    "x": 0.5,
                    "y": 0.4,
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {
                        "size": 12,
                        "color": "rgba(255, 255, 255, 0.7)",
                        "family": "Inter, Arial, sans-serif",
                    },
                    "align": "center",
                },
            ],
            "font": {
                "color": "white",
                "family": "Inter, Arial, sans-serif",
                "size": 14,
            },
            "legend": {
                "title": {
                    "text": "<b style='color:#e06150; font-size:16px'>👥 Team Members</b>",
                    "font": {
                        "color": "#e06150",
                        "size": 16,
                        "family": "Inter, Arial, sans-serif",
                    },
                },
                "orientation": "v",
                "xanchor": "left",
                "x": 1,
                "y": 0,
                "bgcolor": "rgba(42, 42, 42, 0.8)",
                "bordercolor": "rgba(224, 97, 80, 0.3)",
                "borderwidth": 1,
                "font": {
                    "color": "white",
                    "size": 12,
                    "family": "Inter, Arial, sans-serif",
                },
                "itemsizing": "constant",
                "itemwidth": 50,
            },
        },
        "config": {"displaylogo": False, "displayModeBar": False, "showTips": False},
    }


def get_custom_text(values, labels):
    total = sum(values)
    result = []
    for i, v in enumerate(values):
        if v == 0 or total == 0:
            result.append("")
        else:
            percent = (v / total) * 100
            if percent >= 0.95:
                result.append(f"   {labels[i]} ({percent:.1f}%)   ")
            else:
                result.append("")
    return result


app = Flask(__name__)  # . .venv/bin/activate


@app.after_request
def minify_html_response(response):
    ct = response.headers.get("Content-Type", "")
    if not ct.startswith("text/html") or response.direct_passthrough:
        return response

    response.set_data(
        minify_html.minify(
            response.get_data(as_text=True),
            minify_js=True,
            minify_css=True,
            remove_processing_instructions=True,
            remove_bangs=True,
            keep_comments=False,
            keep_ssi_comments=False,
            allow_noncompliant_unquoted_attribute_values=True,
            allow_optimal_entities=True,
            allow_removing_spaces_between_attributes=True,
        )
    )

    response.headers["Content-Length"] = str(len(response.get_data()))
    return response


# Handle http errors (400s)
@app.errorhandler(HTTPException)
def handle_http_exception(e):
    return render_template(
        "error.html",
        code=e.code,
        name=e.name,
        description=e.description,
        error=e,
        debug=app.debug,
    ), e.code


# Configure Flask session
app.secret_key = os.getenv(
    "FLASK_SECRET_KEY", "ibu-dashboard-secret-key-change-in-production"
)

# Admin password configuration
ADMIN_PASSWORD = os.getenv(
    "ADMIN_PASSWORD", "admin123"
)  # Default password - change in .env

# --- Background Scheduler: Email -> Discord Forwarder ------------------------
# Import the worker function from the standalone script
try:
    from ibu_dashboard.email_to_discord import (
        fetch_and_forward as _email_to_discord_run_once,
    )

    _EMAIL_TO_DISCORD_AVAILABLE = True
except Exception as _e:
    print(f"[Email→Discord] Forwarder unavailable: {_e}")
    _EMAIL_TO_DISCORD_AVAILABLE = False

# Config via env (optional overrides)
EMAIL_TO_DISCORD_ENABLED = (
    os.getenv("EMAIL_TO_DISCORD_ENABLED", "true").lower() == "true"
)
EMAIL_TO_DISCORD_INTERVAL_SECONDS = int(
    os.getenv("EMAIL_TO_DISCORD_INTERVAL_SECONDS", "900")
)
EMAIL_TO_DISCORD_START_EAGER = (
    os.getenv("EMAIL_TO_DISCORD_START_EAGER", "true").lower() == "true"
)

_email_discord_stop = threading.Event()
_email_discord_thread = None


def compact_num(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return n

    sign = "-" if n < 0 else ""
    n = abs(n)

    # Use 1 decimal for compactness when needed
    if n >= 1_000_000_000_000:
        v, s = n / 1_000_000_000_000, "T"
    elif n >= 1_000_000_000:
        v, s = n / 1_000_000_000, "B"
    elif n >= 1_000_000:
        v, s = n / 1_000_000, "M"
    elif n >= 1_000:
        v, s = n / 1_000, "K"
    else:
        return f"{sign}{int(n)}"

    # drop trailing .0
    out = f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{sign}{out}{s}"


app.jinja_env.filters["compact"] = compact_num


def _email_to_discord_worker(interval_sec: int):
    logging.getLogger().setLevel(logging.INFO)
    while not _email_discord_stop.is_set():
        try:
            # Only run if all required env vars exist
            required_vars = [
                "IMAP_USER",
                "IMAP_PASS",
                "IMAP_HOST",
                "DISCORD_WEBHOOK_URL",
            ]
            if (
                _EMAIL_TO_DISCORD_AVAILABLE
                and all(os.getenv(v) for v in required_vars)
                and EMAIL_TO_DISCORD_ENABLED
            ):
                _email_to_discord_run_once()
            else:
                logging.info(
                    "[Email→Discord] Skipping worker: missing environment variables"
                )
        except Exception as e:
            logging.exception("[Email→Discord] Worker error: %s", e)
        # Sleep in 1s ticks so we can stop quickly
        for _ in range(max(1, int(interval_sec))):
            if _email_discord_stop.is_set():
                break
            time.sleep(1)


def start_email_to_discord_scheduler():
    global _email_discord_thread
    if not _EMAIL_TO_DISCORD_AVAILABLE:
        return
    if not EMAIL_TO_DISCORD_ENABLED:
        print(
            "[Email→Discord] Scheduler disabled via env (EMAIL_TO_DISCORD_ENABLED=false)"
        )
        return
    # Avoid starting twice (e.g., Flask debug reloader)
    if getattr(app, "_email_discord_started", False):
        return
    # Prevent duplicate starts across Flask reloader or multiple gunicorn workers
    if getattr(app, "debug", False) and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    # Extra process-level guard using an env-scoped flag
    if os.environ.get("EMAIL_TO_DISCORD_ALREADY_STARTED") == "1":
        return
    os.environ["EMAIL_TO_DISCORD_ALREADY_STARTED"] = "1"
    app._email_discord_started = True
    print(
        f"[Email→Discord] Starting scheduler every {EMAIL_TO_DISCORD_INTERVAL_SECONDS}s"
    )
    _email_discord_thread = threading.Thread(
        target=_email_to_discord_worker,
        args=(EMAIL_TO_DISCORD_INTERVAL_SECONDS,),
        daemon=True,
        name="EmailToDiscordWorker",
    )
    _email_discord_thread.start()


def stop_email_to_discord_scheduler():
    _email_discord_stop.set()
    try:
        if _email_discord_thread and _email_discord_thread.is_alive():
            _email_discord_thread.join(timeout=3)
    except Exception:
        pass


# Ensure background workers are stopped on process exit
atexit.register(stop_email_to_discord_scheduler)

# Eagerly start background workers at process start (guarded against duplicates)
if EMAIL_TO_DISCORD_START_EAGER:
    try:
        start_email_to_discord_scheduler()
    except Exception as _e:
        print(f"[Email→Discord] Failed to start scheduler on boot: {_e}")


def compute_simple_stats_from_latest_csv(previous_file_path=None):
    file_path, date_str, file_timestamp = get_latest_csv_file()

    if not file_path or not os.path.exists(file_path):
        return {
            "total_points": 0,
            "active_members": 0,
            "top_performers": [],
            "total_points_gain": 0,
            "active_members_gain": 0,
        }

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"name": "Member", "points": "Points"})

    if "Points" not in df.columns or "Member" not in df.columns:
        return {
            "total_points": 0,
            "active_members": 0,
            "top_performers": [],
            "total_points_gain": 0,
            "active_members_gain": 0,
        }

    df["Points"] = pd.to_numeric(df["Points"], errors="coerce").fillna(0)

    total_points = int(df["Points"].sum())
    active_members = int((df["Points"] > 0).sum())

    top_df = df[df["Points"] > 0].nlargest(10, "Points")
    prev_points_map = {}
    if previous_file_path and os.path.exists(previous_file_path):
        prev = pd.read_csv(previous_file_path)
        prev.columns = prev.columns.str.strip()
        prev = prev.rename(columns={"name": "Member", "points": "Points"})
        prev["Points"] = pd.to_numeric(prev["Points"], errors="coerce").fillna(0)
        prev_points_map = dict(
            zip(prev["Member"].astype(str).str.strip(), prev["Points"])
        )

    top_performers = [
        {
            "name": row["Member"],
            "points": int(row["Points"]),
            "gain": int(
                row["Points"] - prev_points_map.get(str(row["Member"]).strip(), 0)
            ),
        }
        for _, row in top_df.iterrows()
    ]

    total_points_gain = 0
    active_members_gain = 0

    if previous_file_path and os.path.exists(previous_file_path):
        prev = pd.read_csv(previous_file_path)
        prev.columns = prev.columns.str.strip()
        prev = prev.rename(columns={"name": "Member", "points": "Points"})
        prev["Points"] = pd.to_numeric(prev["Points"], errors="coerce").fillna(0)

        total_points_gain = total_points - int(prev["Points"].sum())
        active_members_gain = int((df["Points"] > 0).sum()) - int(
            (prev["Points"] > 0).sum()
        )

    return {
        "total_points": total_points,
        "active_members": active_members,
        "top_performers": top_performers,
        "total_points_gain": total_points_gain,
        "active_members_gain": active_members_gain,
    }


@app.route("/")
def index():
    file_path, date_str, file_timestamp = get_csv_file_by_index(0)
    prev_file_path, _, _ = get_csv_file_by_index(1)

    if not file_path or not date_str:
        latest_file = "No data"
        latest_date = "No data"
        time_ago = "No recent data"
    else:
        try:
            latest_file = os.path.abspath(file_path)
            latest_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
            time_ago = get_time_ago_string(file_timestamp)
        except Exception:
            latest_file = file_path
            latest_date = date_str
            time_ago = "Recently"

    stats = compute_simple_stats_from_latest_csv(previous_file_path=prev_file_path)

    return render_template(
        "index.html",
        saved_file=latest_file,
        latest_date=latest_date,
        time_ago=time_ago,
        stats=stats,
    )


# Helper for switching plotly -> chartjs
def to_chartjs_payload(plotly_like):
    if not isinstance(plotly_like, dict):
        return {"error": "Invalid chart data format."}

    if "error" in plotly_like:
        return plotly_like

    trace = None
    data = plotly_like.get("data")
    if isinstance(data, list) and data:
        trace = data[0]

    if not isinstance(trace, dict):
        return {"error": "Invalid chart trace."}

    labels = trace.get("labels") or []
    values = trace.get("values") or []

    if (
        not isinstance(labels, list)
        or not isinstance(values, list)
        or len(labels) != len(values)
    ):
        return {"error": "Invalid labels/values."}

    marker = trace.get("marker") or {}
    colors = marker.get("colors") or []

    # JSON-safe numerics
    clean_values = []
    for v in values:
        try:
            clean_values.append(int(v))
        except Exception:
            clean_values.append(0)

    total = sum(clean_values)
    active = sum(1 for v in clean_values if v > 0)

    payload = {
        "labels": [str(x) for x in labels],
        "values": clean_values,
        "colors": [str(c) for c in colors] if isinstance(colors, list) else [],
        "meta": {
            "total": total,
            "active_members": active,
            "hole": trace.get("hole", 0.6),
        },
    }

    return payload


@app.route("/get_chart_data")
def get_chart_data():
    chart_type = request.args.get("type")
    start = request.args.get("start")
    end = request.args.get("end")

    def ok(data):
        if not data or ("error" in data):
            return None
        return jsonify(to_chartjs_payload(data))

    if chart_type == "last_day":
        data = get_last_day_data()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the last day."}),
                400,
            )
        )

    elif chart_type == "last_week":
        data = get_last_week_range()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "last_month":
        data = get_last_month_range()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "last_year":
        data = get_last_year_range()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "last_90_days":
        data = get_last_90_days_range()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "last_180_days":
        data = get_last_180_days_range()
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "custom" and start and end:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
        data = get_chart_data_for_range(start_date, end_date)
        out = ok(data)
        return (
            out
            if out
            else (
                jsonify({"error": "Not enough data available for the selected range."}),
                400,
            )
        )

    elif chart_type == "total":
        data = get_chart_total()
        out = ok(data)
        return out if out else (jsonify({"error": "Not enough data available."}), 400)

    return jsonify({"error": "Invalid request"}), 400


@app.route("/visualization")
def visualization():
    # Get date and time info from the latest file for display
    latest_date_fmt = "No data"
    time_ago = "No recent data"
    try:
        # Try to get the latest file info for date formatting
        file_path, date_str, file_timestamp = get_latest_csv_file()
        if date_str:
            latest_date_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime(
                "%B %d, %Y"
            )
            time_ago = get_time_ago_string(file_timestamp)
    except Exception:
        latest_date_fmt = "Recent data"
        time_ago = "Recently"

    # Don't pre-load chart data - let the frontend handle it via AJAX
    return render_template(
        "team-info.html",
        labels=[],
        values=[],
        colors=[],
        latest_date=latest_date_fmt,
        time_ago=time_ago,
    )


def flask_progress_callback(msg, progress_percent, latest_date=None, saved_file=None):
    payload = {"msg": msg, "percent": progress_percent}
    if latest_date is not None:
        payload["latest_date"] = latest_date
    if saved_file is not None:
        payload["saved_file"] = saved_file
    progress_queue.put(json.dumps(payload))


@app.route("/progress_stream")
def progress_stream():
    def event_stream():
        while True:
            message = progress_queue.get()
            yield f"data: {message}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/local_status")
def local_status():
    """Check local file status and list available CSV files"""
    try:
        csv_files = get_csv_files_from_folder()

        if csv_files:
            # Extract dates from filenames
            available_dates = []
            for file in csv_files:
                filename = os.path.basename(file)
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                if date_match:
                    available_dates.append(date_match.group(1))

            available_dates.sort(reverse=True)  # Most recent first

            return jsonify(
                {
                    "success": True,
                    "connection_status": "Local files available",
                    "local_files_count": len(csv_files),
                    "csv_files": [
                        os.path.basename(f) for f in csv_files[:10]
                    ],  # Show first 10 files
                    "available_dates": available_dates[:10],  # Show first 10 dates
                    "latest_date": available_dates[0] if available_dates else None,
                    "data_folder": DATA_FOLDER,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": f"No CSV files found in {DATA_FOLDER} folder",
                }
            )

    except Exception as e:
        return jsonify(
            {"success": False, "error": f"Error checking local files: {str(e)}"}
        )


@app.route("/get_available_dates")
def get_available_dates():
    """Get available dates from CSV files for datepicker highlighting"""
    try:
        csv_files = get_csv_files_from_folder()

        if csv_files:
            # Extract dates from filenames
            available_dates = []
            for file in csv_files:
                filename = os.path.basename(file)
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                if date_match:
                    available_dates.append(date_match.group(1))

            available_dates.sort(reverse=True)  # Most recent first

            return jsonify(
                {
                    "success": True,
                    "available_dates": available_dates,
                    "count": len(available_dates),
                    "latest_date": available_dates[0] if available_dates else None,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "available_dates": [],
                    "error": f"No CSV files found in {DATA_FOLDER} folder",
                }
            )

    except Exception as e:
        return jsonify(
            {
                "success": False,
                "available_dates": [],
                "error": f"Error getting available dates: {str(e)}",
            }
        )


@app.route("/refresh_files")
def refresh_files():
    """Refresh the list of available local CSV files"""
    try:
        csv_files = get_csv_files_from_folder()

        if csv_files:
            # Extract dates and file info
            file_info = []
            for file_path in csv_files:
                filename = os.path.basename(file_path)
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                file_size = os.path.getsize(file_path)
                file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))

                file_info.append(
                    {
                        "filename": filename,
                        "date": date_match.group(1) if date_match else "unknown",
                        "size_bytes": file_size,
                        "modified": file_modified.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

            return jsonify(
                {
                    "success": True,
                    "total_files": len(csv_files),
                    "files": file_info,
                    "message": f"Found {len(csv_files)} CSV files in local folder",
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "total_files": 0,
                    "files": [],
                    "message": f"No CSV files found in {DATA_FOLDER} folder",
                }
            )

    except Exception as e:
        return jsonify({"success": False, "error": f"Error refreshing files: {str(e)}"})


@app.route("/list_files")
def list_files():
    """List all CSV files in the local folder"""
    try:
        if os.path.exists(DATA_FOLDER):
            csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
            file_count = len(csv_files)

            # Sort files by name (which includes date)
            csv_files.sort(reverse=True)

            return jsonify(
                {
                    "success": True,
                    "message": f"Found {file_count} CSV files in local folder.",
                    "files": csv_files,
                    "folder_path": os.path.abspath(DATA_FOLDER),
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "message": f"Data folder '{DATA_FOLDER}' does not exist.",
                    "files": [],
                    "folder_path": os.path.abspath(DATA_FOLDER),
                }
            )
    except Exception as e:
        return jsonify({"success": False, "error": f"Error listing files: {str(e)}"})


def cleanup_on_exit():
    """Clean up function for graceful shutdown"""
    try:
        print("\nShutting down IBU Dashboard...")
        print("No temporary files to clean up (using local folder)")
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\nShutting down gracefully...")
    cleanup_on_exit()
    sys.exit(0)


# Register cleanup functions
atexit.register(cleanup_on_exit)
signal.signal(signal.SIGINT, signal_handler)


@app.route("/get_simple_stats")
def get_simple_stats():
    """Get basic stats without full chart processing"""
    try:
        file_path, date_str, file_timestamp = get_latest_csv_file()
        if not file_path or not os.path.exists(file_path):
            return jsonify(
                {
                    "error": "No data file available",
                    "stats": {
                        "total_points": 0,
                        "active_members": 0,
                        "top_performers": [],
                    },
                }
            )

        # Load CSV directly
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        df = df.rename(columns={"name": "Member", "points": "Points"})

        # Calculate basic stats
        total_points = int(df["Points"].sum())
        active_members = len(df[df["Points"] > 0])

        # Get top 10 performers
        top_performers = df[df["Points"] > 0].nlargest(10, "Points").to_dict("records")

        return jsonify(
            {
                "success": True,
                "stats": {
                    "total_points": total_points,
                    "active_members": active_members,
                    "top_performers": [
                        {
                            "name": performer["Member"],
                            "points": int(performer["Points"]),
                        }
                        for performer in top_performers
                    ],
                },
            }
        )

    except Exception as e:
        print(f"Error getting simple stats: {str(e)}")
        return jsonify(
            {
                "error": f"Error loading stats: {str(e)}",
                "stats": {"total_points": 0, "active_members": 0, "top_performers": []},
            }
        )


@app.route("/get_latest_file_info")
def get_latest_file_info():
    """Get information about the latest CSV file for real-time updates"""
    try:
        file_path, date_str, file_timestamp = get_latest_csv_file()

        if not file_path or not date_str:
            return jsonify(
                {
                    "success": False,
                    "message": "No CSV files found",
                    "latest_file": "No data",
                    "latest_date": "No data",
                    "time_ago": "No recent data",
                    "file_count": 0,
                }
            )

        # Format the information
        try:
            latest_file = os.path.basename(file_path)
            latest_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
            time_ago = get_time_ago_string(file_timestamp)
        except Exception:
            latest_file = os.path.basename(file_path) if file_path else "Unknown"
            latest_date = date_str
            time_ago = "Recently"

        # Get total file count
        csv_files = get_csv_files_from_folder()

        return jsonify(
            {
                "success": True,
                "latest_file": latest_file,
                "latest_date": latest_date,
                "time_ago": time_ago,
                "file_count": len(csv_files),
                "file_path": file_path,
                "date_str": date_str,
            }
        )

    except Exception as e:
        print(f"Error getting latest file info: {str(e)}")
        return jsonify(
            {
                "success": False,
                "error": str(e),
                "message": "Error retrieving file information",
            }
        )


@app.route("/get_updates")
def get_updates():
    try:
        changelog_file = os.path.join("CHANGELOG.md")

        if not os.path.exists(changelog_file):
            return jsonify(
                {"success": False, "error": "Changelog file not found", "updates": []}
            )

        with open(changelog_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Matches:
        # ## [1.4.3] - 2025-08-31
        # ## [Unreleased]
        header_re = re.compile(
            r"^\s*##\s*\[(?P<version>[^\]]+)\]\s*(?:-\s*(?P<date>\d{4}-\d{2}-\d{2}))?\s*$"
        )
        section_re = re.compile(r"^\s*###\s+(?P<section>.+?)\s*$")
        bullet_re = re.compile(r"^\s*[-*]\s+(?P<item>.+?)\s*$")
        hr_re = re.compile(r"^\s*(---|___|\*\*\*)\s*$")

        updates = []
        current_update = None
        current_section = None

        for raw in lines:
            line = raw.rstrip("\n")

            # Skip top-level title/comments/empty lines and horizontal rules
            if not line.strip() or line.lstrip().startswith("# ") or hr_re.match(line):
                continue

            # New version block?
            m = header_re.match(line)
            if m:
                # Close previous block
                if current_update is not None:
                    updates.append(current_update)

                version = m.group("version").strip()
                date = (m.group("date") or "").strip()

                current_update = {
                    "version": version
                    if not version.lower().startswith("v")
                    else version,
                    "date": date,  # Keep a Changelog typically uses YYYY-MM-DD
                    "title": "",  # Optional; you can set this if you add a line in the md
                    "features": [],
                    "is_current": False,
                }
                current_section = None
                continue

            # Ignore anything before the first "## [x.y.z]" header
            if current_update is None:
                continue

            # Section header? (Added/Changed/Fixed/Improved/etc.)
            m = section_re.match(line)
            if m:
                current_section = m.group("section").strip()
                continue

            # Bullet item?
            m = bullet_re.match(line)
            if m:
                item = m.group("item").strip()

                # Flatten as strings like "Added: foo" so your frontend can stay simple.
                if current_section:
                    current_update["features"].append(f"{current_section}: {item}")
                else:
                    current_update["features"].append(item)
                continue

            # Optional: use the first non-empty, non-heading, non-bullet line after the version header as a title
            # (If you add short summaries later, this will pick them up.)
            if not current_update["title"]:
                # Avoid treating "The format is..." boilerplate as a title; keep it simple.
                if not line.lstrip().startswith(("#", "##", "###", "-", "*")):
                    current_update["title"] = line.strip()

        # Append the final block
        if current_update is not None:
            updates.append(current_update)

        # Mark the most recent version as current (skip "Unreleased" if present and empty)
        # Assumes changelog is ordered newest -> oldest, which is standard.
        for u in updates:
            u["is_current"] = False

        # Prefer first real version (not "Unreleased") as current
        current_idx = None
        for i, u in enumerate(updates):
            if u["version"].lower() != "unreleased":
                current_idx = i
                break
        if current_idx is not None:
            updates[current_idx]["is_current"] = True

        return jsonify({"success": True, "updates": updates})

    except Exception as e:
        print(f"Error reading changelog file: {str(e)}")
        return jsonify({"success": False, "error": str(e), "updates": []})


def parse_joined_date(joined_date_str):
    """Parse the joined date string to datetime object"""
    try:
        # Handle format like "December 19th, 2023"
        # Remove ordinal suffixes (st, nd, rd, th)
        cleaned_date = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", joined_date_str)
        return datetime.strptime(cleaned_date, "%B %d, %Y")
    except Exception as e:
        print(f"Error parsing date '{joined_date_str}': {e}")
        return None


def get_member_probation_status():
    """Calculate probation status for all members"""
    try:
        overrides = load_probation_overrides()
        # Get all CSV files to track progress over time
        csv_files = get_csv_files_from_folder()
        if not csv_files:
            return {"error": "No CSV files found"}

        # Get current date for calculations
        current_date = datetime.now()

        # Load the latest CSV to get current member list
        latest_file = csv_files[0]
        latest_df = pd.read_csv(latest_file)

        # Clean column names first
        latest_df.columns = latest_df.columns.str.strip()

        # Store original column names for debugging
        original_columns = list(latest_df.columns)
        print(f"Original columns in latest file: {original_columns}")

        # Apply standard renames but preserve other columns
        column_renames = {}
        if "name" in latest_df.columns:
            column_renames["name"] = "Member"
        if "points" in latest_df.columns:
            column_renames["points"] = "Points"

        if column_renames:
            latest_df = latest_df.rename(columns=column_renames)

        print(f"Columns after rename: {list(latest_df.columns)}")

        # Verify required columns exist
        if "Member" not in latest_df.columns or "Points" not in latest_df.columns:
            return {
                "error": f"Required columns (Member, Points) missing. Found columns: {list(latest_df.columns)}"
            }

        if "Joined Date" not in latest_df.columns:
            return {
                "error": f"Joined Date column missing. Found columns: {list(latest_df.columns)}. Please ensure your CSV files contain member join date information."
            }

        members_status = []

        for _, member_row in latest_df.iterrows():
            try:
                member_name = member_row["Member"]
                joined_date_str = str(member_row["Joined Date"]).strip('"')
                current_points = int(member_row["Points"])

                # Parse joined date
                joined_date = parse_joined_date(joined_date_str)
                if not joined_date:
                    continue

                # Calculate time since joining
                days_since_joined = (current_date - joined_date).days

                # Define probation milestones
                week_1_target = 250000  # 250k points
                month_1_target = 1000000  # 1M points
                month_3_target = 3000000  # 3M points

                # Calculate milestone dates
                week_1_date = joined_date + timedelta(days=7)
                month_1_date = joined_date + timedelta(days=30)
                month_3_date = joined_date + timedelta(days=90)

                # Track points at each milestone - use None to indicate no data found
                week_1_points = None
                month_1_points = None
                month_3_points = current_points  # Current total

                # Go through historical data to find points at milestone dates
                for csv_file in reversed(csv_files):  # Start from oldest
                    try:
                        # Extract date from filename
                        filename = os.path.basename(csv_file)
                        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                        if not date_match:
                            continue

                        file_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")

                        # Skip files before the member joined
                        if file_date < joined_date:
                            continue

                        # Load CSV and find member
                        df = pd.read_csv(csv_file)

                        # Clean column names first
                        df.columns = df.columns.str.strip()

                        # Apply standard renames but preserve other columns
                        column_renames = {}
                        if "name" in df.columns:
                            column_renames["name"] = "Member"
                        if "points" in df.columns:
                            column_renames["points"] = "Points"

                        if column_renames:
                            df = df.rename(columns=column_renames)

                        # Skip if essential columns are missing
                        if "Member" not in df.columns or "Points" not in df.columns:
                            continue

                        # Find member data
                        member_data = df[df["Member"] == member_name]

                        if member_data.empty:
                            continue

                        points_at_date = int(member_data.iloc[0]["Points"])

                        # Record points at milestone dates (find closest date after milestone)
                        if file_date >= week_1_date and week_1_points is None:
                            week_1_points = points_at_date
                        if file_date >= month_1_date and month_1_points is None:
                            month_1_points = points_at_date

                    except Exception:
                        continue

                # Calculate remaining points needed (always show actual remaining, even after deadline)
                week_1_remaining = max(0, week_1_target - current_points)
                month_1_remaining = max(0, month_1_target - current_points)
                month_3_remaining = max(0, month_3_target - current_points)

                # Enhanced logic: check if milestone is passed
                # A milestone is passed if:
                # 1. The deadline has passed AND they had enough points at the deadline (historical data), OR
                # 2. They currently have enough points (early achievement), OR
                # 3. No historical data available but current points show achievement
                # A milestone is failed ONLY if we have historical data showing they didn't meet the target
                week_1_passed = None
                if current_date >= week_1_date:
                    # Deadline has passed
                    if week_1_points is not None:
                        # We have historical data - check if they met the target
                        week_1_passed = week_1_points >= week_1_target
                    else:
                        # No historical data - can't determine failure, check current achievement
                        week_1_passed = (
                            current_points >= week_1_target
                            if current_points >= week_1_target
                            else None
                        )
                else:
                    # Deadline hasn't passed - check if they already achieved it
                    week_1_passed = (
                        current_points >= week_1_target
                        if current_points >= week_1_target
                        else None
                    )

                month_1_passed = None
                if current_date >= month_1_date:
                    # Deadline has passed
                    if month_1_points is not None:
                        # We have historical data - check if they met the target
                        month_1_passed = month_1_points >= month_1_target
                    else:
                        # No historical data - can't determine failure, check current achievement
                        month_1_passed = (
                            current_points >= month_1_target
                            if current_points >= month_1_target
                            else None
                        )
                else:
                    # Deadline hasn't passed - check if they already achieved it
                    month_1_passed = (
                        current_points >= month_1_target
                        if current_points >= month_1_target
                        else None
                    )

                month_3_passed = None
                if current_date >= month_3_date:
                    # Deadline has passed - always use current points as we have that data
                    month_3_passed = month_3_points >= month_3_target
                else:
                    # Deadline hasn't passed - check if they already achieved it
                    month_3_passed = (
                        current_points >= month_3_target
                        if current_points >= month_3_target
                        else None
                    )

                # Apply admin overrides (tri-state: None, True, False)
                override = (
                    overrides.get(member_name, {})
                    if isinstance(overrides, dict)
                    else {}
                )
                if "week_1" in override:
                    if override.get("week_1") is True:
                        week_1_passed = True
                    elif override.get("week_1") is False:
                        week_1_passed = False
                if "month_1" in override:
                    if override.get("month_1") is True:
                        month_1_passed = True
                    elif override.get("month_1") is False:
                        month_1_passed = False
                if "month_3" in override:
                    if override.get("month_3") is True:
                        month_3_passed = True
                    elif override.get("month_3") is False:
                        month_3_passed = False

                # Determine overall probation status
                probation_status = "in_progress"

                # Check if they completed all probation (passed all 3 milestones)
                if week_1_passed and month_1_passed and month_3_passed:
                    probation_status = "passed"
                # Check if they failed any milestone (only if deadline has passed AND they failed)
                elif current_date >= month_3_date and not month_3_passed:
                    probation_status = "failed"
                elif current_date >= month_1_date and not month_1_passed:
                    probation_status = "failed"
                elif current_date >= week_1_date and not week_1_passed:
                    probation_status = "failed"

                # Post-probation compliance tracking (only for members who passed probation)
                post_probation_status = None
                post_probation_periods = []

                if probation_status == "passed":
                    # Calculate post-probation periods (90-day intervals starting after probation ends)
                    probation_end_date = month_3_date  # Probation ends after 3 months

                    # Check if enough time has passed to start post-probation tracking
                    if current_date >= probation_end_date:
                        # Calculate all 90-day periods since probation ended
                        period_start = probation_end_date
                        period_number = 1

                        # Process all periods (completed and current active period)
                        # Process all periods (completed and current active period)
                        while period_start <= current_date:
                            period_end = period_start + timedelta(days=90)
                            is_current_period = (
                                current_date < period_end
                            )  # True if this is the ongoing period

                            # Find points at start and end of this period
                            points_at_start = 0
                            points_at_end = 0

                            # Look through historical data for points at period boundaries
                            # We need EXACT dates for both boundaries, not closest approximations
                            period_start_found = False
                            period_end_found = False

                            # Debug: print period info for current calculations
                            print(
                                f"Processing period {period_number} for {member_name}: {period_start.date()} to {period_end.date()}, current_period: {is_current_period}"
                            )

                            for (
                                csv_file
                            ) in csv_files:  # Check all files, not just reversed
                                try:
                                    filename = os.path.basename(csv_file)
                                    date_match = re.search(
                                        r"(\d{4}-\d{2}-\d{2})", filename
                                    )
                                    if not date_match:
                                        continue

                                    file_date = datetime.strptime(
                                        date_match.group(1), "%Y-%m-%d"
                                    )

                                    # Check for EXACT match with period start date
                                    if (
                                        file_date.date() == period_start.date()
                                        and not period_start_found
                                    ):
                                        # Load CSV and find member for period start
                                        df = pd.read_csv(csv_file)
                                        df.columns = df.columns.str.strip()

                                        column_renames = {}
                                        if "name" in df.columns:
                                            column_renames["name"] = "Member"
                                        if "points" in df.columns:
                                            column_renames["points"] = "Points"

                                        if column_renames:
                                            df = df.rename(columns=column_renames)

                                        if (
                                            "Member" not in df.columns
                                            or "Points" not in df.columns
                                        ):
                                            continue

                                        member_data = df[df["Member"] == member_name]
                                        if not member_data.empty:
                                            points_at_start = int(
                                                member_data.iloc[0]["Points"]
                                            )
                                            period_start_found = True
                                            print(
                                                f"Found period start data: {points_at_start} points on {file_date.date()}"
                                            )

                                    # For current period, use current points instead of end date
                                    if is_current_period:
                                        # For ongoing period, use the latest available CSV file for current points
                                        # Since current_date might not have a CSV, use the latest file
                                        if (
                                            csv_file == csv_files[0]
                                        ):  # This is the latest/most recent file
                                            df = pd.read_csv(csv_file)
                                            df.columns = df.columns.str.strip()

                                            column_renames = {}
                                            if "name" in df.columns:
                                                column_renames["name"] = "Member"
                                            if "points" in df.columns:
                                                column_renames["points"] = "Points"

                                            if column_renames:
                                                df = df.rename(columns=column_renames)

                                            if (
                                                "Member" not in df.columns
                                                or "Points" not in df.columns
                                            ):
                                                continue

                                            member_data = df[
                                                df["Member"] == member_name
                                            ]
                                            if not member_data.empty:
                                                points_at_end = int(
                                                    member_data.iloc[0]["Points"]
                                                )
                                                period_end_found = True
                                                print(
                                                    f"Found current period end data: {points_at_end} points on {file_date.date()}"
                                                )
                                    else:
                                        # Check for EXACT match with period end date (completed periods only)
                                        if (
                                            file_date.date() == period_end.date()
                                            and not period_end_found
                                        ):
                                            # Load CSV and find member for period end
                                            df = pd.read_csv(csv_file)
                                            df.columns = df.columns.str.strip()

                                            column_renames = {}
                                            if "name" in df.columns:
                                                column_renames["name"] = "Member"
                                            if "points" in df.columns:
                                                column_renames["points"] = "Points"

                                            if column_renames:
                                                df = df.rename(columns=column_renames)

                                            if (
                                                "Member" not in df.columns
                                                or "Points" not in df.columns
                                            ):
                                                continue

                                            member_data = df[
                                                df["Member"] == member_name
                                            ]
                                            if not member_data.empty:
                                                points_at_end = int(
                                                    member_data.iloc[0]["Points"]
                                                )
                                                period_end_found = True
                                                print(
                                                    f"Found period end data: {points_at_end} points on {file_date.date()}"
                                                )

                                    # Stop searching if we found both boundary points
                                    if period_start_found and (
                                        period_end_found or is_current_period
                                    ):
                                        break

                                except Exception:
                                    continue

                            # Calculate points earned in this period - ONLY if we have EXACT boundary data
                            points_earned = 0
                            target_points = 3000000  # 3M points per 90-day period

                            # Determine if this period was successful
                            # We REQUIRE exact data for BOTH boundaries to make any determination
                            period_status = "insufficient_data"
                            if (
                                period_start_found
                                and period_end_found
                                and points_at_start >= 0
                                and points_at_end >= 0
                            ):
                                # Ensure we calculate period points correctly
                                points_earned = max(0, points_at_end - points_at_start)

                                # Debug output
                                print(
                                    f"Period calculation for {member_name}: start={points_at_start}, end={points_at_end}, earned={points_earned}"
                                )

                                if is_current_period:
                                    # For current period, use time-based risk assessment (accounts for burst earning patterns)
                                    days_elapsed = max(
                                        1, (current_date - period_start).days
                                    )  # Ensure at least 1 day

                                    if days_elapsed > 0 and days_elapsed <= 90:
                                        if points_earned >= target_points:
                                            period_status = (
                                                "compliant"  # Already achieved target
                                            )
                                        elif (
                                            days_elapsed >= 85
                                            and points_earned < target_points
                                        ):
                                            period_status = "at_risk"  # Close to deadline without target
                                        else:
                                            period_status = "on_track"  # Still have time for burst activity
                                else:
                                    # For completed periods, simple check
                                    period_status = (
                                        "compliant"
                                        if points_earned >= target_points
                                        else "non_compliant"
                                    )

                            # Store the period info with clear data availability indicators
                            period_info = {
                                "period_number": period_number,
                                "start_date": period_start.strftime("%Y-%m-%d"),
                                "end_date": period_end.strftime("%Y-%m-%d"),
                                "points_at_start": points_at_start
                                if period_start_found
                                else None,
                                "points_at_end": points_at_end
                                if period_end_found
                                else None,
                                "points_earned": points_earned
                                if period_start_found and period_end_found
                                else None,
                                "target_points": target_points,
                                "status": period_status,
                                "start_date_found": period_start_found,
                                "end_date_found": period_end_found,
                                "is_current_period": is_current_period,
                            }

                            # Add projection data for current period
                            if (
                                is_current_period
                                and period_start_found
                                and period_end_found
                                and period_status != "insufficient_data"
                            ):
                                days_elapsed = max(
                                    1, (current_date - period_start).days
                                )  # Ensure at least 1 day
                                days_remaining = max(0, 90 - days_elapsed)

                                # Additional validation
                                if (
                                    days_elapsed > 0
                                    and days_elapsed <= 90
                                    and points_earned >= 0
                                ):
                                    daily_rate = points_earned / days_elapsed
                                    projected_total = daily_rate * 90
                                    remaining_needed = max(
                                        0, target_points - points_earned
                                    )
                                    daily_needed = (
                                        remaining_needed / max(1, days_remaining)
                                        if days_remaining > 0
                                        else 0
                                    )

                                    period_info.update(
                                        {
                                            "days_elapsed": days_elapsed,
                                            "days_remaining": days_remaining,
                                            "daily_rate": daily_rate,
                                            "projected_total": projected_total,
                                            "remaining_needed": remaining_needed,
                                            "daily_needed": daily_needed,
                                        }
                                    )

                            post_probation_periods.append(period_info)

                            # Move to next period (but break if current period is ongoing)
                            if is_current_period:
                                break
                            period_start = period_end
                            period_number += 1

                        # Determine overall post-probation status and limit to 3 most recent periods
                        if post_probation_periods:
                            # Keep only the 3 most recent periods (latest periods have highest period_number)
                            post_probation_periods = post_probation_periods[-3:]

                            # Check if we have sufficient data for evaluation
                            periods_with_data = [
                                p
                                for p in post_probation_periods
                                if p["status"] != "insufficient_data"
                            ]

                            # Separate current period from completed periods for status determination
                            current_periods = [
                                p
                                for p in periods_with_data
                                if p.get("is_current_period", False)
                            ]
                            completed_periods = [
                                p
                                for p in periods_with_data
                                if not p.get("is_current_period", False)
                            ]

                            if len(periods_with_data) == 0:
                                # No periods have sufficient data
                                post_probation_status = "insufficient_data"
                            else:
                                # Check for any non-compliant completed periods
                                non_compliant_completed = [
                                    p
                                    for p in completed_periods
                                    if p["status"] == "non_compliant"
                                ]

                                if non_compliant_completed:
                                    post_probation_status = "non_compliant"
                                elif current_periods:
                                    # Use current period status if no completed non-compliant periods
                                    current_status = current_periods[0][
                                        "status"
                                    ]  # Latest current period
                                    if current_status == "compliant":
                                        post_probation_status = "compliant"
                                    elif current_status == "on_track":
                                        post_probation_status = "on_track"
                                    elif current_status == "at_risk":
                                        post_probation_status = "at_risk"
                                    else:
                                        post_probation_status = "in_progress"
                                elif completed_periods:
                                    # Only completed periods, all compliant
                                    post_probation_status = "compliant"
                                else:
                                    post_probation_status = "insufficient_data"
                        else:
                            post_probation_status = "insufficient_data"
                    else:
                        post_probation_status = (
                            "too_early"  # Not enough time passed since probation ended
                        )

                member_status = {
                    "name": member_name,
                    "joined_date": joined_date_str,
                    "joined_date_parsed": joined_date.strftime("%Y-%m-%d"),
                    "days_since_joined": days_since_joined,
                    "current_points": current_points,
                    "probation_status": probation_status,
                    "post_probation_status": post_probation_status,
                    "post_probation_periods": post_probation_periods,
                    "overrides": {
                        "week_1": (
                            overrides.get(member_name, {}).get("week_1")
                            if "week_1" in overrides.get(member_name, {})
                            else None
                        ),
                        "month_1": (
                            overrides.get(member_name, {}).get("month_1")
                            if "month_1" in overrides.get(member_name, {})
                            else None
                        ),
                        "month_3": (
                            overrides.get(member_name, {}).get("month_3")
                            if "month_3" in overrides.get(member_name, {})
                            else None
                        ),
                    },
                    "milestones": {
                        "week_1": {
                            "target": week_1_target,
                            "points_at_deadline": week_1_points,
                            "has_historical_data": week_1_points is not None,
                            "passed": week_1_passed,
                            "deadline": week_1_date.strftime("%Y-%m-%d"),
                            "remaining_points": week_1_remaining,
                            "days_left": max(0, (week_1_date - current_date).days)
                            if current_date < week_1_date
                            else 0,
                        },
                        "month_1": {
                            "target": month_1_target,
                            "points_at_deadline": month_1_points,
                            "has_historical_data": month_1_points is not None,
                            "passed": month_1_passed,
                            "deadline": month_1_date.strftime("%Y-%m-%d"),
                            "remaining_points": month_1_remaining,
                            "days_left": max(0, (month_1_date - current_date).days)
                            if current_date < month_1_date
                            else 0,
                        },
                        "month_3": {
                            "target": month_3_target,
                            "points_at_deadline": month_3_points,
                            "has_historical_data": True,  # Always true since we use current points
                            "passed": month_3_passed,
                            "deadline": month_3_date.strftime("%Y-%m-%d"),
                            "remaining_points": month_3_remaining,
                            "days_left": max(0, (month_3_date - current_date).days)
                            if current_date < month_3_date
                            else 0,
                        },
                    },
                }

                members_status.append(member_status)

            except Exception:
                continue

        # Sort by probation status priority and days since joined
        status_priority = {"failed": 0, "in_progress": 1, "completed": 2}
        members_status.sort(
            key=lambda x: (
                status_priority.get(x["probation_status"], 1),
                x["days_since_joined"],
            )
        )

        return {"success": True, "members": members_status}

    except Exception as e:
        print(f"Error calculating probation status: {str(e)}")
        return {"error": str(e)}


@app.route("/member_info")
def member_info():
    """Member info page with probation tracking"""
    try:
        file_path, date_str, file_timestamp = get_latest_csv_file()

        if file_path:
            latest_date = (
                datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
                if date_str
                else "Unknown"
            )
            time_ago = get_time_ago_string(file_timestamp)
        else:
            latest_date = "No data"
            time_ago = "Unknown"

        probation_data = check_probation_cache()
        members = probation_data.get("members", []) if probation_data else []

        return render_template(
            "member-info.html",
            latest_date=latest_date,
            time_ago=time_ago,
            members=members,
        )

    except Exception as e:
        print(f"Error in member_info route: {str(e)}")
        return render_template(
            "member-info.html",
            latest_date="Error",
            time_ago="Error",
            members=[],
            error=str(e),
        )


@app.route("/get_probation_data")
def get_probation_data():
    """API endpoint to get probation status data"""
    try:
        probation_data = check_probation_cache()

        # Check for probation failures and send notifications
        if NOTIFICATIONS_ENABLED and probation_data and "members" in probation_data:
            try:
                # Get the current CSV file for notification tracking
                file_path, _, _ = get_latest_csv_file()
                threading.Thread(
                    target=notification_service.check_and_notify_failures,
                    args=(probation_data["members"], file_path),
                ).start()
            except Exception as e:
                print(f"Error sending notifications: {e}")
                # Don't fail the API call if notifications fail

        return jsonify(probation_data)
    except Exception as e:
        print(f"Error in get_probation_data: {str(e)}")
        return jsonify({"error": str(e)})


def check_num_csv():
    if not os.path.isdir(DATA_FOLDER):
        return 0, 0
    return len([f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")])


def check_probation_cache():
    csv_count = check_num_csv()

    # Check the cache to see if it needs to recompute
    if os.path.exists(MEMBER_INFO_CACHE_FILE):
        with open(MEMBER_INFO_CACHE_FILE) as f:
            cached = json.load(f)
        cached_csv_count = cached.get("_csv_count", 0)
        # Return cache if CSV count is the same.
        if cached_csv_count == csv_count:
            return cached

    # Otherwise recompute
    data = get_member_probation_status()
    data["_csv_count"] = csv_count
    os.makedirs(os.path.dirname(MEMBER_INFO_CACHE_FILE), exist_ok=True)
    with open(MEMBER_INFO_CACHE_FILE, "w") as f:
        json.dump(data, f)
    return data


@app.route("/test_notification")
def test_notification():
    """Test endpoint to send a sample probation failure notification - requires authentication"""
    # Check if user is authenticated
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Authentication required"}), 401

    if not NOTIFICATIONS_ENABLED:
        return jsonify(
            {
                "error": "Notifications not enabled. Check notification_service.py import and email configuration."
            }
        )

    try:
        # Create a test member data
        test_member = {
            "name": "Test Member",
            "joined_date": "2025-01-01",
            "days_since_joined": 100,
            "current_points": 250000,
            "probation_status": "failed",
            "milestones": {
                "week_1": {
                    "target": 500000,
                    "passed": False,
                    "points_at_deadline": 100000,
                    "remaining_points": 400000,
                    "days_left": 0,
                },
                "month_1": {
                    "target": 1500000,
                    "passed": False,
                    "points_at_deadline": 250000,
                    "remaining_points": 1250000,
                    "days_left": 0,
                },
                "month_3": {
                    "target": 3000000,
                    "passed": None,
                    "points_at_deadline": None,
                    "remaining_points": 2750000,
                    "days_left": 10,
                },
            },
        }

        # Send test notification
        success = notification_service.notify_probation_failure(test_member)

        if success:
            return jsonify({"message": "Test notification sent successfully!"})
        else:
            return jsonify(
                {
                    "error": "Failed to send test notification. Check email configuration."
                }
            )

    except Exception as e:
        return jsonify({"error": f"Error sending test notification: {str(e)}"})


@app.route("/notification-admin")
def notification_admin():
    """Admin panel for notification system - requires authentication"""
    # Check if user is authenticated
    if not session.get("admin_authenticated"):
        return redirect(url_for("admin_login"))

    return render_template("notification-admin.html")


@app.route("/api/admin/emails", methods=["GET", "POST", "DELETE", "PATCH"])
def api_admin_emails():
    """Manage admin recipient emails (file-backed). Admin only.
    GET: returns recipients list (back-compat: array of strings). Use ?full=1 to get objects with prefs.
    POST: body { add?: string|string[]|{email,prefs}|[{...}], replace?: string[]|[{...}] }
    PATCH: body { email: str, prefs: {failed?:bool, passed?:bool, non_compliant?:bool} }
    DELETE: body { remove: string|string[] }
    """
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Authentication required"}), 401
    if not NOTIFICATIONS_ENABLED:
        return jsonify({"error": "Notification service not available"}), 400
    try:
        if request.method == "GET":
            full = request.args.get("full") in ("1", "true", "yes")
            if full:
                return jsonify(
                    {
                        "success": True,
                        "recipients": notification_service.admin_recipients,
                    }
                )
            # Back-compat: return plain emails list
            return jsonify(
                {"success": True, "emails": notification_service.admin_emails}
            )
        payload = request.get_json(silent=True) or {}
        if request.method == "POST":
            if "replace" in payload:
                ok = notification_service.replace_admin_emails(
                    payload.get("replace") or []
                )
            else:
                add_vals = payload.get("add")
                add_list = (
                    add_vals
                    if isinstance(add_vals, list)
                    else [add_vals]
                    if add_vals
                    else []
                )
                ok = notification_service.add_admin_emails(add_list)
            return jsonify(
                {
                    "success": bool(ok),
                    "recipients": notification_service.admin_recipients,
                    "emails": notification_service.admin_emails,
                }
            )
        if request.method == "PATCH":
            email = str((payload.get("email") or "")).strip()
            prefs = payload.get("prefs") or {}
            if not email:
                return jsonify({"success": False, "error": "Missing email"}), 400
            # Only pass keys that were actually provided to avoid wiping others
            changes = {}
            for k in ("failed", "passed", "non_compliant"):
                if k in prefs:
                    changes[k] = prefs[k]
            ok = notification_service.update_admin_email_prefs(email, changes)
            return jsonify(
                {
                    "success": bool(ok),
                    "recipients": notification_service.admin_recipients,
                    "emails": notification_service.admin_emails,
                }
            )
        if request.method == "DELETE":
            rem_vals = payload.get("remove")
            rem_list = (
                rem_vals
                if isinstance(rem_vals, list)
                else [rem_vals]
                if rem_vals
                else []
            )
            ok = notification_service.remove_admin_emails(rem_list)
            return jsonify(
                {
                    "success": bool(ok),
                    "recipients": notification_service.admin_recipients,
                    "emails": notification_service.admin_emails,
                }
            )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/members")
def api_admin_members():
    """Return current members with their existing override flags. Admin only."""
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Authentication required"}), 401
    try:
        file_path, date_str, _ = get_latest_csv_file()
        if not file_path:
            return jsonify(
                {"success": False, "error": "No data file available", "members": []}
            ), 404
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        df = df.rename(columns={"name": "Member", "points": "Points"})
        if "Member" not in df.columns:
            return jsonify({"success": False, "error": "Missing Member column"}), 400
        overrides = load_probation_overrides()
        names = sorted([str(n) for n in df["Member"].dropna().unique().tolist()])
        members = []
        for n in names:
            o = overrides.get(n, {}) if isinstance(overrides, dict) else {}
            members.append(
                {
                    "name": n,
                    "overrides": {
                        "week_1": (o.get("week_1") if "week_1" in o else None),
                        "month_1": (o.get("month_1") if "month_1" in o else None),
                        "month_3": (o.get("month_3") if "month_3" in o else None),
                    },
                }
            )
        return jsonify(
            {
                "success": True,
                "members": members,
                "count": len(members),
                "latest_date": date_str,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/overrides", methods=["GET", "POST"])
def api_overrides():
    """Get or update probation overrides. Admin only.
    GET -> { overrides: { member: {week_1, month_1, month_3} } }
    POST body: { member: str, overrides: {week_1?: bool, month_1?: bool, month_3?: bool}, remove?: bool }
    - If remove = true OR no truthy flags provided, the member entry is deleted.
    """
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Authentication required"}), 401
    try:
        if request.method == "GET":
            return jsonify(
                {"success": True, "overrides": load_probation_overrides() or {}}
            )

        payload = request.get_json(silent=True) or {}
        member = str(payload.get("member", "")).strip()
        if not member:
            return jsonify({"success": False, "error": "Missing 'member'"}), 400

        remove = bool(payload.get("remove"))
        incoming = payload.get("overrides") or {}

        data = load_probation_overrides() or {}
        if remove:
            if member in data:
                del data[member]
        else:
            # Start from existing per-member overrides (dict of provided keys only)
            per = data.get(member, {}) if isinstance(data.get(member, {}), dict) else {}
            # Apply tri-state updates: True/False to set, None to clear key
            for key in ("week_1", "month_1", "month_3"):
                if key in incoming:
                    val = incoming.get(key)
                    if val is True or val is False:
                        per[key] = bool(val)
                    else:
                        # treat anything else (None/null) as clear/no override
                        if key in per:
                            per.pop(key, None)
            # If empty after updates, remove member; else save back
            if per:
                data[member] = per
            else:
                data.pop(member, None)

        if not save_probation_overrides(data):
            return jsonify({"success": False, "error": "Failed to save overrides"}), 500

        # Build a stable tri-state response for the member (include missing keys as null)
        per = data.get(member, {}) if member in data else {}

        def tri(o, k):
            return o.get(k) if k in o else None

        return jsonify(
            {
                "success": True,
                "overrides": {
                    "week_1": tri(per, "week_1"),
                    "month_1": tri(per, "month_1"),
                    "month_3": tri(per, "month_3"),
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/admin_login", methods=["GET", "POST"])
def admin_login():
    """Admin login page"""
    if request.method == "POST":
        password = request.form.get("password")
        if password == ADMIN_PASSWORD:
            session["admin_authenticated"] = True
            return redirect(url_for("notification_admin"))
        else:
            return render_template("admin-login.html", error="Invalid password")

    return render_template("admin-login.html")


@app.route("/admin_logout")
def admin_logout():
    """Admin logout"""
    session.pop("admin_authenticated", None)
    return redirect(url_for("admin_login"))


@app.route("/notification_status")
def notification_status():
    """Get notification system status and configuration - requires authentication"""
    # Check if user is authenticated
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Authentication required"}), 401

    if not NOTIFICATIONS_ENABLED:
        return jsonify(
            {"enabled": False, "error": "Notification service not available"}
        )

    try:
        config_status = {
            "enabled": True,
            "smtp_server": notification_service.smtp_server,
            "smtp_port": notification_service.smtp_port,
            "sender_email": notification_service.sender_email or "Not configured",
            "sender_configured": bool(notification_service.sender_email),
            "admin_emails_configured": len(notification_service.admin_emails),
            "admin_emails": notification_service.admin_emails
            if notification_service.admin_emails
            else [],
            "admin_recipients": notification_service.admin_recipients,
            "notification_history_count": len(
                notification_service.notification_history
            ),
        }
        return jsonify(config_status)
    except Exception as e:
        return jsonify({"enabled": False, "error": str(e)})


@app.route("/api/file_count")
def get_file_count():
    """Get count of CSV files available in the specified date range"""
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        if not start_date or not end_date:
            return jsonify({"error": "Both start_date and end_date are required"}), 400

        # Parse dates
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        if start_dt > end_dt:
            return jsonify({"error": "Start date cannot be after end date"}), 400

        # Check if the data folder exists
        if not os.path.exists(DATA_FOLDER):
            return jsonify({"error": "Data folder not found"}), 404

        # Get all CSV files in the data folder
        csv_files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))

        # Filter files based on date range
        filtered_files = []
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            # Extract date from filename (format: sheepit_team_points_YYYY-MM-DD.csv)
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
            if date_match:
                file_date_str = date_match.group(1)
                try:
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                    if start_dt <= file_date <= end_dt:
                        filtered_files.append(csv_file)
                except ValueError:
                    continue

        return jsonify(
            {"file_count": len(filtered_files), "total_files": len(csv_files)}
        )

    except Exception as e:
        print(f"Error getting file count: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/download_csv_files")
def download_csv_files():
    """Download CSV files from the Scraped_Team_Info folder as a ZIP archive with optional date filtering"""
    try:
        # Get date range parameters
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        # Check if the data folder exists
        if not os.path.exists(DATA_FOLDER):
            return jsonify({"error": "Data folder not found"}), 404

        # Get all CSV files in the data folder
        csv_files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))

        if not csv_files:
            return jsonify({"error": "No CSV files found in data folder"}), 404

        # Filter files based on date range if provided
        filtered_files = csv_files
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")

                filtered_files = []
                for csv_file in csv_files:
                    filename = os.path.basename(csv_file)
                    # Extract date from filename (format: sheepit_team_points_YYYY-MM-DD.csv)
                    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                    if date_match:
                        file_date_str = date_match.group(1)
                        try:
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                            if start_dt <= file_date <= end_dt:
                                filtered_files.append(csv_file)
                        except ValueError:
                            continue

                if not filtered_files:
                    return jsonify(
                        {"error": "No CSV files found in the specified date range"}
                    ), 404

            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        # Create a ZIP file in memory
        memory_file = io.BytesIO()

        with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
            for csv_file in filtered_files:
                # Get just the filename (not the full path)
                filename = os.path.basename(csv_file)
                # Add file to ZIP
                zf.write(csv_file, filename)

        memory_file.seek(0)

        # Generate filename based on date range or timestamp
        if start_date and end_date:
            start_formatted = start_date.replace("-", "")
            end_formatted = end_date.replace("-", "")
            zip_filename = f"IBU_Team_Data_{start_formatted}_to_{end_formatted}.zip"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_filename = f"IBU_Team_Data_{timestamp}.zip"

        return send_file(
            memory_file,
            as_attachment=True,
            download_name=zip_filename,
            mimetype="application/zip",
        )

    except Exception as e:
        print(f"Error creating CSV download: {str(e)}")
        return jsonify({"error": f"Failed to create download: {str(e)}"}), 500


@app.route("/trends")
def trends():
    """Render the trends analysis page with consistent header metadata"""
    latest_file, latest_date_str, file_timestamp = get_latest_csv_file()
    time_ago = get_time_ago_string(file_timestamp) if file_timestamp else "Recently"
    return render_template(
        "trends.html", time_ago=time_ago, latest_date=latest_date_str or "-"
    )


@app.route("/api/trends/members")
def api_trends_members():
    """Return available members (from most recent CSV), earliest & latest dates."""
    try:
        file_paths = get_csv_files_from_folder()
        if not file_paths:
            return jsonify({"success": False, "error": "No CSV files found"}), 404

        # Build structured list with parsed dates
        file_infos = []
        for p in file_paths:
            fname = os.path.basename(p)
            m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
            if not m:
                continue
            try:
                parsed_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            except ValueError:
                continue
            file_infos.append(
                {"path": p, "filename": fname, "parsed_date": parsed_date}
            )

        if not file_infos:
            return jsonify(
                {"success": False, "error": "No parsable CSV filenames"}
            ), 404

        # Latest file by date
        latest_info = max(file_infos, key=lambda x: x["parsed_date"])
        earliest_info = min(file_infos, key=lambda x: x["parsed_date"])

        df = pd.read_csv(latest_info["path"])
        df = normalize_member_points_columns(df)

        members = []
        for _, row in df.iterrows():
            member_name = row.get("Member", "") or row.get("Name", "")
            if not member_name:
                continue
            points_val = row.get("Points", 0)
            try:
                points_int = int(points_val) if pd.notna(points_val) else 0
            except Exception:
                points_int = 0
            members.append({"name": str(member_name), "current_points": points_int})

        members.sort(key=lambda x: x["current_points"], reverse=True)

        return jsonify(
            {
                "success": True,
                "members": members,
                "latest_date": latest_info["parsed_date"].strftime("%Y-%m-%d"),
                "earliest_date": earliest_info["parsed_date"].strftime("%Y-%m-%d"),
                "member_count": len(members),
            }
        )
    except Exception as e:
        print(f"Error getting members list: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/trends/teams")
def api_trends_teams():
    """Return available teams (from most recent rankings CSV) with current metrics."""
    try:
        files = get_team_points_files_from_folder()
        if not files:
            return jsonify(
                {"success": False, "error": "No team rankings files found"}
            ), 404
        file_infos = []
        for p in files:
            fname = os.path.basename(p)
            m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
            if not m:
                continue
            try:
                parsed_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            except ValueError:
                continue
            file_infos.append(
                {"path": p, "filename": fname, "parsed_date": parsed_date}
            )
        if not file_infos:
            return jsonify(
                {"success": False, "error": "No parsable rankings filenames"}
            ), 404
        latest = max(file_infos, key=lambda x: x["parsed_date"])
        earliest = min(file_infos, key=lambda x: x["parsed_date"])
        df = pd.read_csv(latest["path"])
        teams = []
        for _, row in df.iterrows():
            name = str(row.get("Name", "")).strip()
            if not name:
                continue

            def _get_int(col):
                try:
                    return int(row.get(col, 0)) if pd.notna(row.get(col, 0)) else 0
                except Exception:
                    return 0

            teams.append(
                {
                    "name": name,
                    "total_points": _get_int("total_points"),
                    "members": _get_int("members"),
                    "90_days": _get_int("90_days"),
                    "180_days": _get_int("180_days"),
                }
            )
        teams.sort(key=lambda x: x["total_points"], reverse=True)
        return jsonify(
            {
                "success": True,
                "teams": teams,
                "latest_date": latest["parsed_date"].strftime("%Y-%m-%d"),
                "earliest_date": earliest["parsed_date"].strftime("%Y-%m-%d"),
                "team_count": len(teams),
            }
        )
    except Exception as e:
        print(f"Error getting teams list: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/trends/data")
def api_trends_data():
    """Return trend time-series data, with optional aggregation & predictions."""
    # Parse requested series (comma separated in 'series' param)
    series_param = request.args.get("series", "")
    series_list = [s.strip() for s in series_param.split(",") if s.strip()]

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    chart_type = request.args.get("chart_type", "line")
    time_period = request.args.get("time_period", "daily")
    predictions_enabled = request.args.get("predictions", "false").lower() == "true"
    prediction_method = request.args.get("prediction_method", "linear")
    prediction_days = int(request.args.get("prediction_days", "30"))
    value_mode = request.args.get(
        "value_mode", "cumulative"
    )  # 'cumulative' or 'interval'
    fill_lines = request.args.get("fill_lines", "true").lower() == "true"
    team_metric = request.args.get(
        "team_metric", "total_points"
    )  # total_points|members|90_days|180_days
    hide_first_interval = (
        request.args.get("hide_first_interval", "false").lower() == "true"
    )
    hide_first_interval = True
    original_series_list = list(series_list)
    # Separate team series (prefixed with team:) from member series
    team_series_requested = []
    member_series = []
    for s in series_list:
        if s.lower().startswith("team:"):
            team_series_requested.append(s[5:].strip())
        else:
            member_series.append(s)
    series_list = member_series

    file_paths = get_csv_files_from_folder()
    if not file_paths:
        return jsonify({"success": False, "error": "No data files available"}), 404

    # Build structured list (date parsing only once)
    file_infos = []
    for p in file_paths:
        fname = os.path.basename(p)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
        if not m:
            continue
        try:
            parsed_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        file_infos.append({"path": p, "filename": fname, "parsed_date": parsed_date})

    if not file_infos:
        return jsonify({"success": False, "error": "No parsable data files"}), 404

    # Date filtering
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            file_infos = [f for f in file_infos if f["parsed_date"] >= start_dt]
        except ValueError:
            return jsonify({"success": False, "error": "Invalid start_date"}), 400
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            file_infos = [f for f in file_infos if f["parsed_date"] <= end_dt]
        except ValueError:
            return jsonify({"success": False, "error": "Invalid end_date"}), 400

    if not file_infos:
        return jsonify({"success": False, "error": "No files in selected range"}), 404

    file_infos.sort(key=lambda x: x["parsed_date"])

    # Collect daily data
    daily_data = {
        name: {"dates": [], "points": [], "daily_change": [], "rank": []}
        for name in series_list
        if name != "total"
    }
    # 'total' handled separately
    total_series_needed = "total" in series_list
    total_data = (
        {"dates": [], "points": [], "daily_change": []} if total_series_needed else None
    )

    last_points_tracker = {name: None for name in daily_data.keys()}
    last_total_points = None

    for info in file_infos:
        try:
            df = pd.read_csv(info["path"])
            df = normalize_member_points_columns(df)
        except Exception as e:
            print(f"Failed reading {info['path']}: {e}")
            continue
        date_label = info["parsed_date"].strftime("%Y-%m-%d")

        # Total points (sum Points column) if needed
        if total_series_needed:
            total_points_value = 0
            if "Points" in df.columns:
                try:
                    total_points_value = int(df["Points"].fillna(0).astype(int).sum())
                except Exception:
                    total_points_value = int(df["Points"].fillna(0).sum())
            total_data["dates"].append(date_label)
            total_data["points"].append(total_points_value)
            if last_total_points is not None:
                total_data["daily_change"].append(
                    total_points_value - last_total_points
                )
            else:
                total_data["daily_change"].append(0)
            last_total_points = total_points_value

        # Individual members
        for member_name in daily_data.keys():
            # Find row
            member_row = None
            for _, row in df.iterrows():
                row_name = row.get("Name", str(row.get("Member", "")))
                row_name = str(row_name).strip()
                if row_name == member_name:
                    member_row = row
                    break
            points_val = 0
            rank_val = 0
            if member_row is not None:
                try:
                    points_val = (
                        int(member_row.get("Points", 0))
                        if pd.notna(member_row.get("Points", 0))
                        else 0
                    )
                except Exception:
                    points_val = 0
                try:
                    rank_val = (
                        int(member_row.get("Rank", 0))
                        if pd.notna(member_row.get("Rank", 0))
                        else 0
                    )
                except Exception:
                    rank_val = 0
            # Append
            daily_member = daily_data[member_name]
            daily_member["dates"].append(date_label)
            daily_member["points"].append(points_val)
            daily_member["rank"].append(rank_val)
            if last_points_tracker[member_name] is not None:
                daily_member["daily_change"].append(
                    points_val - last_points_tracker[member_name]
                )
            else:
                daily_member["daily_change"].append(0)
            last_points_tracker[member_name] = points_val

    # Combine into trends_data structure

    trends_data = {}
    if total_series_needed:
        trends_data[
            "Total Team Points"
        ] = {  # Display label different from param 'total'
            "dates": total_data["dates"],
            "points": total_data["points"],
            "daily_change": total_data["daily_change"],
            # Provide rank list of zeros so aggregation logic doesn't index error
            "rank": [0] * len(total_data["dates"]),
        }
    for k, v in daily_data.items():
        trends_data[k] = v

    # --- Team rankings integration -------------------------------------------------
    if team_series_requested:
        team_files = get_team_points_files_from_folder()
        if team_files:
            team_file_infos = []
            for p in team_files:
                fname = os.path.basename(p)
                m2 = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
                if not m2:
                    continue
                try:
                    parsed_date = datetime.strptime(m2.group(1), "%Y-%m-%d").date()
                except ValueError:
                    continue
                # Respect date filters
                if start_date:
                    try:
                        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
                        if parsed_date < sd:
                            continue
                    except Exception:
                        pass
                if end_date:
                    try:
                        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
                        if parsed_date > ed:
                            continue
                    except Exception:
                        pass
                team_file_infos.append({"path": p, "parsed_date": parsed_date})
            team_file_infos.sort(key=lambda x: x["parsed_date"])
            team_data_struct = {
                t: {"dates": [], "points": [], "daily_change": [], "rank": []}
                for t in team_series_requested
            }
            last_vals = {t: None for t in team_series_requested}
            for info in team_file_infos:
                try:
                    df_team = pd.read_csv(info["path"])
                except Exception as e:
                    print(f"Failed reading team rankings {info['path']}: {e}")
                    continue
                date_label = info["parsed_date"].strftime("%Y-%m-%d")
                # Build normalized name map once per file for robust matching
                try:
                    df_team["_norm_name"] = (
                        df_team["Name"].astype(str).str.strip().str.lower()
                    )
                except Exception:
                    df_team["_norm_name"] = ""
                # Pre-compute sanitized names
                try:
                    df_team["_san_name"] = df_team["Name"].apply(_sanitize_team_name)
                except Exception:
                    df_team["_san_name"] = df_team["_norm_name"]
                debug_team_matching = []
                for tname in team_series_requested:
                    target_norm = tname.strip().lower()
                    target_san = _sanitize_team_name(tname)
                    # 1. Exact norm
                    row = df_team[df_team["_norm_name"] == target_norm]
                    # 2. Exact sanitized
                    if row.empty:
                        row = df_team[df_team["_san_name"] == target_san]
                    # 3. Startswith norm (first 12 chars)
                    if row.empty:
                        cand = df_team[
                            df_team["_norm_name"].str.startswith(target_norm[:12])
                        ]
                        if len(cand) == 1:
                            row = cand
                    # 4. Startswith sanitized (first 12 chars)
                    if row.empty:
                        cand2 = df_team[
                            df_team["_san_name"].str.startswith(target_san[:12])
                        ]
                        if len(cand2) == 1:
                            row = cand2
                    # 5. Contains sanitized token (rare fallback) - pick first smallest rank
                    if row.empty and target_san:
                        subset = df_team[
                            df_team["_san_name"].str.contains(
                                target_san.split(" ")[0], na=False
                            )
                        ]
                        if len(subset) == 1:
                            row = subset
                        elif len(subset) > 1:
                            # choose row with minimal rank value if available
                            try:
                                subset_numeric = subset.copy()
                                subset_numeric["__rk"] = pd.to_numeric(
                                    subset_numeric.get("Rank"), errors="coerce"
                                ).fillna(999999)
                                row = subset_numeric.sort_values("__rk").head(1)
                            except Exception:
                                row = subset.head(1)
                    if row.empty:
                        debug_team_matching.append(
                            {
                                "requested": tname,
                                "target_norm": target_norm,
                                "target_san": target_san,
                                "status": "not_found",
                            }
                        )
                        continue
                    r = row.iloc[0]

                    def _metric_value(col):
                        try:
                            return int(r.get(col, 0)) if pd.notna(r.get(col, 0)) else 0
                        except Exception:
                            return 0

                    if team_metric == "members":
                        val = _metric_value("members")
                    elif team_metric == "90_days":
                        val = _metric_value("90_days")
                    elif team_metric == "180_days":
                        val = _metric_value("180_days")
                    else:
                        val = _metric_value("total_points")
                    try:
                        rk = int(r.get("Rank", 0)) if pd.notna(r.get("Rank", 0)) else 0
                    except Exception:
                        rk = 0
                    entry = team_data_struct[tname]
                    entry["dates"].append(date_label)
                    entry["points"].append(val)
                    if last_vals[tname] is None:
                        entry["daily_change"].append(0)
                    else:
                        entry["daily_change"].append(max(0, val - last_vals[tname]))
                    entry["rank"].append(rk)
                    last_vals[tname] = val
                    debug_team_matching.append(
                        {
                            "requested": tname,
                            "target_norm": target_norm,
                            "target_san": target_san,
                            "matched_name": str(r.get("Name")),
                            "date": date_label,
                            "value": val,
                            "rank": rk,
                        }
                    )
                if debug_team_matching:
                    # Print once per file to avoid log spam
                    print(
                        f"[TEAM_MATCH_DEBUG] file={info['path']} entries={debug_team_matching}"
                    )
            for tname, tdata in team_data_struct.items():
                if tdata["dates"]:
                    # Prefix to distinguish from member names
                    trends_data[f"Team: {tname}"] = tdata
    # Track which dates originally existed (before gap fill) for interval production distribution
    for series_name, sdata in trends_data.items():
        sdata["observed_dates"] = set(sdata["dates"])

    # Fill missing daily dates (forward-fill points) to avoid gaps in daily view
    if time_period == "daily":
        trends_data = fill_missing_daily_dates(trends_data)

    # Snapshot raw daily points & compute raw daily produced (before aggregation & before interval transformation)
    for series_name, sdata in trends_data.items():
        pts = sdata.get("points", [])
        daily_prod = []
        prev = None
        for p in pts:
            if prev is None:
                daily_prod.append(0)
            else:
                daily_prod.append(max(0, p - prev))
            prev = p
        sdata["daily_points_raw"] = list(pts)
        sdata["daily_dates_raw"] = list(sdata.get("dates", []))
        sdata["daily_produced_raw"] = daily_prod

    # Aggregate if needed
    if time_period != "daily":
        trends_data = aggregate_time_period(trends_data, time_period)

    # Convert to interval production if requested (replace points with per-period produced)
    if value_mode == "interval":
        for series_name, sdata in trends_data.items():
            pts = sdata.get("points", [])
            dates = sdata.get("dates", [])
            produced = [0] * len(pts)
            if not pts:
                sdata["produced"] = produced
                continue
            # Daily: distribute delta across gaps between real observations
            if time_period == "daily":
                observed = sdata.get("observed_dates", set())
                last_real_index = None
                last_real_points = None
                for idx, (d_str, p_val) in enumerate(zip(dates, pts)):
                    if d_str in observed:
                        if last_real_index is None:
                            # First observation: set produced to 0 to avoid huge spike (user preference)
                            produced[idx] = 0
                            last_real_index = idx
                            last_real_points = p_val
                            continue
                        # Compute gap span (number of days between observations)
                        gap_days = idx - last_real_index
                        delta = (
                            max(0, p_val - last_real_points)
                            if last_real_points is not None
                            else 0
                        )
                        if gap_days <= 0:
                            last_real_index = idx
                            last_real_points = p_val
                            continue
                        # Even distribution across gap_days (includes current real day)
                        # Example: real at i0=0 and i1=2 (gap_days=2) -> distribute across indices 1 and 2
                        per_day = delta // gap_days if gap_days > 0 else 0
                        remainder = delta - per_day * gap_days
                        for j in range(1, gap_days + 1):
                            target_index = last_real_index + j
                            add_val = per_day + (
                                remainder if j == gap_days else 0
                            )  # put remainder on last real day
                            produced[target_index] = add_val
                        last_real_index = idx
                        last_real_points = p_val
                sdata["produced"] = produced
            else:
                # Aggregated periods: simple difference period-over-period
                prev = None
                for i, p in enumerate(pts):
                    if prev is None:
                        # First aggregated period produced = 0 (avoid giant first bar)
                        produced[i] = 0
                    else:
                        produced[i] = max(0, p - prev)
                    prev = p
                sdata["produced"] = produced
        # produced now holds per-day estimated production (distributed over gaps)

        # Optional: remove the first interval point entirely if requested (hide_first_interval=true)
        if hide_first_interval:
            for sname, sdata in trends_data.items():
                if len(sdata.get("dates", [])) > 1:
                    for key in [
                        "dates",
                        "points",
                        "daily_change",
                        "rank",
                        "produced",
                        "daily_points_raw",
                        "daily_produced_raw",
                        "daily_dates_raw",
                    ]:
                        if (
                            key in sdata
                            and isinstance(sdata[key], list)
                            and len(sdata[key]) > 1
                        ):
                            sdata[key] = sdata[key][1:]
                    # observed_dates is a set; remove first date if present
                    if "observed_dates" in sdata and isinstance(
                        sdata["observed_dates"], set
                    ):
                        # Determine original first date (after potential slice we don't need it)
                        # Not strictly necessary to adjust, leave as-is or rebuild:
                        pass

    # Build traces
    if chart_type == "candlestick":
        traces = prepare_candlestick_data(
            trends_data, value_mode=value_mode, time_period=time_period
        )
    elif chart_type == "bar":
        traces = prepare_bar_data(trends_data, value_mode=value_mode)
    else:
        traces = prepare_line_data(
            trends_data, value_mode=value_mode, fill_enabled=fill_lines
        )

    # Predictions
    if predictions_enabled and prediction_days > 0:
        add_prediction_traces(traces, prediction_method, prediction_days)

    # Distinct dates count (from any first non-empty series)
    data_points = 0
    for t in traces:
        if t.get("type") in ("scatter", "bar") and t.get("x"):
            data_points = max(data_points, len(t.get("x", [])))

    y_axis_title = "Points Produced" if value_mode == "interval" else "Points"
    layout = {
        "title": "",
        "paper_bgcolor": "rgba(255,255,255,0.1)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "xaxis": {
            "title": "Date",
            "gridcolor": "rgba(255,255,255,0.08)",
            "showline": False,
            "zeroline": False,
            "tickangle": -35,
            "ticks": "outside",
            "tickcolor": "rgba(255,255,255,0.15)",
            "ticklen": 6,
        },
        "yaxis": {
            "title": y_axis_title,
            "rangemode": "tozero",
            "gridcolor": "rgba(255,255,255,0.08)",
            "showline": False,
            "zeroline": False,
        },
        "legend": {
            "orientation": "h",
            "bgcolor": "rgba(0,0,0,0)",
            "yanchor": "bottom",
            "y": 1.02,
            "x": 0,
        },
        "margin": {"l": 60, "r": 30, "t": 30, "b": 70},
        "hovermode": "x unified",
        "hoverlabel": {
            "bgcolor": "#1e2533",
            "bordercolor": "#3a4558",
            "font": {"color": "#ffffff"},
        },
        "font": {"family": "Segoe UI, Inter, Arial", "color": "#f0f2f6", "size": 12},
        "transition": {"duration": 400, "easing": "cubic-in-out"},
    }
    # Override hovermode for interval candlestick so custom hovertemplate displays instead of default OHLC unified panel
    if chart_type == "candlestick" and value_mode == "interval":
        layout["hovermode"] = "closest"
    config = {
        "displaylogo": False,
        "responsive": True,
        "modeBarButtonsToRemove": [
            "zoom2d",
            "pan2d",
            "select2d",
            "lasso2d",
            "autoScale2d",
            "resetScale2d",
            "toggleSpikelines",
        ],
        "toImageButtonOptions": {"format": "png", "filename": "ibu_trends_chart"},
    }

    return jsonify(
        {
            "success": True,
            "data": traces,
            "layout": layout,
            "config": config,
            "data_points": data_points,
            "metadata": {
                "chart_type": chart_type,
                "time_period": time_period,
                "value_mode": value_mode,
                "series_requested": original_series_list,
                "member_series_resolved": series_list,
                "team_series_requested": team_series_requested,
                "team_metric": team_metric,
                "fill_lines": fill_lines,
                "date_range": {
                    "start": file_infos[0]["parsed_date"].strftime("%Y-%m-%d"),
                    "end": file_infos[-1]["parsed_date"].strftime("%Y-%m-%d"),
                },
            },
        }
    )


def add_prediction_traces(traces, method, days):
    """Append prediction traces in-place based on existing line/candlestick traces.
    We only generate predictions for scatter (line) data or candlestick close values."""
    try:
        future_color_suffix = {"linear": "dash", "moving_average": "dot"}.get(
            method, "dash"
        )
        for trace in list(traces):  # iterate over a copy
            # Determine y-series
            if trace.get("type") == "candlestick":
                y_series = trace.get("close", [])
            else:
                y_series = trace.get("y", [])
            x_series = trace.get("x", [])
            if len(y_series) < 3:
                continue  # not enough data
            # Build numeric x as day indices
            base_dates = [datetime.strptime(d, "%Y-%m-%d") for d in x_series]
            start_date = base_dates[0]
            x_numeric = [(d - start_date).days for d in base_dates]

            if method == "linear":
                # Simple linear regression
                n = len(x_numeric)
                sum_x = sum(x_numeric)
                sum_y = sum(y_series)
                sum_xx = sum(x * x for x in x_numeric)
                sum_xy = sum(x * y for x, y in zip(x_numeric, y_series))
                denom = n * sum_xx - sum_x * sum_x
                if denom == 0:
                    continue
                slope = (n * sum_xy - sum_x * sum_y) / denom
                intercept = (sum_y - slope * sum_x) / n

                def predict(x):
                    return intercept + slope * x

                # Use last trend continuation
            else:  # moving_average
                window = 5 if len(y_series) >= 5 else max(2, len(y_series) // 2)
                avg_changes = []
                for i in range(1, len(y_series)):
                    avg_changes.append(y_series[i] - y_series[i - 1])
                # average of last window changes
                recent_change = (
                    sum(avg_changes[-window:]) / len(avg_changes[-window:])
                    if avg_changes
                    else 0
                )

                def predict(x):
                    # x here is absolute day index relative to start_date
                    last_index = x_numeric[-1]
                    delta_days = x - last_index
                    return y_series[-1] + recent_change * delta_days

            future_dates = []
            future_values = []
            last_index = x_numeric[-1]
            last_date = base_dates[-1]
            for i in range(1, days + 1):
                future_date = last_date + timedelta(days=i)
                future_dates.append(future_date.strftime("%Y-%m-%d"))
                # Clamp predictions at zero to avoid negative values
                y_pred = predict(last_index + i)
                if y_pred < 0:
                    y_pred = 0
                future_values.append(y_pred)

            pred_trace = {
                "name": f"{trace['name']} (Prediction)",
                "type": "scatter",
                "mode": "lines",
                "x": future_dates,
                "y": future_values,
                "line": {
                    "color": trace.get("line", {}).get("color", "#999999"),
                    "dash": future_color_suffix,
                },
                "opacity": 0.7,
                "hovertemplate": "<b>%{meta}</b><br>Date: %{x}<br>Predicted Points: %{y:.0f}<extra></extra>",
                "meta": trace["name"],
            }
            traces.append(pred_trace)
    except Exception as e:
        print(f"Prediction generation error: {e}")


def aggregate_time_period(trends_data, time_period):
    """Aggregate daily data into weekly, monthly, yearly, or fixed window periods (90/180 days).
    For 90/180 day aggregation, buckets are aligned using the earliest date across all series
    as the anchor to ensure consistent bucket boundaries for comparison.
    """
    aggregated_data = {}

    # Compute a global anchor date (earliest across all series) for fixed-length windows
    anchor_date = None
    try:
        for series in trends_data.values():
            for d in series.get("dates", []) or []:
                dt = datetime.strptime(d, "%Y-%m-%d")
                if anchor_date is None or dt < anchor_date:
                    anchor_date = dt
    except Exception:
        anchor_date = None

    # Determine window size (in days) for custom fixed windows
    window_days = None
    if time_period == "90_days":
        window_days = 90
    elif time_period == "180_days":
        window_days = 180

    for member_name, data in trends_data.items():
        aggregated_data[member_name] = {
            "dates": [],
            "points": [],
            "daily_change": [],
            "rank": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
        }

        if not data["dates"]:
            continue

        # Group data by time period
        current_period = None
        period_data = []

        for i, date_str in enumerate(data["dates"]):
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")

            # Determine period based on time_period
            if window_days and anchor_date is not None:
                # Fixed-length window (e.g., 90 or 180 days) anchored at earliest date
                delta_days = (date_obj - anchor_date).days
                bucket_index = delta_days // window_days
                period_start = anchor_date + timedelta(days=bucket_index * window_days)
                period_key = period_start.strftime("%Y-%m-%d")
            elif time_period == "weekly":
                # Get Monday of the week
                period_key = (date_obj - timedelta(days=date_obj.weekday())).strftime(
                    "%Y-%m-%d"
                )
            elif time_period == "monthly":
                period_key = date_obj.strftime("%Y-%m-01")
            elif time_period == "yearly":
                period_key = date_obj.strftime("%Y-01-01")
            else:
                period_key = date_str

            if current_period != period_key:
                # Process previous period
                if period_data:
                    process_period_data(
                        aggregated_data[member_name], current_period, period_data
                    )

                # Start new period
                current_period = period_key
                period_data = []

            # Safe rank access
            rank_value = 0
            if i < len(data.get("rank", [])):
                try:
                    rank_value = (
                        int(data["rank"][i]) if data["rank"][i] is not None else 0
                    )
                except Exception:
                    rank_value = 0
            period_data.append(
                {
                    "date": date_str,
                    "points": data["points"][i] if i < len(data["points"]) else 0,
                    "daily_change": data["daily_change"][i]
                    if i < len(data["daily_change"])
                    else 0,
                    "rank": rank_value,
                }
            )

        # Process last period
        if period_data:
            process_period_data(
                aggregated_data[member_name], current_period, period_data
            )

    return aggregated_data


def process_period_data(member_data, period_date, period_data):
    """Process data for a specific time period"""
    if not period_data:
        return

    # Calculate OHLC for candlestick charts
    points_values = [d["points"] for d in period_data]
    open_val = points_values[0]
    close_val = points_values[-1]
    high_val = max(points_values)
    low_val = min(points_values)

    # Calculate total change for the period
    total_change = sum(d["daily_change"] for d in period_data)

    # Use average rank for the period
    avg_rank = (
        sum(d["rank"] for d in period_data) / len(period_data) if period_data else 0
    )

    member_data["dates"].append(period_date)
    member_data["points"].append(close_val)
    member_data["daily_change"].append(total_change)
    member_data["rank"].append(int(avg_rank))
    member_data["open"].append(open_val)
    member_data["high"].append(high_val)
    member_data["low"].append(low_val)
    member_data["close"].append(close_val)


def prepare_bar_data(trends_data, value_mode="cumulative"):
    """Prepare bar chart data.
    Cumulative mode: bar height = cumulative points value (monotonic, what users usually expect for totals).
    Interval mode: bar height = produced value (per-period production).
    (Previous behavior used positive deltas for cumulative; replaced for clarity.)"""
    chart_data = []
    interval = value_mode == "interval"
    for member_name, data in trends_data.items():
        if not data.get("dates"):
            continue
        color = name_to_color(member_name)
        if interval and "produced" in data:
            y_vals = data["produced"]
            label = "Produced"
        else:  # cumulative
            y_vals = data.get("points", [])
            label = "Points"
        trace = {
            "name": member_name,
            "type": "bar",
            "x": data["dates"],
            "y": y_vals,
            "marker": {"color": color, "line": {"width": 0}},
            "opacity": 0.9,
            "hovertemplate": f"<b>{member_name}</b><br>Date: %{{x}}<br>{label}: %{{y:,}}<extra></extra>",
        }
        chart_data.append(trace)
    return chart_data


def prepare_candlestick_data(trends_data, value_mode="cumulative", time_period="daily"):
    """Prepare candlestick traces.
    Interval mode (production comparison between periods):
      - open  = previous period's produced amount (0 for first)
      - close = current period's produced amount
      - high/low = max/min(open, close) for simple magnitude bar (not financial high/low)
      - customdata = [produced, change_vs_prev, percent_change_string]
      - hovertemplate shows Produced, Δ, and %Δ.
    Cumulative mode:
      - Uses stored OHLC if available or falls back to points list for all fields (traditional cumulative level candle).
    (Historical note: earlier version attempted baseline cumulative points; simplified now to production vs previous production.)"""
    chart_data = []
    interval = value_mode == "interval"
    for member_name, data in trends_data.items():
        dates = data.get("dates")
        if not dates:
            continue
        points_list = data.get("points", [])  # cumulative closes
        produced_list = data.get("produced") if interval else None
        open_vals = []
        high_vals = []
        low_vals = []
        close_vals = []
        if interval and produced_list is not None:
            for i, prod in enumerate(produced_list):
                prev_prod = produced_list[i - 1] if i > 0 else 0
                o = prev_prod
                c = prod
                open_vals.append(o)
                close_vals.append(c)
                high_vals.append(max(o, c))
                low_vals.append(min(o, c))
            customdata = []
            for o, c in zip(open_vals, close_vals):
                produced = c
                change = c - o
                if o <= 0:
                    pct_str = "—" if produced > 0 else "0.0%"
                else:
                    pct_val = (change / o) * 100.0
                    pct_str = f"{pct_val:.1f}%"
                customdata.append([produced, change, pct_str])
            hover_template = (
                "<b>%{meta}</b><br>"
                "Period: %{x}<br>"
                "Produced: %{customdata[0]:,}<br>"
                "Δ: %{customdata[1]:,} (%{customdata[2]})"
                "<extra></extra>"
            )
        else:
            o = data.get("open") or points_list
            high = data.get("high") or points_list
            low = data.get("low") or points_list
            c = data.get("close") or points_list
            open_vals = o
            high_vals = high
            low_vals = low
            close_vals = c
            hover_template = "<b>%{meta}</b><br>Period: %{x}<br>O: %{open:,}<br>H: %{high:,}<br>L: %{low:,}<br>C: %{close:,}<extra></extra>"
        base_color = name_to_color(member_name)
        inc_color = blend_with(
            base_color, (34, 197, 94), 0.55
        )  # blend with emerald-500
        dec_color = blend_with(base_color, (248, 113, 113), 0.55)  # blend with red-400
        trace = {
            "name": member_name,
            "type": "candlestick",
            "x": dates,
            "open": open_vals,
            "high": high_vals,
            "low": low_vals,
            "close": close_vals,
            "increasing": {"line": {"color": inc_color}},
            "decreasing": {"line": {"color": dec_color}},
            "whiskerwidth": 0.5,
            # Use hovertemplate (not hoverinfo) so custom production stats show; hoverinfo here was suppressing template
            "hovertemplate": hover_template,
            "meta": member_name,
        }
        if interval and produced_list:
            trace["customdata"] = customdata
            # Suppress default candlestick hover (which can still show OHLC in some modes)
            trace["hoverinfo"] = "skip"
            # Compute approximate width based on smallest x-spacing (date axis -> milliseconds)
            width_ms = None
            if len(dates) > 1:
                try:
                    parsed = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
                    gaps = [
                        (parsed[i + 1] - parsed[i]).total_seconds() * 1000
                        for i in range(len(parsed) - 1)
                    ]  # ms
                    if gaps:
                        # Candlestick body tends to be narrower than full category; use 60% of min gap
                        width_ms = min(gaps) * 0.6
                except Exception:
                    width_ms = None
            # Expand height by 1% for normal candles, but apply a minimum height for flat (no variation) candles
            overall_low = min(low_vals)
            overall_high = max(high_vals)
            overall_range = overall_high - overall_low
            if overall_range <= 0:
                overall_range = 1  # prevent division by zero
            min_span = max(
                overall_range * 0.01, 0.5
            )  # at least 0.5 (points) or 0.1% of range
            expanded_base = []
            expanded_height = []
            for high, low in zip(high_vals, low_vals):
                span = high - low
                if span <= 0:
                    # Flat candle: create a small vertical hoverable band centered around the flat value
                    span_eff = min_span
                    lower = low - span_eff / 2.0
                    if lower < 0:
                        lower = 0
                    expanded_base.append(lower)
                    expanded_height.append(span_eff)
                else:
                    extra = span * 0.01  # total 1% extra
                    lower = low - extra * 0.5
                    if lower < 0:
                        lower = 0
                    expanded_base.append(lower)
                    expanded_height.append(span + extra)
            # Add transparent bar overlay spanning (slightly bigger than) candle to capture hover anywhere
            overlay_bar = {
                "name": member_name,
                "type": "bar",
                "x": dates,
                "y": expanded_height,
                "base": expanded_base,
                "marker": {"color": "rgba(0,0,0,0)", "line": {"width": 0}},
                "opacity": 0.01,  # nearly invisible but area still interactive
                "customdata": customdata,
                "hovertemplate": (
                    "<b>%{meta}</b><br>"
                    "Period: %{x}<br>"
                    "Produced: %{customdata[0]:,}<br>"
                    "Δ: %{customdata[1]:,} (%{customdata[2]})"
                    "<extra></extra>"
                ),
                "meta": member_name,
                "showlegend": False,
                "hoverlabel": {"namelength": -1},
                "offsetgroup": f"ovl_{member_name}",
                "legendgroup": f"ovl_{member_name}",
                "width": width_ms,  # match approximate candle body width
            }
            chart_data.append(overlay_bar)
        chart_data.append(trace)
    return chart_data


def prepare_line_data(trends_data, value_mode="cumulative", fill_enabled=True):
    """Prepare data for line charts.
    value_mode: 'cumulative' (points) or 'interval' (produced).
    fill_enabled: when True apply area fill under lines; when False lines only."""
    chart_data = []
    label = "Produced" if value_mode == "interval" else "Points"
    for member_name, data in trends_data.items():
        if not data.get("dates"):
            continue
        color = name_to_color(member_name)
        if value_mode == "interval" and "produced" in data:
            y_vals = data["produced"]
        else:
            y_vals = data.get("points", [])
        trace_data = {
            "name": member_name,
            "type": "scatter",
            "mode": "lines+markers",
            "x": data["dates"],
            "y": y_vals,
            "line": {
                "color": color,
                "width": 2.4,
                "shape": "spline",
                "smoothing": 0.65,
            },
            "marker": {"size": 5, "color": color, "line": {"width": 0}},
            "hovertemplate": f"<b>{member_name}</b><br>Date: %{{x}}<br>{label}: %{{y:,}}<extra></extra>",
        }
        if fill_enabled:
            # Apply area fill for every series in both modes
            alpha_suffix = "30"  # ~19% opacity
            fillcolor = color + alpha_suffix if len(color) == 7 else color
            trace_data["fill"] = "tozeroy"
            trace_data["fillcolor"] = fillcolor
        chart_data.append(trace_data)
    return chart_data


def fill_missing_daily_dates(trends_data):
    """Forward-fill missing dates across series so lines are continuous.
    Uses min->max date over all series; carries last known point; daily_change=0 for filled days."""
    try:
        # Gather all dates
        all_dates = set()
        for series in trends_data.values():
            for d in series.get("dates", []):
                all_dates.add(d)
        if not all_dates:
            return trends_data
        start_dt = min(datetime.strptime(d, "%Y-%m-%d") for d in all_dates)
        end_dt = max(datetime.strptime(d, "%Y-%m-%d") for d in all_dates)
        # Build full date list
        full_dates = []
        cur = start_dt
        while cur <= end_dt:
            full_dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        for name, series in trends_data.items():
            if not series.get("dates"):  # skip empty
                continue
            existing_idx = {d: i for i, d in enumerate(series["dates"])}
            new_dates = []
            new_points = []
            new_changes = []
            new_rank = []
            has_rank = bool(series.get("rank")) and len(series["rank"]) == len(
                series["dates"]
            )
            last_points = None
            last_rank = None
            for d in full_dates:
                if d in existing_idx:
                    idx = existing_idx[d]
                    val = series["points"][idx]
                    chg = series["daily_change"][idx]
                    rk = series["rank"][idx] if has_rank else None
                    last_points = val
                    last_rank = rk
                else:
                    if last_points is None:
                        # Before first recorded date for this series: skip
                        continue
                    val = last_points
                    chg = 0
                    rk = last_rank
                new_dates.append(d)
                new_points.append(val)
                new_changes.append(chg)
                if has_rank:
                    new_rank.append(rk if rk is not None else 0)
            # Replace if extended
            if len(new_dates) > len(series["dates"]):
                series["dates"] = new_dates
                series["points"] = new_points
                series["daily_change"] = new_changes
                if has_rank:
                    series["rank"] = new_rank
        return trends_data
    except Exception as e:
        print(f"fill_missing_daily_dates error: {e}")
        return trends_data


def get_version() -> str:
    try:
        changelog_file = "CHANGELOG.md"

        if not os.path.exists(changelog_file):
            return "unknown"

        header_re = re.compile(
            r"^\s*##\s*\[(?P<version>[^\]]+)\]\s*(?:-\s*(?P<date>\d{4}-\d{2}-\d{2}))?\s*$"
        )

        with open(changelog_file, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.rstrip("\n")
                m = header_re.match(line)
                if not m:
                    continue

                v = (m.group("version") or "").strip()
                if not v or v.lower() == "unreleased":
                    continue

                return v[1:] if v.lower().startswith("v") else v

        return "unknown"

    except Exception as e:
        print(f"Error reading changelog version: {e}")
        return "unknown"


@app.context_processor
def version():
    return {"app_version": get_version()}


# Flask startup
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    print(f"Access the dashboard at: http://0.0.0.0:{port}")
    app.run(debug=True, host="0.0.0.0", port=port)
