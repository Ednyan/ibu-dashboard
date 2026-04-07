import csv
import hashlib
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import tomllib

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.toml"


def _ordinal_suffix(day: int) -> str:
    if 11 <= day % 100 <= 13:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def _format_run_timestamp(dt: datetime | None = None) -> str:
    now = (dt or datetime.now()).astimezone()
    month = now.strftime("%b")
    day = now.day
    year = now.year
    hour = now.strftime("%I").lstrip("0") or "0"
    minute = now.strftime("%M")
    am_pm = now.strftime("%p")
    tz = now.strftime("%Z") or "LOCAL"
    return f"{month} {day}{_ordinal_suffix(day)}, {year} {hour}:{minute}{am_pm} {tz}"


def _coerce_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _resolve_config_path(config_path: str | None) -> Path:
    if config_path:
        candidate = Path(config_path)
    else:
        candidate = DEFAULT_CONFIG_PATH

    if candidate.is_absolute():
        return candidate

    cwd_candidate = Path.cwd() / candidate
    if cwd_candidate.exists():
        return cwd_candidate

    return PROJECT_ROOT / candidate


def _load_scraper_config(config_path: str | None = None) -> tuple[dict, Path]:
    path = _resolve_config_path(config_path)
    defaults = {
        "scraper": {
            "general": {
                "request_timeout_seconds": 60,
                "teams_rank_limit": 150,
            },
            "sheepit": {
                "username": "your_username_here",
                "password": "your_password_here",
                "login_url": "https://www.sheepit-renderfarm.com/user/authenticate",
                "team_url": "https://www.sheepit-renderfarm.com/team/2109",
                "teams_points_url": "https://www.sheepit-renderfarm.com/team",
            },
            "output": {
                "team_info_folder": "Scraped_Team_Info",
                "teams_points_folder": "Scraped_Teams_Points",
            },
            "dashboard": {
                "team_probation_url": "",
                "trigger_notifications": True,
            },
        },
    }
    raw = {}

    if path.exists():
        try:
            with open(path, "rb") as f:
                raw = tomllib.load(f)
        except Exception as e:
            print(f"Error: Failed to parse config at {path}: {e}")
            raise RuntimeError(f"Invalid TOML in {path}: {e}") from e
    else:
        print(f"Warning: Config file not found at {path}. Using built-in defaults.")
        print("Warning: Copy config.toml.example to config.toml and set real values.")

    scraper_root = raw.get("scraper", {})
    general_cfg = scraper_root.get("general", {})
    sheepit_cfg = scraper_root.get("sheepit", {})
    output_cfg = scraper_root.get("output", {})
    dashboard_cfg = scraper_root.get("dashboard", {})

    config = {
        "sheepit": {
            "username": sheepit_cfg.get(
                "username", defaults["scraper"]["sheepit"]["username"]
            ),
            "password": sheepit_cfg.get(
                "password", defaults["scraper"]["sheepit"]["password"]
            ),
            "login_url": sheepit_cfg.get(
                "login_url", defaults["scraper"]["sheepit"]["login_url"]
            ),
            "team_url": sheepit_cfg.get(
                "team_url", defaults["scraper"]["sheepit"]["team_url"]
            ),
            "teams_points_url": sheepit_cfg.get(
                "teams_points_url", defaults["scraper"]["sheepit"]["teams_points_url"]
            ),
        },
        "output": {
            "team_info_folder": output_cfg.get(
                "team_info_folder", defaults["scraper"]["output"]["team_info_folder"]
            ),
            "teams_points_folder": output_cfg.get(
                "teams_points_folder",
                defaults["scraper"]["output"]["teams_points_folder"],
            ),
        },
        "dashboard": {
            "team_probation_url": dashboard_cfg.get(
                "team_probation_url",
                defaults["scraper"]["dashboard"]["team_probation_url"],
            ),
            "trigger_notifications": _coerce_bool(
                dashboard_cfg.get(
                    "trigger_notifications",
                    defaults["scraper"]["dashboard"]["trigger_notifications"],
                ),
                default=defaults["scraper"]["dashboard"]["trigger_notifications"],
            ),
        },
        "scraper": {
            "request_timeout_seconds": int(
                general_cfg.get(
                    "request_timeout_seconds",
                    defaults["scraper"]["general"]["request_timeout_seconds"],
                )
            ),
            "teams_rank_limit": int(
                general_cfg.get(
                    "teams_rank_limit",
                    defaults["scraper"]["general"]["teams_rank_limit"],
                )
            ),
        },
    }

    return config, path


CONFIG, CONFIG_PATH = _load_scraper_config()

SCRAPED_TEAM_INFO_FOLDER = CONFIG["output"]["team_info_folder"]
SCRAPED_TEAMS_POINTS_FOLDER = CONFIG["output"]["teams_points_folder"]

LOGIN_URL = CONFIG["sheepit"]["login_url"]
TEAM_URL = CONFIG["sheepit"]["team_url"]
TEAMS_POINTS_URL = CONFIG["sheepit"]["teams_points_url"]
TEAM_PROBATION_URL = CONFIG["dashboard"]["team_probation_url"]

USERNAME = CONFIG["sheepit"]["username"]
PASSWORD = CONFIG["sheepit"]["password"]

REQUEST_TIMEOUT_SECONDS = CONFIG["scraper"]["request_timeout_seconds"]
TEAMS_RANK_LIMIT = CONFIG["scraper"]["teams_rank_limit"]
TRIGGER_NOTIFICATIONS = CONFIG["dashboard"]["trigger_notifications"]


def name_to_color(name):
    """Generate a consistent color for a team member name"""
    hash_object = hashlib.md5(name.encode())
    return "#" + hash_object.hexdigest()[:6]


def ensure_output_folder():
    """Create the output folder if it doesn't exist"""
    if not os.path.exists(SCRAPED_TEAM_INFO_FOLDER):
        os.makedirs(SCRAPED_TEAM_INFO_FOLDER)
    if not os.path.exists(SCRAPED_TEAMS_POINTS_FOLDER):
        os.makedirs(SCRAPED_TEAMS_POINTS_FOLDER)


def scrape_teams_points():
    """Scrape aggregate teams points table (rankings) from SheepIt /team page.
    Output structure per row: Rank, Name, 90_days, 180_days, total_points, members."""
    payload = {"login": USERNAME, "password": PASSWORD}
    try:
        with requests.session() as session:
            login_response = session.post(
                LOGIN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS
            )
            if login_response.status_code != 200:
                print(
                    f"Error: Login failed while scraping team rankings (status {login_response.status_code})"
                )
                return None
            resp = session.get(TEAMS_POINTS_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code != 200:
                print(
                    f"Error: Failed to fetch teams rankings page (status {resp.status_code})"
                )
                return None
            soup = BeautifulSoup(resp.content, "html.parser")
            table = soup.find("table")
            if not table:
                print("Error: Could not find teams rankings table on page.")
                return None
            rows = table.find_all("tr")[1:]  # skip header
            extracted = []

            def parse_int(cell):
                if not cell:
                    return 0
                txt = cell.get_text(" ", strip=True)
                digits = re.sub(r"[^0-9]", "", txt)
                return int(digits) if digits else 0

            def get_data_sort_int(td):
                if td is None:
                    return 0
                raw = td.get("data-sort")
                if raw:
                    try:
                        # Keep only digits and optional decimal point then take integer part
                        cleaned = re.sub(r"[^0-9.]", "", raw)
                        if cleaned:
                            return int(float(cleaned))
                    except Exception:
                        pass
                return parse_int(td)

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue
                raw_rank = parse_int(cols[0])
                if raw_rank == 0 or raw_rank > TEAMS_RANK_LIMIT:
                    # Skip empty rank rows / stop after rank limit
                    if raw_rank > TEAMS_RANK_LIMIT:
                        break
                    continue
                name = cols[1].get_text(strip=True)
                ninety = get_data_sort_int(cols[2])
                one_eighty = get_data_sort_int(cols[3])
                total_pts = get_data_sort_int(cols[4])
                members = parse_int(cols[5])
                extracted.append(
                    {
                        "rank": raw_rank,
                        "name": name,
                        "90_days": ninety,
                        "180_days": one_eighty,
                        "total_points": total_pts,
                        "members": members,
                    }
                )
            return extracted
    except requests.RequestException as e:
        print(f"Error: Network error while scraping team rankings: {e}")
        return None
    except Exception as e:
        print(f"Error: Unexpected error while scraping team rankings: {e}")
        return None


def save_teams_points_to_csv(teams_points):
    """Save teams points ranking data to CSV in SCRAPED_TEAMS_POINTS_FOLDER.
    Columns: Date, Rank, Name, 90_days, 180_days, total_points, members"""
    if not teams_points:
        return None
    ensure_output_folder()
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    filename = f"sheepit_teams_points_{date_str}.csv"
    path = os.path.join(SCRAPED_TEAMS_POINTS_FOLDER, filename)
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Date",
                    "Rank",
                    "Name",
                    "90_days",
                    "180_days",
                    "total_points",
                    "members",
                ]
            )
            for row in teams_points:
                writer.writerow(
                    [
                        date_str,
                        row["rank"],
                        row["name"],
                        row["90_days"],
                        row["180_days"],
                        row["total_points"],
                        row["members"],
                    ]
                )
        return path
    except Exception as e:
        print(f"Error: Failed to save teams rankings CSV: {e}")
        return None


def scrape_team_data():
    """Scrape team data from SheepIt renderfarm"""
    # Login payload
    payload = {
        "login": USERNAME,
        "password": PASSWORD,
    }

    try:
        print("Logging into SheepIt")
        with requests.session() as session:
            login_response = session.post(
                LOGIN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS
            )

            if login_response.status_code != 200:
                print(f"Error: Login failed (status {login_response.status_code})")
                return None

            print("Fetching team data")
            team_response = session.get(TEAM_URL, timeout=REQUEST_TIMEOUT_SECONDS)

            if team_response.status_code != 200:
                print(
                    f"Error: Failed to fetch team page (status {team_response.status_code})"
                )
                return None

            # Parse HTML
            soup = BeautifulSoup(team_response.content, "html.parser")
            table = soup.find("table")

            if not table:
                print("Error: Could not find team table on the page.")
                return None

            # Extract data from table
            rows = table.find_all("tr")[1:]  # Skip header
            team_data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                rank = cols[0].get_text(strip=True)
                member_name = cols[1].get_text(strip=True)
                points_text = cols[2].get_text(strip=True).replace(",", "")
                joined_date_text = cols[3].get_text(strip=True)
                color = name_to_color(member_name)

                try:
                    points = int(points_text)
                except ValueError:
                    points = 0

                team_data.append(
                    {
                        "rank": rank,
                        "name": member_name,
                        "points": points,
                        "joined_date": joined_date_text,
                        "color": color,
                    }
                )

            print("Successfully scraped data")
            return team_data

    except requests.RequestException as e:
        print(f"Error: Network error while scraping team data: {e}")
        return None
    except Exception as e:
        print(f"Error: Unexpected error while scraping team data: {e}")
        return None


def save_team_data_to_csv(team_data):
    """Save team data to CSV file in the Scraped_Team_Info folder"""
    if not team_data:
        return None

    # Ensure output folder exists
    ensure_output_folder()

    # Generate filename with current date
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d")
    csv_filename = f"sheepit_team_points_{timestamp_str}.csv"
    csv_filepath = os.path.join(SCRAPED_TEAM_INFO_FOLDER, csv_filename)

    try:
        with open(csv_filepath, "w", newline="", encoding="utf-8") as csvfile:
            # Use the full format that matches existing files for probation tracking
            writer = csv.writer(csvfile)
            writer.writerow(["Date", "Rank", "Member", "Points", "Joined Date"])

            for entry in team_data:
                writer.writerow(
                    [
                        timestamp_str,  # Date
                        entry["rank"],  # Rank
                        entry["name"],  # Member
                        entry["points"],  # Points
                        entry["joined_date"],  # Joined Date
                    ]
                )

        return csv_filepath

    except Exception as e:
        print(f"Error: Failed to save team CSV: {e}")
        return None


def trigger_notifications():
    """Trigger notification processing by calling the probation data endpoint"""
    if not TEAM_PROBATION_URL:
        print("Warning: team_probation_url is not configured in config.toml")
        return False

    try:
        response = requests.get(TEAM_PROBATION_URL, timeout=REQUEST_TIMEOUT_SECONDS)

        if response.status_code == 200:
            return True
        else:
            print(
                f"Warning: Notification endpoint responded with status {response.status_code}"
            )
            return False

    except requests.RequestException as e:
        print(f"Error: Failed to trigger notifications: {e}")
        return False
    except Exception as e:
        print(f"Error: Unexpected error while triggering notifications: {e}")
        return False


def main():
    """Main function to run the scraper"""
    print(_format_run_timestamp())
    print("=" * 50)

    # Check credentials
    if USERNAME == "your_username_here" or PASSWORD == "your_password_here":
        print("Error: Missing SheepIt credentials in config.toml")
        print("=" * 50)
        return

    # Scrape team member points page
    team_data = scrape_team_data()
    if team_data:
        members_csv_path = save_team_data_to_csv(team_data)
    else:
        members_csv_path = None

    # Scrape teams rankings page
    teams_points = scrape_teams_points()
    if teams_points:
        rankings_csv_path = save_teams_points_to_csv(teams_points)
    else:
        rankings_csv_path = None

    success_any = bool(members_csv_path or rankings_csv_path)
    if success_any:
        # Slight delay then trigger dashboard refresh (only once)
        if TRIGGER_NOTIFICATIONS:
            time.sleep(2)
            trigger_notifications()
    else:
        print("Error: No data scraped successfully")

    print("=" * 50)


if __name__ == "__main__":
    main()
