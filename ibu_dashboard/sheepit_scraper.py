import csv
import hashlib
import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SCRAPED_TEAM_INFO_FOLDER = os.getenv("DATA_FOLDER", "Scraped_Team_Info")
SCRAPED_TEAMS_POINTS_FOLDER = os.getenv(
    "SCRAPED_TEAMS_POINTS_FOLDER", "Scraped_Teams_Points"
)
# SheepIt URLs
LOGIN_URL = "https://www.sheepit-renderfarm.com/user/authenticate"
TEAM_URL = os.getenv("SHEEPIT_TEAM_URL", "https://www.sheepit-renderfarm.com/team/2109")
TEAMS_POINTS_URL = os.getenv(
    "SHEEPIT_TEAMS_POINTS_URL", "https://www.sheepit-renderfarm.com/team"
)
TEAM_PROBATION_URL = os.getenv("TEAM_PROBATION_URL", "")

# Login credentials from environment variables
USERNAME = os.getenv("SHEEPIT_USERNAME", "your_username_here")
PASSWORD = os.getenv("SHEEPIT_PASSWORD", "your_password_here")


def name_to_color(name):
    """Generate a consistent color for a team member name"""
    hash_object = hashlib.md5(name.encode())
    return "#" + hash_object.hexdigest()[:6]


def ensure_output_folder():
    """Create the output folder if it doesn't exist"""
    if not os.path.exists(SCRAPED_TEAM_INFO_FOLDER):
        os.makedirs(SCRAPED_TEAM_INFO_FOLDER)
        print(f"Created folder: {SCRAPED_TEAM_INFO_FOLDER}")
    if not os.path.exists(SCRAPED_TEAMS_POINTS_FOLDER):
        os.makedirs(SCRAPED_TEAMS_POINTS_FOLDER)
        print(f"Created folder: {SCRAPED_TEAMS_POINTS_FOLDER}")


def scrape_teams_points():
    """Scrape aggregate teams points table (rankings) from SheepIt /team page.
    Output structure per row: Rank, Name, 90_days, 180_days, total_points, members."""
    print("🔄 Starting SheepIt teams points scraping...")
    payload = {"login": USERNAME, "password": PASSWORD}
    try:
        with requests.session() as session:
            login_response = session.post(LOGIN_URL, data=payload)
            if login_response.status_code != 200:
                print(
                    f"❌ Login (teams points) failed with status code: {login_response.status_code}"
                )
                return None
            resp = session.get(TEAMS_POINTS_URL)
            if resp.status_code != 200:
                print(
                    f"❌ Failed to fetch teams points page. Status {resp.status_code}"
                )
                return None
            soup = BeautifulSoup(resp.content, "html.parser")
            table = soup.find("table")
            if not table:
                print("❌ Could not find teams points table on page.")
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
                if raw_rank == 0 or raw_rank > 150:
                    # Skip empty rank rows / stop after >150
                    if raw_rank > 150:
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
            print(f"✅ Scraped {len(extracted)} teams from rankings page")
            return extracted
    except requests.RequestException as e:
        print(f"❌ Network error during teams points scraping: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during teams points scraping: {e}")
        return None


def save_teams_points_to_csv(teams_points):
    """Save teams points ranking data to CSV in SCRAPED_TEAMS_POINTS_FOLDER.
    Columns: Date, Rank, Name, 90_days, 180_days, total_points, members"""
    if not teams_points:
        print("❌ No teams points data to save")
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
        print(f"💾 Saved teams points rankings to {path}")
        return path
    except Exception as e:
        print(f"❌ Error saving teams points CSV: {e}")
        return None


def scrape_team_data():
    """Scrape team data from SheepIt renderfarm"""
    print("🔄 Starting SheepIt team data scraping...")

    # Login payload
    payload = {
        "login": USERNAME,
        "password": PASSWORD,
    }

    try:
        # Start session and login
        print("🔑 Logging into SheepIt...")
        with requests.session() as session:
            login_response = session.post(LOGIN_URL, data=payload)

            if login_response.status_code != 200:
                print(f"❌ Login failed with status code: {login_response.status_code}")
                return None

            # Get team page
            print("📊 Fetching team data...")
            team_response = session.get(TEAM_URL)

            if team_response.status_code != 200:
                print(
                    f"❌ Failed to get team page. Status code: {team_response.status_code}"
                )
                return None

            # Parse HTML
            soup = BeautifulSoup(team_response.content, "html.parser")
            table = soup.find("table")

            if not table:
                print("❌ Could not find team table on the page.")
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

            print(f"✅ Successfully scraped data for {len(team_data)} team members")
            return team_data

    except requests.RequestException as e:
        print(f"❌ Network error during scraping: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during scraping: {e}")
        return None


def save_team_data_to_csv(team_data):
    """Save team data to CSV file in the Scraped_Team_Info folder"""
    if not team_data:
        print("❌ No team data to save")
        return None

    # Ensure output folder exists
    ensure_output_folder()

    # Generate filename with current date
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d")
    csv_filename = f"sheepit_team_points_{timestamp_str}.csv"
    csv_filepath = os.path.join(SCRAPED_TEAM_INFO_FOLDER, csv_filename)

    try:
        # Write CSV file
        print(f"💾 Saving data to: {csv_filepath}")
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

        print(f"✅ Successfully saved {len(team_data)} records to {csv_filename}")
        return csv_filepath

    except Exception as e:
        print(f"❌ Error saving CSV file: {e}")
        return None


def trigger_notifications():
    """Trigger notification processing by calling the probation data endpoint"""
    if not TEAM_PROBATION_URL:
        print("⚠️  IBU Dashboard URL not configured in .env file")
        return False

    try:
        print("📧 Triggering notification processing...")
        response = requests.get(TEAM_PROBATION_URL, timeout=60)

        if response.status_code == 200:
            print("✅ Successfully triggered notification processing")
            return True
        else:
            print(
                f"⚠️  Notification endpoint responded with status code: {response.status_code}"
            )
            return False

    except requests.RequestException as e:
        print(f"❌ Failed to trigger notifications: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error triggering notifications: {e}")
        return False


def main():
    """Main function to run the scraper"""
    print("🚀 SheepIt Team Data Scraper - Local Version")
    print("=" * 50)

    # Check credentials
    if USERNAME == "your_username_here" or PASSWORD == "your_password_here":
        print("⚠️  WARNING: Please set your SheepIt credentials!")
        print("You can either:")
        print("1. Set environment variables: SHEEPIT_USERNAME and SHEEPIT_PASSWORD")
        print("2. Edit this script and replace the placeholder values")
        print("\nExample using environment variables:")
        print(
            "Windows: set SHEEPIT_USERNAME=your_username && set SHEEPIT_PASSWORD=your_password"
        )
        print(
            "Linux/Mac: export SHEEPIT_USERNAME=your_username && export SHEEPIT_PASSWORD=your_password"
        )
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
        print("=" * 50)
        print("🎉 Scraping run summary")
        if members_csv_path:
            print(f"• Members file: {members_csv_path} ({len(team_data)} rows)")
        if rankings_csv_path:
            print(f"• Rankings file: {rankings_csv_path} ({len(teams_points)} rows)")
        print(
            "\n💡 The dashboard will automatically detect new member file(s) within 30 seconds."
        )
        # Slight delay then trigger dashboard refresh (only once)
        time.sleep(2)
        notification_success = trigger_notifications()
        if notification_success:
            print("✅ All processes completed successfully!")
        else:
            print("⚠️ Dashboard refresh request failed or not configured")
    else:
        print("❌ No data scraped successfully (members or rankings)")


if __name__ == "__main__":
    main()
