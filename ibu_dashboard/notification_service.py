import json
import os
import smtplib
import ssl
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx


class NotificationService:
    """
    Service for sending email notifications about member probation status changes
    """

    def __init__(self):
        # Email configuration - you can set these as environment variables
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL", "")
        self.sender_password = os.getenv("SENDER_PASSWORD", "")
        # Recipients configuration - managed via JSON file (not .env)
        self.admin_emails_file = os.getenv(
            "ADMIN_EMAILS_FILE", os.path.join("config", "admin_emails.json")
        )
        # recipients stored as list of {"email": str, "prefs": {"failed": bool, "passed": bool, "non_compliant": bool}}
        self.admin_recipients = self.load_admin_emails()

        # Notification settings
        notifications_path = Path("notification_history/history.json")
        notifications_path.parent.mkdir(parents=True, exist_ok=True)

        self.notifications_file = str(notifications_path)
        self.notification_history = self.load_notification_history()

        # CSV tracking to prevent duplicate notifications
        self.last_processed_csv = self.notification_history.get(
            "last_processed_csv", ""
        )
        self.last_notification_date = self.notification_history.get(
            "last_notification_date", ""
        )

        # Discord webhook configuration (optional)
        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        # Allow explicit enable/disable via env; default to enabled when URL provided
        self.discord_enabled = os.getenv(
            "DISCORD_NOTIFICATIONS_ENABLED", "auto"
        ).lower()
        if self.discord_enabled not in ("true", "false"):
            self.discord_enabled = "true" if self.discord_webhook_url else "false"
        # Optional webhook profile customization (match email_to_discord.py)
        self.discord_username = os.getenv("DISCORD_WEBHOOK_USERNAME", "").strip()
        self.discord_avatar_url = os.getenv("DISCORD_WEBHOOK_AVATAR_URL", "").strip()

    @property
    def admin_emails(self) -> List[str]:
        """Backward-compatible list of just email strings for existing APIs/UI."""
        return self.get_all_emails()

    def _default_prefs(self) -> Dict[str, bool]:
        return {"failed": True, "passed": True, "non_compliant": True}

    def _normalize_recipients(self, data: Any) -> List[Dict[str, Any]]:
        """Normalize raw JSON data into list of recipient dicts with prefs."""
        recipients: List[Dict[str, Any]] = []
        if isinstance(data, dict) and "admin_emails" in data:
            data = data.get("admin_emails")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    email = item.strip()
                    if email:
                        recipients.append(
                            {"email": email, "prefs": self._default_prefs().copy()}
                        )
                elif isinstance(item, dict):
                    email = str(item.get("email", "")).strip()
                    if not email:
                        continue
                    prefs = item.get("prefs") or {}
                    norm_prefs = self._default_prefs()
                    for k in list(norm_prefs.keys()):
                        if k in prefs:
                            norm_prefs[k] = bool(prefs[k])
                    recipients.append({"email": email, "prefs": norm_prefs})
        return recipients

    def load_admin_emails(self) -> List[Dict[str, Any]]:
        """Load admin recipients with preferences from JSON; backward compatible with list-of-strings."""
        try:
            if os.path.exists(self.admin_emails_file):
                with open(self.admin_emails_file, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                    recips = self._normalize_recipients(raw)
                    return recips
            return []
        except Exception as e:
            print(f"Error loading admin emails: {e}")
            return []

    def save_admin_emails(self, recipients: Optional[List[Dict[str, Any]]]) -> bool:
        """Save admin recipients (with prefs) to JSON file. Ensures folder exists."""
        try:
            recips = []
            for r in recipients or []:
                email = str(r.get("email", "")).strip()
                if not email:
                    continue
                prefs = r.get("prefs") or {}
                # normalize prefs
                norm = self._default_prefs()
                for k in norm.keys():
                    if k in prefs:
                        norm[k] = bool(prefs[k])
                recips.append({"email": email, "prefs": norm})
            # Ensure folder exists
            d = os.path.dirname(self.admin_emails_file)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            tmp = self.admin_emails_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(recips, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.admin_emails_file)
            self.admin_recipients = recips
            return True
        except Exception as e:
            print(f"Error saving admin emails: {e}")
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            return False

    def get_all_emails(self) -> List[str]:
        return [r.get("email") for r in (self.admin_recipients or []) if r.get("email")]

    def add_admin_emails(self, emails):
        """Add one or more emails (string or list). Defaults prefs to all True. Returns bool."""
        curr = {r["email"]: r for r in (self.admin_recipients or []) if r.get("email")}
        to_add = emails if isinstance(emails, list) else [emails]
        for e in to_add:
            if isinstance(e, dict):
                email = str(e.get("email", "")).strip()
                prefs = e.get("prefs") or {}
            else:
                email = str(e).strip()
                prefs = {}
            if not email:
                continue
            if email in curr:
                # merge prefs if provided
                if prefs:
                    merged = curr[email]["prefs"]
                    for k, v in prefs.items():
                        if k in merged:
                            merged[k] = bool(v)
            else:
                norm = self._default_prefs()
                for k in list(norm.keys()):
                    if k in prefs:
                        norm[k] = bool(prefs[k])
                curr[email] = {"email": email, "prefs": norm}
        return self.save_admin_emails(list(curr.values()))

    def remove_admin_emails(self, emails):
        """Remove one or more emails from the list."""
        curr = {r["email"]: r for r in (self.admin_recipients or []) if r.get("email")}
        for e in emails if isinstance(emails, list) else [emails]:
            s = str(e).strip()
            if s in curr:
                del curr[s]
        return self.save_admin_emails(list(curr.values()))

    def replace_admin_emails(self, emails):
        """Replace entire list with provided emails (strings or objects)."""
        if isinstance(emails, list):
            # normalize list
            return self.save_admin_emails(self._normalize_recipients(emails))
        return False

    def update_admin_email_prefs(self, email: str, prefs: Dict[str, bool]) -> bool:
        """Update preference flags for a given email."""
        email = str(email or "").strip()
        if not email:
            return False
        found = False
        for r in self.admin_recipients or []:
            if r.get("email") == email:
                for k in ["failed", "passed", "non_compliant"]:
                    if k in prefs:
                        r["prefs"][k] = bool(prefs[k])
                found = True
                break
        if not found:
            # Add with provided prefs merged with defaults
            norm = self._default_prefs()
            for k in list(norm.keys()):
                if k in prefs:
                    norm[k] = bool(prefs[k])
            self.admin_recipients.append({"email": email, "prefs": norm})
        return self.save_admin_emails(self.admin_recipients)

    def get_recipients_for(self, event_type: str) -> List[str]:
        """Return list of email strings that opted into the given event_type.
        event_type in {'failed','passed','non_compliant'}
        """
        valid = {"failed", "passed", "non_compliant"}
        et = event_type if event_type in valid else None
        emails: List[str] = []
        if not et:
            return emails
        for r in self.admin_recipients or []:
            email = r.get("email")
            prefs = r.get("prefs") or {}
            if email and prefs.get(et, True):
                emails.append(email)
        return emails

    def load_notification_history(self) -> Dict:
        """Load notification history to avoid duplicate notifications"""
        try:
            if os.path.exists(self.notifications_file):
                with open(self.notifications_file, "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading notification history: {e}")
        return {}

    def save_notification_history(self):
        """Save notification history"""
        try:
            # Update tracking info before saving
            self.notification_history["last_processed_csv"] = self.last_processed_csv
            self.notification_history["last_notification_date"] = (
                self.last_notification_date
            )

            with open(self.notifications_file, "w") as f:
                json.dump(self.notification_history, f, indent=2)
        except Exception as e:
            print(f"Error saving notification history: {e}")

    def should_check_for_notifications(self, current_csv_file: str) -> bool:
        """
        Determine if we should check for notifications based on CSV file and date
        Only check if:
        1. New CSV file detected, OR
        2. Same CSV but different day (in case CSV is updated)
        """
        from datetime import datetime

        current_date = datetime.now().strftime("%Y-%m-%d")

        # Extract just the filename from the full path for comparison
        current_csv_name = (
            os.path.basename(current_csv_file) if current_csv_file else ""
        )
        last_csv_name = (
            os.path.basename(self.last_processed_csv) if self.last_processed_csv else ""
        )

        # Check if this is a new CSV file or a new day
        if current_csv_name != last_csv_name:
            print(
                f"📊 New CSV detected: {current_csv_name} (previous: {last_csv_name})"
            )
            return True
        elif current_date != self.last_notification_date:
            print(
                f"📅 Same CSV but new day: {current_date} (last notification: {self.last_notification_date})"
            )
            return True

        print(
            f"⏭️ Skipping notification check - already processed {current_csv_name} today ({current_date})"
        )
        return False

    def update_csv_tracking(self, csv_file: str):
        """Update the tracking info for the last processed CSV"""
        from datetime import datetime

        self.last_processed_csv = csv_file
        self.last_notification_date = datetime.now().strftime("%Y-%m-%d")
        self.save_notification_history()

        print(
            f"📝 Updated CSV tracking: {os.path.basename(csv_file)} on {self.last_notification_date}"
        )

    def has_been_notified(self, member_name: str, status: str) -> bool:
        """Check if we've already sent a notification for this member and status"""
        key = f"{member_name}_{status}_{self.last_notification_date}"
        return key in self.notification_history

    # def mark_as_notified(self, member_name: str, status: str):
    # """Mark that we've sent a notification for this member and status"""
    # key = f"{member_name}_{status}"
    # self.notification_history[key] = {
    # "timestamp": datetime.now().isoformat(),
    # "member": member_name,
    # "status": status
    # }
    # self.save_notification_history()

    def create_failure_email(self, member_data: Dict) -> tuple:
        """Create email content for probation failure notification"""
        subject = (
            f"🚨 PROBATION FAILURE ALERT: {member_data.get('name', 'Unknown Member')}"
        )

        # Safely get member data with defaults
        member_name = member_data.get("name", "Unknown Member")
        joined_date = member_data.get("joined_date", "Unknown")
        days_since_joined = member_data.get("days_since_joined", "Unknown")
        current_points = member_data.get("current_points", 0)

        # Ensure current_points is a number for formatting
        if current_points is None:
            current_points = 0

        # Determine which milestone(s) failed
        failed_milestones = []
        milestones = member_data.get("milestones", {})

        if not milestones.get("week_1", {}).get("passed"):
            failed_milestones.append("First Week (250K points)")
        if not milestones.get("month_1", {}).get("passed"):
            failed_milestones.append("First Month (1M points)")
        if not milestones.get("month_3", {}).get("passed"):
            failed_milestones.append("Three Months (3M points)")

        failed_text = (
            ", ".join(failed_milestones) if failed_milestones else "Unknown milestone"
        )

        # Create HTML email content
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #e06150, #d14a3a); color: white; padding: 20px; border-radius: 10px 10px 0 0; text-align: center; }}
                .content {{ padding: 20px; }}
                .alert {{ background-color: #fef2f2; border-left: 4px solid #ef4444; padding: 15px; margin: 15px 0; border-radius: 4px; }}
                .details {{ background-color: #f9fafb; padding: 15px; border-radius: 8px; margin: 15px 0; }}
                .milestone {{ padding: 10px; margin: 5px 0; border-radius: 6px; }}
                .milestone.failed {{ background-color: #fef2f2; border-left: 3px solid #ef4444; }}
                .milestone.passed {{ background-color: #f0fdf4; border-left: 3px solid #22c55e; }}
                .milestone.pending {{ background-color: #fefce8; border-left: 3px solid #eab308; }}
                .footer {{ background-color: #f9fafb; padding: 15px; border-radius: 0 0 10px 10px; text-align: center; font-size: 12px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚨 Probation Failure Alert</h1>
                    <p>Immediate attention required</p>
                </div>

                <div class="content">
                    <div class="alert">
                        <h2>Member: {member_name}</h2>
                        <p><strong>Status:</strong> FAILED PROBATION</p>
                        <p><strong>Failed Milestone(s):</strong> {failed_text}</p>
                    </div>

                    <div class="details">
                        <h3>Member Details</h3>
                        <p><strong>Joined Date:</strong> {joined_date}</p>
                        <p><strong>Days Since Joining:</strong> {days_since_joined}</p>
                        <p><strong>Current Points:</strong> {current_points:,}</p>
                        <p><strong>Notification Time:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                    </div>

                    <h3>Milestone Status</h3>
        """

        # Add milestone details
        milestone_order = [
            ("week_1", "First Week", "250K points"),
            ("month_1", "First Month", "1M points"),
            ("month_3", "Three Months", "3M points"),
        ]

        for key, title, target in milestone_order:
            milestone = milestones.get(key, {})
            passed = milestone.get("passed")
            points_at_deadline = milestone.get("points_at_deadline", current_points)

            # Ensure points_at_deadline is a number
            if points_at_deadline is None:
                points_at_deadline = current_points

            if passed:
                status_class = "passed"
                status_text = "✅ PASSED"
            elif not passed:
                status_class = "failed"
                status_text = "❌ FAILED"
            else:
                status_class = "pending"
                status_text = "⏳ IN PROGRESS"

            html_content += f"""
                    <div class="milestone {status_class}">
                        <strong>{title}</strong> - {status_text}<br>
                        Target: {target} | Achieved: {points_at_deadline:,} points
                    </div>
            """

        html_content += f"""

                <div class="footer">
                    <p>This is an automated notification from the I.B.U Team Dashboard</p>
                    <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p>
                </div>
            </div>
        </body>
        </html>
        """

        # Create plain text version
        text_content = f"""
PROBATION FAILURE ALERT - {member_name}

URGENT: Member {member_name} has FAILED probation requirements.

Member Details:
- Name: {member_name}
- Joined: {joined_date}
- Days Since Joining: {days_since_joined}
- Current Points: {current_points:,}
- Failed Milestone(s): {failed_text}

Milestone Status:
"""

        for key, title, target in milestone_order:
            milestone = milestones.get(key, {})
            passed = milestone.get("passed")
            points_at_deadline = milestone.get("points_at_deadline", current_points)

            # Ensure points_at_deadline is a number
            if points_at_deadline is None:
                points_at_deadline = current_points

            status_text = (
                "PASSED" if passed else "FAILED" if not passed else "IN PROGRESS"
            )
            text_content += f"- {title}: {status_text} (Target: {target}, Achieved: {points_at_deadline:,})\n"

        text_content += f"""
Required Actions:
- Review member performance data
- Contact member for performance discussion
- Consider probation extension or termination
- Update member status in team management system

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

        return subject, html_content, text_content

    def create_passed_email(self, member_data: Dict) -> tuple:
        """Create email content for probation pass notification"""
        subject = f"🎉 PROBATION PASSED: {member_data.get('name', 'Unknown Member')}"

        # Safely get member data with defaults
        member_name = member_data.get("name", "Unknown Member")
        joined_date = member_data.get("joined_date", "Unknown")
        days_since_joined = member_data.get("days_since_joined", "Unknown")
        current_points = member_data.get("current_points", 0)

        # Ensure current_points is a number for formatting
        if current_points is None:
            current_points = 0

        # Determine which milestone(s) failed
        failed_milestones = []
        milestones = member_data.get("milestones", {})

        if not milestones.get("week_1", {}).get("passed"):
            failed_milestones.append("First Week (250K points)")
        if not milestones.get("month_1", {}).get("passed"):
            failed_milestones.append("First Month (1M points)")
        if not milestones.get("month_3", {}).get("passed"):
            failed_milestones.append("Three Months (3M points)")

        # Create HTML email content
        html_content = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                    .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                    .header {{ background: linear-gradient(135deg, #5ae050, #56d13a); color: white; padding: 20px; border-radius: 10px 10px 0 0; text-align: center; }}
                    .content {{ padding: 20px; }}
                    .alert {{ background-color: #f0fdf4; border-left: 4px solid #22c55e; padding: 15px; margin: 15px 0; border-radius: 4px; }}
                    .details {{ background-color: #f9fafb; padding: 15px; border-radius: 8px; margin: 15px 0; }}
                    .milestone {{ padding: 10px; margin: 5px 0; border-radius: 6px; }}
                    .milestone.failed {{ background-color: #fef2f2; border-left: 3px solid #ef4444; }}
                    .milestone.passed {{ background-color: #f0fdf4; border-left: 3px solid #22c55e; }}
                    .milestone.pending {{ background-color: #fefce8; border-left: 3px solid #eab308; }}
                    .footer {{ background-color: #f9fafb; padding: 15px; border-radius: 0 0 10px 10px; text-align: center; font-size: 12px; color: #6b7280; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🎉 Probation Passed</h1>
                        <p>Congratulations IBU Team, a member has passed their probation!</p>
                    </div>

                    <div class="content">
                        <div class="alert">
                            <h2>Member: {member_name}</h2>
                            <p><strong>Status:</strong> PASSED PROBATION</p>
                        </div>

                        <div class="details">
                            <h3>Member Details</h3>
                            <p><strong>Joined Date:</strong> {joined_date}</p>
                            <p><strong>Days Since Joining:</strong> {days_since_joined}</p>
                            <p><strong>Current Points:</strong> {current_points:,}</p>
                            <p><strong>Notification Time:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                        </div>

                        <h3>Milestone Status</h3>
            """

        # Add milestone details
        milestone_order = [
            ("week_1", "First Week", "250K points"),
            ("month_1", "First Month", "1M points"),
            ("month_3", "Three Months", "3M points"),
        ]

        for key, title, target in milestone_order:
            milestone = milestones.get(key, {})
            passed = milestone.get("passed")
            points_at_deadline = milestone.get("points_at_deadline", current_points)

            # Ensure points_at_deadline is a number
            if points_at_deadline is None:
                points_at_deadline = current_points

            if passed:
                status_class = "passed"
                status_text = "✅ PASSED"
            elif not passed:
                status_class = "failed"
                status_text = "❌ FAILED"
            else:
                status_class = "pending"
                status_text = "⏳ IN PROGRESS"

            html_content += f"""
                        <div class="milestone {status_class}">
                            <strong>{title}</strong> - {status_text}<br>
                            Target: {target} | Achieved: {points_at_deadline:,} points
                        </div>
                """

        html_content += f"""

                    </div>

                    <div class="footer">
                        <p>This is an automated notification from the I.B.U Team Dashboard</p>
                        <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p>
                    </div>
                </div>
            </body>
            </html>
            """

        text_content = f"""
    PROBATION PASSED - {member_name}

    Member Details:
    - Name: {member_name}
    - Joined: {joined_date}
    - Days Since Joining: {days_since_joined}
    - Current Points: {current_points:,}

    Congratulations! This member has successfully passed probation.

    Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """
        return subject, html_content, text_content

    def create_non_compliant_email(self, member_data: Dict) -> tuple:
        """Create email content for post-probation non-compliance notification"""
        subject = f"⚠️ NON-COMPLIANT MEMBER: {member_data.get('name', 'Unknown Member')}"

        # Safely get member data with defaults
        member_name = member_data.get("name", "Unknown Member")
        joined_date = member_data.get("joined_date", "Unknown")
        current_points = member_data.get("current_points", 0)

        # Ensure current_points is a number for formatting
        if current_points is None:
            current_points = 0

        # Get most recent post-probation period performance
        periods = member_data.get("post_probation_periods", [])
        if periods:
            # Use the last completed period (not the current one)
            if len(periods) > 1:
                latest_period = periods[-2]
            else:
                latest_period = periods[-1]
            points_earned = latest_period.get("points_earned", "N/A")
            target_points = latest_period.get("target_points", "N/A")
            period_start = latest_period.get("start_date", "")
            period_end = latest_period.get("end_date", "")
        else:
            points_earned = "N/A"
            target_points = "N/A"
            period_start = ""
            period_end = ""

        recent_periods = periods[-3:] if periods else []

        # Create HTML email content
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #8f50e0, #853ad1); color: white; padding: 20px; border-radius: 10px 10px 0 0; text-align: center; }}
                .content {{ padding: 20px; }}
                .alert {{ background-color: #c0acef; border-left: 4px solid #7643ef; padding: 15px; margin: 15px 0; border-radius: 4px; }}
                .details {{ background-color: #f9fafb; padding: 15px; border-radius: 8px; margin: 15px 0; }}
                .milestone {{ padding: 10px; margin: 5px 0; border-radius: 6px; }}
                .milestone.failed {{ background-color: #fef2f2; border-left: 3px solid #ef4444; }}
                .milestone.passed {{ background-color: #f0fdf4; border-left: 3px solid #22c55e; }}
                .milestone.pending {{ background-color: #fefce8; border-left: 3px solid #eab308; }}
                .footer {{ background-color: #f9fafb; padding: 15px; border-radius: 0 0 10px 10px; text-align: center; font-size: 12px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>⚠️ Non Compliant</h1>
                    <p>Attention IBU Team, a member has been marked as non-compliant!</p>
                </div>

                <div class="content">
                    <div class="alert">
                        <h2>Member: {member_name}</h2>
                        <p><strong>Status:</strong> NON-COMPLIANT</p>
                    </div>

                    <div class="details">
                        <h3>Member Details</h3>
                        <p><strong>Joined Date:</strong> {joined_date}</p>
                        <p><strong>Current Points:</strong> {current_points:,}</p>
                        <p><strong>Most Recent 90-Day Period:</strong> {period_start} to {period_end}</p>
                        <p><strong>Points Earned:</strong> {points_earned:,}</p>
                        <p><strong>Notification Time:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                    </div>

                    <h3>Post Probation Status</h3>
        """

        # Add milestone details
        if recent_periods:
            for period in recent_periods:
                start_date = period.get("start_date", "")
                end_date = period.get("end_date", "")
                points_earned = period.get("points_earned", "N/A")
                target_points = period.get("target_points", "N/A")
                status = period.get("status", "Unknown")
                html_content += f"""
                <div style="margin-bottom:10px;">
                    <strong>{start_date} to {end_date}</strong><br>
                    Status: {status}<br>
                    Points: {points_earned:,} / {target_points:,}
                </div>
                """
        else:
            html_content += "<p>No post-probation period data available.</p>"

        html_content += """
                <p>This member is non-compliant in the post-probation phase.<br>
                <strong>Reason:</strong> Did not meet the required performance in the last period.</p>
            </div>
        </body>
        </html>
        """

        html_content += f"""

                </div>

                <div class="footer">
                    <p>This is an automated notification from the I.B.U Team Dashboard</p>
                    <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p>
                </div>
            </div>
        </body>
        </html>
        """

        text_content = f"""
NON-COMPLIANT MEMBER - {member_name}

Member Details:
- Name: {member_name}
- Current Points: {current_points:,}

This member is non-compliant in the post-probation phase. Please review and take necessary action.

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        return subject, html_content, text_content

    def send_email(
        self, to_emails: List[str], subject: str, html_content: str, text_content: str
    ) -> bool:
        """Send email notification"""
        if not self.sender_email or not self.sender_password:
            print("Email credentials not configured. Skipping email notification.")
            return False
        try:
            # Normalize recipients
            recipients = [
                e for e in (to_emails or []) if isinstance(e, str) and e.strip()
            ]
            if not recipients:
                print("No recipients provided. Skipping email notification.")
                return False

            # Build message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = formataddr(("IBU Assistant", self.sender_email))
            msg["To"] = ", ".join(recipients)

            part_text = MIMEText(text_content or "", "plain", _charset="utf-8")
            part_html = MIMEText(html_content or "", "html", _charset="utf-8")
            msg.attach(part_text)
            msg.attach(part_html)

            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipients, msg.as_string())
            return True
        except Exception as e:
            print(f"Error sending email: {e}")
            return False

    # ---------- Discord Webhook Support ---------------------------------------
    def _discord_post(self, payload: Dict) -> bool:
        """Low-level POST to Discord webhook with simple retry on 429/5xx."""
        if not self.discord_webhook_url or self.discord_enabled != "true":
            return False
        try:
            # Use a short timeout and a tiny retry for rate-limit/server hiccups
            with httpx.Client(timeout=8.0) as client:
                resp = client.post(self.discord_webhook_url, json=payload)
                # Basic retry on 429/5xx once
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = 1.0
                    try:
                        # If rate-limited, respect Retry-After seconds if present
                        ra = resp.headers.get("Retry-After")
                        if ra:
                            wait = float(ra)
                    except Exception:
                        pass
                    try:
                        threading.Event().wait(wait)
                    except Exception:
                        pass
                    resp = client.post(self.discord_webhook_url, json=payload)
                if 200 <= resp.status_code < 300:
                    return True
                print(
                    f"[Discord] webhook post failed: {resp.status_code} {resp.text[:200]}"
                )
                return False
        except Exception as e:
            print(f"[Discord] webhook error: {e}")
            return False

    def _build_discord_embed(
        self,
        title: str,
        description: str,
        color: int,
        fields: Optional[List[Dict]] = None,
    ) -> Dict:
        embed: Dict[str, Any] = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if fields:
            embed["fields"] = fields
        return embed

    def _send_discord_notification(
        self, event: str, member_data: Dict, subject: str, text_content: str
    ) -> bool:
        """Send a concise Discord message for member event (failed/passed/non_compliant)."""
        if self.discord_enabled != "true" or not self.discord_webhook_url:
            return False
        try:
            name = member_data.get("name", "Unknown Member")
            # Choose emoji/color per event
            if event == "failed":
                emoji = "🚨"
                color = 0xEF4444  # red-500
                title = f"Probation Failure: {name}"
            elif event == "passed":
                emoji = "🎉"
                color = 0x22C55E  # green-500
                title = f"Probation Passed: {name}"
            else:
                emoji = "⚠️"
                color = 0x8B5CF6  # violet-500
                title = f"Non-Compliant: {name}"

            # Build a short description (first lines of text content)
            summary = text_content.strip().splitlines()
            # Take first 6 non-empty lines as summary
            picked = []
            for line in summary:
                s = line.strip()
                if s:
                    picked.append(s)
                if len(picked) >= 6:
                    break
            description = "\n".join(picked)

            # Include a few key fields if available
            fields: List[Dict[str, Any]] = []
            # Member hyperlink to SheepIt profile
            if name and name != "Unknown Member":
                try:
                    display = str(name).replace("_", r"\_")
                except Exception:
                    display = str(name)
                profile_url = f"https://www.sheepit-renderfarm.com/user/{name}/profile"
                fields.append(
                    {
                        "name": "Member",
                        "value": f"[{display}]({profile_url})",
                        "inline": True,
                    }
                )
            joined = member_data.get("joined_date") or member_data.get(
                "joined_date_parsed"
            )
            if joined:
                fields.append({"name": "Joined", "value": str(joined), "inline": True})
            cp = member_data.get("current_points")
            if cp is not None:
                try:
                    fields.append(
                        {
                            "name": "Current Points",
                            "value": f"{int(cp):,}",
                            "inline": True,
                        }
                    )
                except Exception:
                    fields.append(
                        {"name": "Current Points", "value": str(cp), "inline": True}
                    )
            pp = member_data.get("post_probation_status")
            if pp:
                fields.append(
                    {"name": "Post-Probation", "value": str(pp), "inline": True}
                )

            embed = self._build_discord_embed(
                f"{emoji} {title}", description, color, fields
            )
            payload = {
                "content": None,  # no extra content, embed only for neatness
                "embeds": [embed],
            }
            # Apply optional profile overrides
            if self.discord_username:
                payload["username"] = self.discord_username
            if self.discord_avatar_url:
                payload["avatar_url"] = self.discord_avatar_url
            # Send asynchronously so we don't block email flow
            threading.Thread(
                target=self._discord_post, args=(payload,), daemon=True
            ).start()
            return True
        except Exception as e:
            print(f"[Discord] build/send error: {e}")
            return False

    def notify_probation_failure(self, member_data: Dict) -> bool:
        """Send probation failure notification"""
        member_name = member_data.get("name", "Unknown")
        # Check if we've already sent this notification
        if self.has_been_notified(member_name, "failed"):
            print(
                f"Already notified about {member_name} probation failure. Skipping duplicate."
            )
            return True
        # Create email content
        subject, html_content, text_content = self.create_failure_email(member_data)
        # Send email
        recipients = self.get_recipients_for("failed")
        success = self.send_email(recipients, subject, html_content, text_content)
        # Mirror to Discord (best-effort, independent of email success)
        self._send_discord_notification("failed", member_data, subject, text_content)
        # if success:
        #     self.mark_as_notified(member_name, "failed")
        return success

    def notify_probation_passed(self, member_data: Dict) -> bool:
        """Send probation passed notification"""
        member_name = member_data.get("name", "Unknown")
        if self.has_been_notified(member_name, "passed"):
            print(
                f"Already notified about {member_name} probation passed. Skipping duplicate."
            )
            return True
        subject, html_content, text_content = self.create_passed_email(member_data)
        recipients = self.get_recipients_for("passed")
        success = self.send_email(recipients, subject, html_content, text_content)
        self._send_discord_notification("passed", member_data, subject, text_content)
        # if success:
        #     self.mark_as_notified(member_name, "passed")
        return success

    def notify_non_compliant(self, member_data: Dict) -> bool:
        """Send non-compliant notification"""
        member_name = member_data.get("name", "Unknown")
        if self.has_been_notified(member_name, "non_compliant"):
            print(
                f"Already notified about {member_name} non-compliance. Skipping duplicate."
            )
            return True
        subject, html_content, text_content = self.create_non_compliant_email(
            member_data
        )
        recipients = self.get_recipients_for("non_compliant")
        success = self.send_email(recipients, subject, html_content, text_content)
        self._send_discord_notification(
            "non_compliant", member_data, subject, text_content
        )
        # if success:
        #     self.mark_as_notified(member_name, "non_compliant")
        return success

    def check_and_notify_failures(
        self, members_data: List[Dict], current_csv_file: str = None
    ):
        """
        Check all members for failures and send notifications
        Only sends notifications if this is a new CSV file or a new day
        """
        # If no CSV file provided, skip the smart checking
        if current_csv_file and not self.should_check_for_notifications(
            current_csv_file
        ):
            return

        failed_members = [
            m for m in members_data if m.get("probation_status") == "failed"
        ]
        non_compliant_members = [
            m for m in members_data if m.get("post_probation_status") == "non_compliant"
        ]
        # Only send 'passed' notification if member is not non-compliant
        passed_members = [
            m
            for m in members_data
            if m.get("probation_status") == "passed"
            and m.get("post_probation_status") != "non_compliant"
        ]

        # Add debug logging for all member types
        print(
            f"🔍 Processing: {len(failed_members)} failed, {len(passed_members)} passed, {len(non_compliant_members)} non-compliant"
        )
        print(
            f"🔍 Passed members: {[m.get('name', 'Unknown') for m in passed_members]}"
        )

        if not failed_members and not passed_members and not non_compliant_members:
            print("✅ No members requiring notifications found in current data")
            if current_csv_file:
                self.update_csv_tracking(current_csv_file)
            return

        # Process all member types
        print("📬 Processing notifications for all member types...")

        notifications_sent = 0

        # Process failed members
        if failed_members:
            print(
                f"🚨 Found {len(failed_members)} failed members. Sending notifications..."
            )
        for member in failed_members:
            try:
                member_name = member.get("name", "Unknown")
                notif_key = f"{member_name}"
                prev_entry = self.notification_history.get(notif_key)
                prev_status = prev_entry["status"] if prev_entry else None

                # Only send notification if status has changed
                if not prev_entry or prev_status != "failed":
                    success = self.notify_probation_failure(member)
                    if success:
                        notifications_sent += 1
                        print(f"🔔 Sent failed notification for {member_name}")
                else:
                    print(
                        f"⏭️ No status change for {member_name} (failed), not sending email."
                    )

                # Always update notification history to latest date/status
                self.notification_history[notif_key] = {
                    "timestamp": datetime.now().isoformat(),
                    "member": member_name,
                    "status": "failed",
                    "csv_file": os.path.basename(current_csv_file)
                    if current_csv_file
                    else "unknown",
                }
            except Exception as e:
                print(f"❌ Error notifying about {member.get('name', 'Unknown')}: {e}")

        # Process passed members
        if passed_members:
            print(
                f"🎉 Found {len(passed_members)} passed members. Sending notifications..."
            )
        for member in passed_members:
            try:
                member_name = member.get("name", "Unknown")
                notif_key = f"{member_name}"
                prev_entry = self.notification_history.get(notif_key)
                prev_status = prev_entry["status"] if prev_entry else None

                if not prev_entry or prev_status != "passed":
                    success = self.notify_probation_passed(member)
                    if success:
                        notifications_sent += 1
                        print(f"🔔 Sent passed notification for {member_name}")
                else:
                    print(
                        f"⏭️ No status change for {member_name} (passed), not sending email."
                    )

                self.notification_history[notif_key] = {
                    "timestamp": datetime.now().isoformat(),
                    "member": member_name,
                    "status": "passed",
                    "csv_file": os.path.basename(current_csv_file)
                    if current_csv_file
                    else "unknown",
                }
            except Exception as e:
                print(f"❌ Error notifying about {member.get('name', 'Unknown')}: {e}")

        # Process non-compliant members
        if non_compliant_members:
            print(
                f"⚠️ Found {len(non_compliant_members)} non-compliant members. Sending notifications..."
            )
        for member in non_compliant_members:
            try:
                member_name = member.get("name", "Unknown")
                notif_key = f"{member_name}"
                prev_entry = self.notification_history.get(notif_key)
                prev_status = prev_entry["status"] if prev_entry else None

                if not prev_entry or prev_status != "non_compliant":
                    success = self.notify_non_compliant(member)
                    if success:
                        notifications_sent += 1
                        print(f"🔔 Sent non-compliant notification for {member_name}")
                else:
                    print(
                        f"⏭️ No status change for {member_name} (non_compliant), not sending email."
                    )

                self.notification_history[notif_key] = {
                    "timestamp": datetime.now().isoformat(),
                    "member": member_name,
                    "status": "non_compliant",
                    "csv_file": os.path.basename(current_csv_file)
                    if current_csv_file
                    else "unknown",
                }
            except Exception as e:
                print(f"❌ Error notifying about {member.get('name', 'Unknown')}: {e}")

        if notifications_sent > 0:
            print(f"✅ Sent {notifications_sent} total notifications")
        else:
            print(
                "✅ No new notifications needed - all members already notified for current status"
            )

        # Update CSV tracking after processing
        if current_csv_file:
            self.update_csv_tracking(current_csv_file)


# Global notification service instance
notification_service = NotificationService()
