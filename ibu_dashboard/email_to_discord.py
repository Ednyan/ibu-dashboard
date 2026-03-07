import email
import html
import imaplib
import json
import os
import re
import time
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime

import requests
from dotenv import load_dotenv

# Resolve absolute paths relative to this file so multiple processes share the same files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "config", "email_to_discord_state.json")
LOCK_FILE = os.path.join(BASE_DIR, "config", "email_to_discord.lock")


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state: dict):
    # Ensure target directory exists
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir and not os.path.exists(state_dir):
        os.makedirs(state_dir, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _try_acquire_lock(lock_path: str, stale_seconds: int) -> bool:
    """Best-effort cross-process lock using atomic create of a lock file.
    If an existing lock is stale (mtime older than threshold), remove and retry once.
    Returns True if acquired, False otherwise.
    """
    _ensure_dir(lock_path)
    now = time.time()
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps({"pid": os.getpid(), "ts": now}))
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
        return True
    except FileExistsError:
        try:
            st = os.stat(lock_path)
            if (now - st.st_mtime) > max(30, stale_seconds):
                # Stale lock: remove and retry once
                os.remove(lock_path)
                # retry once
                return _try_acquire_lock(lock_path, stale_seconds)
        except Exception:
            # If we cannot stat/remove, assume locked
            pass
        return False


def _release_lock(lock_path: str):
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass
    except Exception:
        # Non-fatal
        pass


def decode_mime(s: str) -> str:
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return str(s)


def extract_text(msg: email.message.Message) -> str:
    """Extract readable text from an email message. Prefer text/plain; fallback to stripped HTML."""

    def strip_html(html_src: str) -> str:
        # Remove head, script, and style blocks entirely
        text = re.sub(r"(?is)<(head|script|style)[^>]*>.*?</\1>", "", html_src)
        # Remove HTML comments
        text = re.sub(r"(?is)<!--.*?-->", "", text)
        # If there's a body tag, focus only on its contents
        m = re.search(r"(?is)<body[^>]*>(.*?)</body>", text)
        if m:
            text = m.group(1)
        # Line breaks for common block-level endings
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(p|div|li|tr|td|th|h[1-6])>", "\n", text)
        # Strip all remaining tags
        text = re.sub(r"(?is)<[^>]+>", "", text)
        # Unescape HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r"\r\n|\r", "\n", text)
        text = re.sub(r"\u00A0", " ", text)  # non-breaking space
        text = re.sub(r"\t+", " ", text)
        text = re.sub(r"[ \f\v]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Trim surrounding whitespace
        return text.strip()

    def decode_part(part) -> str:
        """Decode a text/* part to unicode with fallbacks."""
        raw = part.get_payload(decode=True)
        if raw is None:
            # Might already be a string
            payload = part.get_payload()
            if isinstance(payload, str):
                return payload
            # As a last resort
            return ""
        # Try declared charset then common fallbacks
        charsets = []
        cs = part.get_content_charset()
        if cs:
            charsets.append(cs)
        charsets.extend(["utf-8", "latin-1"])  # fallbacks
        seen = set()
        for cs in [c for c in charsets if not (c in seen or seen.add(c))]:
            try:
                return raw.decode(cs, errors="replace")
            except Exception:
                continue
        try:
            return raw.decode(errors="replace")
        except Exception:
            return ""

    def normalize_ws(s: str) -> str:
        if not s:
            return ""
        s = re.sub(r"\r\n|\r", "\n", s)
        s = re.sub(r"\n{3,}", "\n\n", s)
        return s.strip()

    if msg.is_multipart():
        # Collect candidates
        plain_texts = []
        html_texts = []
        other_texts = []
        for part in msg.walk():
            ctype = part.get_content_type() or ""
            if part.is_multipart():
                continue
            disp = str(part.get("Content-Disposition", "")).lower()
            if "attachment" in disp:
                continue
            if ctype == "text/plain":
                t = decode_part(part)
                if t and t.strip():
                    plain_texts.append(t)
            elif ctype == "text/html":
                h = decode_part(part)
                if h and h.strip():
                    html_texts.append(h)
            elif ctype.startswith("text/"):
                t = decode_part(part)
                if t and t.strip():
                    other_texts.append(t)
        # Prefer non-empty plain text
        if plain_texts:
            # pick the longest non-empty
            return normalize_ws(max(plain_texts, key=len))
        # Then stripped HTML
        if html_texts:
            # choose the longest stripped result
            stripped = [strip_html(h) for h in html_texts]
            stripped = [s for s in stripped if s and s.strip()]
            if stripped:
                return normalize_ws(max(stripped, key=len))
        # Finally any other text/*
        if other_texts:
            return normalize_ws(max(other_texts, key=len))
    else:
        ctype = msg.get_content_type() or ""
        if ctype == "text/plain":
            return normalize_ws(decode_part(msg))
        if ctype == "text/html":
            return normalize_ws(strip_html(decode_part(msg)))
        if ctype.startswith("text/"):
            return normalize_ws(decode_part(msg))
    return ""


def split_for_discord(content: str, limit: int = 1900):
    """Split content into chunks under a character limit (default ~Discord content)."""
    chunks = []
    s = content or ""
    while len(s) > limit:
        cut = s.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(s[:cut])
        s = s[cut:].lstrip()
    if s:
        chunks.append(s)
    return chunks


def _hyperlink_username(body: str) -> str:
    """Replace all occurrences like 'User <username>' (with optional punctuation/quotes) with a masked link.
    Examples matched: 'User Rhine_JTG', 'User: "Rhine_JTG"', 'user - Rhine_JTG'.
    """
    if not body:
        return body
    # Pattern: 'User' + optional space/punct + optional quotes/brackets + username token
    pattern = re.compile(
        r"(?i)\bUser\b\s*[:\-–—]?\s*[\"'\(\[]?([A-Za-z0-9_.\-]+)[\"'\)\]]?", re.UNICODE
    )

    def repl(m: re.Match) -> str:
        uname = m.group(1)
        if not uname:
            return m.group(0)
        url = f"https://www.sheepit-renderfarm.com/user/{uname}/profile"
        # Escape underscores in display to avoid italic formatting
        display = uname.replace("_", r"\_")
        return f"User [{display}]({url})"

    return pattern.sub(repl, body)


def _truncate_body(body: str) -> str:
    """Truncate body right after the first ☺ emoji; also stop at a standalone '---' line if present."""
    if not body:
        return body
    cut_positions = []
    # Stop after first white smiling face emoji (U+263A)
    try:
        idx = body.index("☺")
        cut_positions.append(idx + 1)
    except ValueError:
        pass
    # Optional: stop before horizontal rule line '---' if present on its own line
    m = re.search(r"(?m)^---+$", body)
    if m:
        cut_positions.append(m.start())
    if not cut_positions:
        return body
    cut_at = min(cut_positions)
    return body[:cut_at].rstrip()


def send_to_discord(
    webhook_url: str, subject: str, sender: str, date_str: str, body: str
):
    header = f"**From:** {sender}\n**Subject:** {subject}\n**Date:** {date_str}\n"
    # Prepare body: hyperlink usernames and truncate at desired marker(s)
    body = _hyperlink_username(body)
    body = _truncate_body(body)

    # We'll send header as 'content' and body inside an embed description so masked links work.
    # Embed description limit is 4096 chars; use a safety margin.
    body_chunks = split_for_discord(body or "(no content)", limit=4000)
    debug = os.getenv("EMAIL_TO_DISCORD_DEBUG", "false").lower() == "true"
    # Optional per-message overrides for webhook display name and avatar
    wb_username = os.getenv("DISCORD_WEBHOOK_USERNAME", "").strip()
    wb_avatar = os.getenv("DISCORD_WEBHOOK_AVATAR_URL", "").strip()
    # Optional banner and embed color
    banner_url = os.getenv("DISCORD_BANNER_URL", "").strip()
    banner_enabled = os.getenv("DISCORD_BANNER_ENABLED", "false").lower() == "true"
    embed_color_hex = os.getenv("DISCORD_EMBED_COLOR", "").strip()
    embed_color = None
    if embed_color_hex:
        try:
            embed_color = int(embed_color_hex, 16)
        except ValueError:
            embed_color = None
    for i, ch in enumerate(body_chunks, 1):
        content = header if i == 1 else "(cont.)"
        embed = {"description": ch}
        if i == 1 and banner_enabled and banner_url:
            embed["image"] = {"url": banner_url}
        if embed_color is not None:
            embed["color"] = embed_color
        payload = {"content": content, "embeds": [embed]}
        if wb_username:
            payload["username"] = wb_username
        if wb_avatar:
            payload["avatar_url"] = wb_avatar
        if debug:
            print(
                f"[Email→Discord] Posting chunk {i}/{len(body_chunks)} (desc_len={len(ch)}) banner={'yes' if (i == 1 and banner_enabled and banner_url) else 'no'} color={'set' if embed_color is not None else 'none'}"
            )
        resp = requests.post(webhook_url, json=payload)
        # Basic 429 handling
        if resp.status_code == 429:
            try:
                retry = float(resp.json().get("retry_after", 1.0))
            except Exception:
                retry = (
                    float(resp.headers.get("Retry-After", 1))
                    if resp.headers.get("Retry-After")
                    else 1.0
                )
            time.sleep(retry)
            if debug:
                print(f"[Email→Discord] Retrying after 429 in {retry}s")
            resp = requests.post(webhook_url, json=payload)
        if debug:
            print(f"[Email→Discord] Discord status={resp.status_code}")
        if resp.status_code >= 300:
            raise RuntimeError(f"Discord webhook error {resp.status_code}: {resp.text}")
        time.sleep(0.3)  # gentle pacing


def match_filters(frm: str, subj: str, from_whitelist, subj_keywords):
    frm_l = (frm or "").lower()
    subj_l = (subj or "").lower()
    if from_whitelist:
        if not any(w.strip().lower() in frm_l for w in from_whitelist if w.strip()):
            return False
    if subj_keywords:
        if not any(k.strip().lower() in subj_l for k in subj_keywords if k.strip()):
            return False
    return True


def fetch_and_forward():
    """Fetch unread emails via IMAP and forward matching ones to Discord."""
    # Load .env from project root regardless of current working directory
    try:
        load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))
    except Exception:
        load_dotenv()

    imap_host = os.getenv("IMAP_HOST", "").strip()
    imap_user = os.getenv("IMAP_USER", "").strip()
    imap_pass = os.getenv("IMAP_PASS", "").strip()
    imap_folder = os.getenv("IMAP_FOLDER", "INBOX")
    discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    debug = os.getenv("EMAIL_TO_DISCORD_DEBUG", "false").lower() == "true"

    # Filters (comma-separated)
    from_whitelist = [
        x.strip() for x in os.getenv("FILTER_FROM", "").split(",") if x.strip()
    ]
    subj_keywords = [
        x.strip() for x in os.getenv("FILTER_SUBJECT", "").split(",") if x.strip()
    ]

    if not (imap_host and imap_user and imap_pass and discord_webhook):
        raise RuntimeError(
            "Missing IMAP_* or DISCORD_WEBHOOK_URL environment variables"
        )

    # Acquire a cross-process lock to avoid duplicate runs from multiple schedulers/processes
    stale_secs = int(os.getenv("EMAIL_TO_DISCORD_LOCK_STALE_SECONDS", "600"))
    lock_acquired = _try_acquire_lock(LOCK_FILE, stale_secs)
    if not lock_acquired:
        if debug:
            print("[Email→Discord] Lock held by another process; skipping this cycle")
        return

    state = load_state()
    last_uid = int(state.get("last_uid", 0))
    if debug:
        print(f"[Email→Discord] Using state last_uid={last_uid}")

    M = imaplib.IMAP4_SSL(imap_host)
    try:
        M.login(imap_user, imap_pass)
        M.select(imap_folder)
        # Fetch UNSEEN newer than last UID; if last_uid=0, fetch all UNSEEN
        typ, data = M.uid("search", None, "(UNSEEN)")
        if typ != "OK":
            raise RuntimeError("IMAP search failed")

        uids = [int(x) for x in (data[0].split() if data and data[0] else [])]
        uids = [u for u in uids if u > last_uid]
        uids.sort()
        if debug:
            print(
                f"[Email→Discord] Found {len(uids)} UNSEEN uids newer than {last_uid}: {uids[:10]}{'...' if len(uids) > 10 else ''}"
            )

        for uid in uids:
            # Use BODY.PEEK[] to avoid setting \Seen when fetching the message
            typ, msg_data = M.uid("fetch", str(uid), "(BODY.PEEK[])")
            if typ != "OK" or not msg_data or not msg_data[0]:
                last_uid = max(last_uid, uid)
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subj = decode_mime(msg.get("Subject"))
            frm = decode_mime(msg.get("From"))
            dt_hdr = msg.get("Date")
            try:
                dt = parsedate_to_datetime(dt_hdr) if dt_hdr else None
                date_str = dt.strftime("%Y-%m-%d %H:%M") if dt else (dt_hdr or "")
            except Exception:
                date_str = dt_hdr or ""

            if not match_filters(frm, subj, from_whitelist, subj_keywords):
                if debug:
                    print(
                        f"[Email→Discord] Skipping UID {uid} (filters not matched). From='{frm}', Subject='{subj}'"
                    )
                last_uid = max(last_uid, uid)
                continue

            body = extract_text(msg).strip()
            if debug:
                print(
                    f"[Email→Discord] Forwarding UID {uid}: From='{frm}', Subject='{subj}', body_len={len(body)}"
                )
            try:
                send_to_discord(discord_webhook, subj, frm, date_str, body)
                # Mark as seen only after successful Discord post
                try:
                    M.uid("store", str(uid), "+FLAGS", "(\\Seen)")
                except Exception as me:
                    if debug:
                        print(f"[Email→Discord] Failed to mark UID {uid} as Seen: {me}")
                last_uid = max(last_uid, uid)
            except Exception as post_err:
                # Do not mark as seen on failure to allow retry in next cycle
                if debug:
                    print(f"[Email→Discord] Post failed for UID {uid}: {post_err}")

        state["last_uid"] = last_uid
        save_state(state)
        if debug:
            print(f"[Email→Discord] Updated last_uid to {last_uid}")
    finally:
        try:
            M.logout()
        except Exception:
            pass
        # Always release the lock
        _release_lock(LOCK_FILE)


if __name__ == "__main__":
    fetch_and_forward()
