import os
import io
import re
import html
import json
import time
import queue
import logging
import threading
from datetime import datetime, timezone
from collections import OrderedDict
from flask import Flask, request, jsonify
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ==============================================================================
# ENVIRONMENT VARIABLES
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Target Telegram Channel ID (e.g. -1001234567890 or @channelusername)
ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID", "").strip()

# Required for Discord -> Telegram reverse sync (and fallback for 429 shared-IP blocks)
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "").strip()
DISCORD_POLL_INTERVAL = int(os.environ.get("DISCORD_POLL_INTERVAL", "30").strip() or "30")

# Optional customization & security
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
DISCORD_BOT_USERNAME = os.environ.get("DISCORD_BOT_USERNAME", "").strip()
DISCORD_BOT_AVATAR_URL = os.environ.get("DISCORD_BOT_AVATAR_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

DEFAULT_TELEGRAM_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/8/82/Telegram_logo.svg/512px-Telegram_logo.svg.png"
EMBED_COLOR = 0xFDFDFD
DISCORD_API_BASE = "https://discord.com/api/v10"
USER_AGENT = f"DiscordBot ({RENDER_EXTERNAL_URL or 'https://render.com'}, 1.0)"

# ==============================================================================
# DEDUPLICATION & STATE CACHES
# ==============================================================================
PROCESSED_MESSAGES = OrderedDict()        # (chat_id_str, tg_message_id) -> timestamp
PROCESSED_MEDIA_GROUPS = OrderedDict()    # (chat_id_str, media_group_id) -> timestamp
PROCESSED_DISCORD_IDS = OrderedDict()     # discord_message_id_str -> timestamp
MAX_CACHE_SIZE = 500

RUNTIME_TELEGRAM_CHAT_ID = ALLOWED_CHAT_ID
DISCORD_GUILD_ID = os.environ.get("DISCORD_GUILD_ID", "").strip()
LAST_DISCORD_MESSAGE_ID = None
POLLER_LOCK = threading.Lock()
WORKER_START_LOCK = threading.Lock()
WORKERS_STARTED_PID = None

# Background queue for outgoing Telegram -> Discord vouches to prevent burst 429s
DISCORD_SEND_QUEUE = queue.Queue()


def redact_secrets(text: str) -> str:
    """Masks Discord webhook tokens and Telegram bot tokens in log output."""
    if not text:
        return ""
    masked = re.sub(
        r"(https://discord(?:app)?\.com/api/webhooks/\d+/)[^\s\"']+",
        r"\1[REDACTED]",
        str(text),
    )
    if TELEGRAM_BOT_TOKEN:
        masked = masked.replace(TELEGRAM_BOT_TOKEN, "[REDACTED_TG_TOKEN]")
    return masked


def discord_headers() -> dict:
    return {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "User-Agent": USER_AGENT,
    }


def auto_discover_discord_channel_id():
    """
    If DISCORD_CHANNEL_ID was not manually set in Render, automatically fetches
    the channel_id from the DISCORD_WEBHOOK_URL metadata or via DISCORD_BOT_TOKEN.
    """
    global DISCORD_CHANNEL_ID, DISCORD_GUILD_ID
    if DISCORD_CHANNEL_ID and DISCORD_GUILD_ID:
        return

    # 1. Try extracting webhook_id from DISCORD_WEBHOOK_URL and querying with Bot Token
    m = re.search(r"discord(?:app)?\.com/api/webhooks/(\d+)/([^\s/?#]+)", DISCORD_WEBHOOK_URL)
    if m and DISCORD_BOT_TOKEN:
        webhook_id = m.group(1)
        try:
            r = requests.get(
                f"{DISCORD_API_BASE}/webhooks/{webhook_id}",
                headers=discord_headers(),
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                if not DISCORD_CHANNEL_ID and data.get("channel_id"):
                    DISCORD_CHANNEL_ID = str(data["channel_id"])
                if not DISCORD_GUILD_ID and data.get("guild_id"):
                    DISCORD_GUILD_ID = str(data["guild_id"])
                if DISCORD_CHANNEL_ID:
                    logger.info(
                        f"Auto-discovered DISCORD_CHANNEL_ID={DISCORD_CHANNEL_ID} "
                        f"(guild_id={DISCORD_GUILD_ID}) via Bot Token + Webhook ID."
                    )
                    return
        except Exception as exc:
            logger.warning(f"Webhook metadata lookup via Bot Token failed: {redact_secrets(str(exc))}")

    # 2. Fallback: query the webhook URL directly (GET on webhook URL returns its channel_id and guild_id)
    if DISCORD_WEBHOOK_URL:
        try:
            r = requests.get(DISCORD_WEBHOOK_URL, headers={"User-Agent": USER_AGENT}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if not DISCORD_CHANNEL_ID and data.get("channel_id"):
                    DISCORD_CHANNEL_ID = str(data["channel_id"])
                if not DISCORD_GUILD_ID and data.get("guild_id"):
                    DISCORD_GUILD_ID = str(data["guild_id"])
                if DISCORD_CHANNEL_ID:
                    logger.info(
                        f"Auto-discovered DISCORD_CHANNEL_ID={DISCORD_CHANNEL_ID} "
                        f"(guild_id={DISCORD_GUILD_ID}) directly from DISCORD_WEBHOOK_URL."
                    )
        except Exception as exc:
            logger.warning(f"Direct webhook metadata lookup failed: {redact_secrets(str(exc))}")


def ensure_background_workers_running():
    """
    Ensures background threads are started INSIDE the forked Gunicorn worker process
    (even when Gunicorn uses --preload or forks worker processes after module import).
    """
    global WORKERS_STARTED_PID
    current_pid = os.getpid()
    if WORKERS_STARTED_PID == current_pid:
        return

    with WORKER_START_LOCK:
        if WORKERS_STARTED_PID == current_pid:
            return

        auto_discover_discord_channel_id()
        register_telegram_webhook()

        sender_thread = threading.Thread(
            target=discord_sender_worker,
            name=f"discord-sender-{current_pid}",
            daemon=True,
        )
        sender_thread.start()

        if DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID:
            poller_thread = threading.Thread(
                target=discord_poller_loop,
                name=f"discord-poller-{current_pid}",
                daemon=True,
            )
            poller_thread.start()
        else:
            logger.info(
                f"[PID {current_pid}] DISCORD_BOT_TOKEN or DISCORD_CHANNEL_ID missing "
                f"(bot_token={bool(DISCORD_BOT_TOKEN)}, channel_id={DISCORD_CHANNEL_ID or 'unset'}); "
                f"running Telegram -> Discord only."
            )

        WORKERS_STARTED_PID = current_pid
        logger.info(f"[PID {current_pid}] Background worker threads are active.")


@app.before_request
def _boot_workers_on_first_request():
    ensure_background_workers_running()


def remember_in_cache(cache: OrderedDict, key):
    cache[key] = time.time()
    while len(cache) > MAX_CACHE_SIZE:
        cache.popitem(last=False)


def is_duplicate_or_album_extra(chat_id: str, message_id: int, media_group_id: str | None) -> bool:
    msg_key = (str(chat_id), int(message_id))
    if msg_key in PROCESSED_MESSAGES:
        return True
    remember_in_cache(PROCESSED_MESSAGES, msg_key)

    if media_group_id:
        group_key = (str(chat_id), str(media_group_id))
        if group_key in PROCESSED_MEDIA_GROUPS:
            return True
        remember_in_cache(PROCESSED_MEDIA_GROUPS, group_key)

    return False


def tg_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


# ==============================================================================
# TELEGRAM -> DISCORD HELPERS & RATE-LIMIT SAFE SENDER
# ==============================================================================
def download_telegram_file(file_id: str) -> bytes | None:
    try:
        r = requests.get(tg_api_url("getFile"), params={"file_id": file_id}, timeout=10)
        data = r.json()
        if not data.get("ok"):
            return None
        file_path = data["result"].get("file_path")
        if not file_path:
            return None
        file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        file_resp = requests.get(file_url, timeout=15)
        if file_resp.status_code == 200:
            return file_resp.content
    except Exception as exc:
        logger.warning(f"Failed to download Telegram file {file_id}: {redact_secrets(str(exc))}")
    return None


def fetch_user_avatar_bytes(user_id: int) -> bytes | None:
    try:
        r = requests.get(
            tg_api_url("getUserProfilePhotos"),
            params={"user_id": user_id, "limit": 1},
            timeout=10,
        )
        data = r.json()
        if not data.get("ok"):
            return None
        photos = data.get("result", {}).get("photos", [])
        if not photos or not photos[0]:
            return None
        best_photo = photos[0][-1]
        return download_telegram_file(best_photo["file_id"])
    except Exception as exc:
        logger.warning(f"Could not fetch avatar for user {user_id}: {redact_secrets(str(exc))}")
    return None


def extract_forward_author(msg: dict) -> tuple[str, str | None, int | None]:
    origin = msg.get("forward_origin")
    if origin:
        origin_type = origin.get("type")
        if origin_type == "user":
            user = origin.get("sender_user", {})
            user_id = user.get("id")
            username = user.get("username")
            full_name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
            if username:
                display = f"@{username}" if not full_name else f"{full_name} (@{username})"
                return display, f"https://t.me/{username}", user_id
            return (full_name or "Telegram User"), None, user_id

        elif origin_type == "hidden_user":
            sender_name = origin.get("sender_user_name", "Anonymous User")
            return sender_name, None, None

        elif origin_type in ("chat", "channel"):
            chat_obj = origin.get("sender_chat") or origin.get("chat") or {}
            title = chat_obj.get("title", "Telegram Channel")
            username = chat_obj.get("username")
            if username:
                return f"{title} (@{username})", f"https://t.me/{username}", None
            return title, None, None

    if "forward_from" in msg:
        user = msg["forward_from"]
        user_id = user.get("id")
        username = user.get("username")
        full_name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
        if username:
            display = f"@{username}" if not full_name else f"{full_name} (@{username})"
            return display, f"https://t.me/{username}", user_id
        return (full_name or "Telegram User"), None, user_id

    if "forward_sender_name" in msg:
        return msg["forward_sender_name"], None, None

    if "forward_from_chat" in msg:
        chat_obj = msg["forward_from_chat"]
        title = chat_obj.get("title", "Telegram Channel")
        username = chat_obj.get("username")
        if username:
            return f"{title} (@{username})", f"https://t.me/{username}", None
        return title, None, None

    if "from" in msg:
        user = msg["from"]
        user_id = user.get("id")
        username = user.get("username")
        full_name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
        if username:
            display = f"@{username}" if not full_name else f"{full_name} (@{username})"
            return display, f"https://t.me/{username}", user_id
        return (full_name or "Telegram User"), None, user_id

    if "author_signature" in msg:
        return msg["author_signature"], None, None

    return "Verified Customer", None, None


def build_message_link(chat: dict, message_id: int) -> str | None:
    username = chat.get("username")
    if username:
        return f"https://t.me/{username}/{message_id}"

    chat_id_str = str(chat.get("id", ""))
    if chat_id_str.startswith("-100"):
        clean_id = chat_id_str[4:]
        return f"https://t.me/c/{clean_id}/{message_id}"
    return None


def format_blockquote(text: str) -> str:
    if not text or not text.strip():
        return "> *(Media Vouch)*"
    lines = text.strip().splitlines()
    quoted_lines = [f"> {line}" if line.strip() else ">" for line in lines]
    return "\n".join(quoted_lines)


def build_multipart_files(raw_attachments: list[tuple[str, str, bytes]]) -> dict:
    """Rebuilds fresh BytesIO objects on each retry attempt so streams are never exhausted."""
    files = {}
    for field_name, filename, content_bytes in raw_attachments:
        files[field_name] = (filename, io.BytesIO(content_bytes), "image/jpeg")
    return files


def extract_retry_after(resp: requests.Response, attempt: int) -> tuple[float, str]:
    scope = resp.headers.get("X-RateLimit-Scope", "unknown")
    retry_after = None

    try:
        data = resp.json()
        if isinstance(data, dict) and "retry_after" in data:
            retry_after = float(data["retry_after"])
            if data.get("global"):
                scope = "global"
    except Exception:
        pass

    if retry_after is None:
        header_val = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Reset-After")
        if header_val:
            try:
                retry_after = float(header_val)
            except ValueError:
                pass

    body_preview = (resp.text or "")[:200].lower()
    if "cloudflare" in body_preview or "1015" in body_preview or "<!doctype html" in body_preview:
        scope = "cloudflare-shared-ip"
        if retry_after is None:
            retry_after = min(30.0, 5.0 * (2 ** attempt))

    if retry_after is None:
        retry_after = min(15.0, 2.0 * (2 ** attempt))

    return min(max(retry_after + 0.5, 1.0), 60.0), scope


def post_to_discord_with_retry(
    message_id: int,
    author_name: str,
    discord_payload: dict,
    raw_attachments: list[tuple[str, str, bytes]],
) -> bool:
    """
    Delivers the vouch to Discord:
    1. If DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are available, sends directly via the
       authenticated Discord Bot API first (completely immune to Render shared-IP webhook 429 bans).
    2. Otherwise (or if Bot API fails), sends via DISCORD_WEBHOOK_URL with automatic 429 retry.
    """
    auto_discover_discord_channel_id()

    # Priority 1: Authenticated Discord Bot API (has its own isolated 50 req/sec bucket)
    if DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID:
        if send_via_discord_bot_api(message_id, author_name, discord_payload, raw_attachments):
            return True

    # Priority 2: Discord Webhook URL with retry backoff
    if not DISCORD_WEBHOOK_URL:
        logger.error(f"Cannot send vouch #{message_id}: neither Bot API nor DISCORD_WEBHOOK_URL succeeded.")
        return False

    headers = {"User-Agent": USER_AGENT}
    max_attempts = 4

    for attempt in range(max_attempts):
        try:
            if raw_attachments:
                files = build_multipart_files(raw_attachments)
                resp = requests.post(
                    DISCORD_WEBHOOK_URL,
                    headers=headers,
                    data={"payload_json": json.dumps(discord_payload)},
                    files=files,
                    timeout=15,
                )
            else:
                resp = requests.post(
                    DISCORD_WEBHOOK_URL,
                    headers=headers,
                    json=discord_payload,
                    timeout=15,
                )

            if resp.status_code in (200, 204):
                logger.info(f"Forwarded Telegram vouch #{message_id} from {author_name} to Discord via Webhook.")
                return True

            if resp.status_code == 429:
                wait_sec, scope = extract_retry_after(resp, attempt)
                logger.warning(
                    f"Discord webhook 429 rate limit (scope={scope}, attempt={attempt + 1}/{max_attempts}) "
                    f"for vouch #{message_id}. Waiting {wait_sec:.1f}s..."
                )
                time.sleep(wait_sec)
                continue

            logger.error(
                f"Discord webhook returned HTTP {resp.status_code} for vouch #{message_id}: "
                f"{redact_secrets(resp.text[:300])}"
            )
            return False

        except Exception as exc:
            logger.error(f"Error sending vouch #{message_id} to Discord (attempt {attempt + 1}): {redact_secrets(str(exc))}")
            time.sleep(2.0)

    logger.error(f"Exhausted all retries for Telegram vouch #{message_id}.")
    return False


def send_via_discord_bot_api(
    message_id: int,
    author_name: str,
    discord_payload: dict,
    raw_attachments: list[tuple[str, str, bytes]],
) -> bool:
    """
    Sends the vouch embed using the authenticated Discord Bot Token (POST /channels/{id}/messages).
    """
    try:
        bot_payload = {"embeds": discord_payload.get("embeds", [])}
        url = f"{DISCORD_API_BASE}/channels/{DISCORD_CHANNEL_ID}/messages"
        if raw_attachments:
            files = build_multipart_files(raw_attachments)
            resp = requests.post(
                url,
                headers=discord_headers(),
                data={"payload_json": json.dumps(bot_payload)},
                files=files,
                timeout=15,
            )
        else:
            resp = requests.post(
                url,
                headers=discord_headers(),
                json=bot_payload,
                timeout=15,
            )

        if resp.status_code in (200, 201):
            msg_data = resp.json()
            if msg_data.get("id"):
                remember_in_cache(PROCESSED_DISCORD_IDS, str(msg_data["id"]))
            logger.info(f"Forwarded Telegram vouch #{message_id} from {author_name} to Discord via Bot API.")
            return True
        else:
            logger.warning(
                f"Discord Bot API returned HTTP {resp.status_code} for vouch #{message_id}: "
                f"{redact_secrets(resp.text[:250])}"
            )
    except Exception as exc:
        logger.warning(f"Discord Bot API send failed for vouch #{message_id}: {redact_secrets(str(exc))}")
    return False


def discord_sender_worker():
    """
    Background worker that processes outgoing Discord embeds sequentially
    with a 1.5s spacing delay.
    """
    logger.info(f"[PID {os.getpid()}] Started background Discord sender queue worker.")
    while True:
        item = DISCORD_SEND_QUEUE.get()
        if item is None:
            break
        try:
            message_id, author_name, discord_payload, raw_attachments = item
            post_to_discord_with_retry(message_id, author_name, discord_payload, raw_attachments)
            time.sleep(1.5)
        except Exception as exc:
            logger.error(f"Unexpected error in discord_sender_worker: {redact_secrets(str(exc))}")
        finally:
            DISCORD_SEND_QUEUE.task_done()


# ==============================================================================
# DISCORD -> TELEGRAM REVERSE SYNC HELPERS
# ==============================================================================
def clean_discord_content(content: str, mentions: list) -> str:
    if not content:
        return ""
    text = content
    for user in mentions or []:
        uid = str(user.get("id", ""))
        uname = user.get("username") or user.get("global_name") or "user"
        if uid:
            text = text.replace(f"<@{uid}>", f"@{uname}").replace(f"<@!{uid}>", f"@{uname}")
    text = re.sub(r"<a?(:[a-zA-Z0-9_]+:)\d+>", r"\1", text)
    return text.strip()


def send_discord_vouch_to_telegram(discord_msg: dict):
    target_chat = ALLOWED_CHAT_ID or RUNTIME_TELEGRAM_CHAT_ID
    if not target_chat:
        logger.warning("Cannot forward Discord vouch to Telegram: ALLOWED_CHAT_ID is not set yet.")
        return

    msg_id = str(discord_msg.get("id", ""))
    author = discord_msg.get("author", {})
    username = author.get("username", "DiscordUser")
    global_name = author.get("global_name")
    if global_name and global_name.lower() != username.lower():
        author_display = f"{global_name} (@{username})"
    else:
        author_display = f"@{username}"

    raw_content = discord_msg.get("content", "")
    mentions = discord_msg.get("mentions", [])
    clean_text = clean_discord_content(raw_content, mentions)
    if not clean_text:
        clean_text = "(Media Vouch)"

    discord_link = (
        f"https://discord.com/channels/{DISCORD_GUILD_ID}/{DISCORD_CHANNEL_ID}/{msg_id}"
        if DISCORD_GUILD_ID and DISCORD_CHANNEL_ID and msg_id
        else None
    )

    header_link = (
        f'<a href="{discord_link}">⭐ Verified Vouch — View on Discord ↗</a>'
        if discord_link
        else "<b>⭐ Verified Discord Vouch</b>"
    )

    html_caption = (
        f"<b>{html.escape(author_display)}</b>\n"
        f"{header_link}\n\n"
        f"<blockquote>{html.escape(clean_text)}</blockquote>\n\n"
        f"<code>Discord Vouch #{msg_id[-6:]}</code>"
    )

    image_bytes = None
    for att in discord_msg.get("attachments", []):
        ctype = (att.get("content_type") or "").lower()
        fname = (att.get("filename") or "").lower()
        if ctype.startswith("image/") or fname.endswith((".jpg", ".jpeg", ".png", ".webp")):
            try:
                img_resp = requests.get(att["url"], timeout=15)
                if img_resp.status_code == 200:
                    image_bytes = img_resp.content
                    break
            except Exception as exc:
                logger.warning(f"Failed to download Discord attachment: {redact_secrets(str(exc))}")

    try:
        if image_bytes:
            resp = requests.post(
                tg_api_url("sendPhoto"),
                data={
                    "chat_id": target_chat,
                    "caption": html_caption,
                    "parse_mode": "HTML",
                },
                files={"photo": ("discord_vouch.jpg", io.BytesIO(image_bytes), "image/jpeg")},
                timeout=15,
            )
        else:
            resp = requests.post(
                tg_api_url("sendMessage"),
                json={
                    "chat_id": target_chat,
                    "text": html_caption,
                    "parse_mode": "HTML",
                    "link_preview_options": {"is_disabled": True},
                },
                timeout=15,
            )

        res_json = resp.json()
        if res_json.get("ok"):
            tg_result = res_json["result"]
            tg_chat_id = str(tg_result.get("chat", {}).get("id", ""))
            tg_msg_id = int(tg_result.get("message_id", 0))
            if tg_chat_id and tg_msg_id:
                remember_in_cache(PROCESSED_MESSAGES, (tg_chat_id, tg_msg_id))
            logger.info(f"Forwarded Discord vouch #{msg_id} from {author_display} to Telegram (#{tg_msg_id}).")
        else:
            logger.error(f"Telegram API rejected Discord vouch: {redact_secrets(str(res_json))}")
    except Exception as exc:
        logger.error(f"Error sending Discord vouch to Telegram: {redact_secrets(str(exc))}")


def init_discord_poller_state() -> bool:
    global DISCORD_GUILD_ID, LAST_DISCORD_MESSAGE_ID
    auto_discover_discord_channel_id()
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        return False

    try:
        ch_resp = requests.get(
            f"{DISCORD_API_BASE}/channels/{DISCORD_CHANNEL_ID}",
            headers=discord_headers(),
            timeout=10,
        )
        if ch_resp.status_code == 429:
            wait_sec, scope = extract_retry_after(ch_resp, 1)
            logger.warning(f"Discord channel lookup 429 (scope={scope}), sleeping {wait_sec:.1f}s")
            time.sleep(wait_sec)
            return False
        if ch_resp.status_code == 200:
            ch_data = ch_resp.json()
            if not DISCORD_GUILD_ID and ch_data.get("guild_id"):
                DISCORD_GUILD_ID = str(ch_data["guild_id"])
        else:
            logger.warning(f"Discord channel lookup returned {ch_resp.status_code}: {redact_secrets(ch_resp.text[:200])}")
            return False

        msg_resp = requests.get(
            f"{DISCORD_API_BASE}/channels/{DISCORD_CHANNEL_ID}/messages",
            headers=discord_headers(),
            params={"limit": 1},
            timeout=10,
        )
        if msg_resp.status_code == 200:
            msgs = msg_resp.json()
            if isinstance(msgs, list) and len(msgs) > 0:
                LAST_DISCORD_MESSAGE_ID = str(msgs[0]["id"])
                remember_in_cache(PROCESSED_DISCORD_IDS, LAST_DISCORD_MESSAGE_ID)
            else:
                LAST_DISCORD_MESSAGE_ID = "0"
            logger.info(
                f"Discord poller initialized (guild_id={DISCORD_GUILD_ID}, "
                f"channel_id={DISCORD_CHANNEL_ID}, starting after message_id={LAST_DISCORD_MESSAGE_ID})"
            )
            return True
        else:
            logger.warning(f"Discord messages init returned {msg_resp.status_code}: {redact_secrets(msg_resp.text[:200])}")
            return False
    except Exception as exc:
        logger.warning(f"Failed to initialize Discord poller: {redact_secrets(str(exc))}")
        return False


def poll_discord_once():
    global LAST_DISCORD_MESSAGE_ID
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        return

    with POLLER_LOCK:
        if LAST_DISCORD_MESSAGE_ID is None:
            if not init_discord_poller_state():
                return

        params = {"limit": 50}
        if LAST_DISCORD_MESSAGE_ID and LAST_DISCORD_MESSAGE_ID != "0":
            params["after"] = LAST_DISCORD_MESSAGE_ID

        try:
            resp = requests.get(
                f"{DISCORD_API_BASE}/channels/{DISCORD_CHANNEL_ID}/messages",
                headers=discord_headers(),
                params=params,
                timeout=10,
            )
            if resp.status_code == 429:
                wait_sec, scope = extract_retry_after(resp, 1)
                logger.warning(f"Discord poll 429 rate limit (scope={scope}), backing off {wait_sec:.1f}s")
                time.sleep(wait_sec)
                return

            if resp.status_code != 200:
                logger.warning(f"Discord poll HTTP {resp.status_code}: {redact_secrets(resp.text[:200])}")
                return

            messages = resp.json()
            if not isinstance(messages, list) or not messages:
                return

            messages_sorted = sorted(messages, key=lambda m: int(m.get("id", "0")))

            for msg in messages_sorted:
                msg_id = str(msg.get("id", ""))
                if not msg_id:
                    continue

                LAST_DISCORD_MESSAGE_ID = msg_id
                if msg_id in PROCESSED_DISCORD_IDS:
                    continue
                remember_in_cache(PROCESSED_DISCORD_IDS, msg_id)

                if msg.get("webhook_id") is not None:
                    continue

                author = msg.get("author", {})
                if author.get("bot"):
                    continue

                if msg.get("type", 0) not in (0, 19):
                    continue

                if not (msg.get("content") or "").strip() and not msg.get("attachments"):
                    continue

                send_discord_vouch_to_telegram(msg)

        except Exception as exc:
            logger.warning(f"Error during Discord polling: {redact_secrets(str(exc))}")


def discord_poller_loop():
    logger.info(f"[PID {os.getpid()}] Starting Discord -> Telegram background poller (interval={DISCORD_POLL_INTERVAL}s)")
    while True:
        poll_discord_once()
        time.sleep(max(10, DISCORD_POLL_INTERVAL))


# ==============================================================================
# FLASK ROUTES
# ==============================================================================
@app.route("/", methods=["GET", "HEAD"])
@app.route("/health", methods=["GET", "HEAD"])
def health_check():
    return jsonify({
        "status": "ok",
        "service": "telegram-discord-vouch-bridge",
        "worker_pid": os.getpid(),
        "queue_size": DISCORD_SEND_QUEUE.qsize(),
        "discord_channel_id": DISCORD_CHANNEL_ID or None,
        "discord_to_telegram_enabled": bool(DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID),
    }), 200


@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    global RUNTIME_TELEGRAM_CHAT_ID

    if WEBHOOK_SECRET:
        header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if header_secret != WEBHOOK_SECRET:
            return jsonify({"error": "Unauthorized"}), 403

    update = request.get_json(silent=True) or {}
    msg = update.get("channel_post") or update.get("message")
    if not msg:
        return jsonify({"status": "ignored", "reason": "no message payload"}), 200

    chat = msg.get("chat", {})
    chat_id = str(chat.get("id", ""))
    chat_username = f"@{chat.get('username', '')}".lower()

    if ALLOWED_CHAT_ID:
        allowed_clean = ALLOWED_CHAT_ID.lower()
        if chat_id != allowed_clean and chat_username != allowed_clean:
            logger.info(f"Ignored message from unauthorized chat: {chat_id} ({chat_username})")
            return jsonify({"status": "ignored", "reason": "unauthorized chat"}), 200

    if chat_id and not RUNTIME_TELEGRAM_CHAT_ID:
        RUNTIME_TELEGRAM_CHAT_ID = chat_id

    sender_user = msg.get("from", {})
    if sender_user.get("is_bot") and not msg.get("forward_origin") and not msg.get("forward_from"):
        return jsonify({"status": "ignored", "reason": "bot message"}), 200

    message_id = msg.get("message_id", 0)
    media_group_id = msg.get("media_group_id")
    raw_text = msg.get("text") or msg.get("caption") or ""

    if "Discord Vouch #" in raw_text or "Verified Discord Vouch" in raw_text:
        return jsonify({"status": "ignored", "reason": "discord synced message"}), 200

    if not raw_text and "photo" not in msg and "document" not in msg and "video" not in msg:
        return jsonify({"status": "ignored", "reason": "service or empty message"}), 200

    if is_duplicate_or_album_extra(chat_id, message_id, media_group_id):
        logger.info(f"Skipped duplicate or album-extra message #{message_id} (media_group_id={media_group_id})")
        return jsonify({"status": "ignored", "reason": "duplicate or media album extra"}), 200

    author_name, author_url, user_id = extract_forward_author(msg)
    vouch_link = build_message_link(chat, message_id)
    msg_timestamp = msg.get("date", int(datetime.now(timezone.utc).timestamp()))
    iso_timestamp = datetime.fromtimestamp(msg_timestamp, tz=timezone.utc).isoformat()

    raw_attachments: list[tuple[str, str, bytes]] = []

    avatar_bytes = fetch_user_avatar_bytes(user_id) if user_id else None
    if avatar_bytes:
        raw_attachments.append(("files[0]", "avatar.jpg", avatar_bytes))
        author_icon_url = "attachment://avatar.jpg"
    else:
        author_icon_url = DEFAULT_TELEGRAM_ICON

    vouch_image_bytes = None
    if "photo" in msg and msg["photo"]:
        largest_photo = msg["photo"][-1]
        vouch_image_bytes = download_telegram_file(largest_photo["file_id"])

    author_block = {
        "name": author_name,
        "icon_url": author_icon_url,
    }
    if author_url:
        author_block["url"] = author_url
    elif vouch_link:
        author_block["url"] = vouch_link

    embed = {
        "author": author_block,
        "title": "⭐ Verified Vouch — View on Telegram ↗",
        "description": format_blockquote(raw_text),
        "color": EMBED_COLOR,
        "footer": {
            "text": f"Telegram Vouch #{message_id}"
        },
        "timestamp": iso_timestamp,
    }

    if vouch_link:
        embed["url"] = vouch_link

    if vouch_image_bytes:
        file_index = len(raw_attachments)
        raw_attachments.append((f"files[{file_index}]", "vouch.jpg", vouch_image_bytes))
        embed["image"] = {"url": "attachment://vouch.jpg"}

    discord_payload = {
        "embeds": [embed]
    }
    if DISCORD_BOT_USERNAME:
        discord_payload["username"] = DISCORD_BOT_USERNAME
    if DISCORD_BOT_AVATAR_URL:
        discord_payload["avatar_url"] = DISCORD_BOT_AVATAR_URL

    # Ensure background threads exist in this worker PID, then enqueue
    ensure_background_workers_running()
    DISCORD_SEND_QUEUE.put((message_id, author_name, discord_payload, raw_attachments))
    logger.info(f"Queued Telegram vouch #{message_id} from {author_name} for Discord delivery.")

    return jsonify({"status": "queued", "vouch_id": message_id}), 200


def register_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN or not RENDER_EXTERNAL_URL:
        return
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/webhook"
    payload = {
        "url": webhook_url,
        "allowed_updates": ["channel_post", "message"],
    }
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    try:
        r = requests.post(tg_api_url("setWebhook"), json=payload, timeout=10)
        logger.info(f"Telegram setWebhook response: {r.json()}")
    except Exception as exc:
        logger.warning(f"Could not auto-register Telegram webhook: {redact_secrets(str(exc))}")


if __name__ == "__main__":
    ensure_background_workers_running()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
