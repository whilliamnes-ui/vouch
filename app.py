import os
import io
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Required Environment Variables
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Optional Environment Variables
# Restrict to a specific Telegram channel ID (e.g. -1001234567890) or @channelusername
ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
DISCORD_BOT_USERNAME = os.environ.get("DISCORD_BOT_USERNAME", "").strip()
DISCORD_BOT_AVATAR_URL = os.environ.get("DISCORD_BOT_AVATAR_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

# Default fallback icon if the Telegram user has hidden privacy settings or no avatar
DEFAULT_TELEGRAM_ICON = "https://tse1.mm.bing.net/th/id/OIP._ShzJj6u5Ut7lSd_PWbX2wHaKz?r=0&rs=1&pid=ImgDetMain&o=7&rm=3"

# Embed color #fdfdfd (16645629 in decimal)
EMBED_COLOR = 0xFDFDFD


def tg_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def download_telegram_file(file_id: str) -> bytes | None:
    """Downloads a file from Telegram in memory so the bot token is never exposed in public URLs."""
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
        logger.warning(f"Failed to download Telegram file {file_id}: {exc}")
    return None


def fetch_user_avatar_bytes(user_id: int) -> bytes | None:
    """Fetches the user's current profile picture from Telegram if privacy settings allow."""
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
        # Pick medium resolution (or largest available in the first set)
        best_photo = photos[0][-1]
        return download_telegram_file(best_photo["file_id"])
    except Exception as exc:
        logger.warning(f"Could not fetch avatar for user {user_id}: {exc}")
    return None


def extract_forward_author(msg: dict) -> tuple[str, str | None, int | None]:
    """
    Extracts (author_display, author_url, user_id) from a forwarded Telegram message.
    Supports both Bot API 7.0+ `forward_origin` and legacy `forward_from` fields.
    """
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

    # Fallback to legacy fields
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

    # Fallback if message was posted directly without forwarding
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
    """Builds the direct t.me link to the specific vouch message in the channel."""
    username = chat.get("username")
    if username:
        return f"https://t.me/{username}/{message_id}"

    chat_id_str = str(chat.get("id", ""))
    if chat_id_str.startswith("-100"):
        clean_id = chat_id_str[4:]
        return f"https://t.me/c/{clean_id}/{message_id}"
    return None


def format_blockquote(text: str) -> str:
    """Formats multi-line vouch text into a clean Discord blockquote."""
    if not text or not text.strip():
        return "> *(Media Vouch)*"
    lines = text.strip().splitlines()
    quoted_lines = [f"> {line}" if line.strip() else ">" for line in lines]
    return "\n".join(quoted_lines)


@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok", "service": "telegram-discord-vouch-bridge"}), 200


@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    # Optional secret token verification
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

    # Check if restricted to a specific channel
    if ALLOWED_CHAT_ID:
        allowed_clean = ALLOWED_CHAT_ID.lower()
        if chat_id != allowed_clean and chat_username != allowed_clean:
            logger.info(f"Ignored message from unauthorized chat: {chat_id} ({chat_username})")
            return jsonify({"status": "ignored", "reason": "unauthorized chat"}), 200

    message_id = msg.get("message_id", 0)
    raw_text = msg.get("text") or msg.get("caption") or ""

    # Ignore empty service messages (e.g., pinned message notifications, channel photo changes)
    if not raw_text and "photo" not in msg and "document" not in msg and "video" not in msg:
        return jsonify({"status": "ignored", "reason": "service or empty message"}), 200

    author_name, author_url, user_id = extract_forward_author(msg)
    vouch_link = build_message_link(chat, message_id)
    msg_timestamp = msg.get("date", int(datetime.now(timezone.utc).timestamp()))
    iso_timestamp = datetime.fromtimestamp(msg_timestamp, tz=timezone.utc).isoformat()

    # Prepare files for multipart upload (keeps bot token 100% hidden and hosts images permanently on Discord)
    files_to_upload = {}

    # 1. Fetch Author Avatar if user_id is available
    avatar_bytes = fetch_user_avatar_bytes(user_id) if user_id else None
    if avatar_bytes:
        files_to_upload["files[0]"] = ("avatar.jpg", io.BytesIO(avatar_bytes), "image/jpeg")
        author_icon_url = "attachment://avatar.jpg"
    else:
        author_icon_url = DEFAULT_TELEGRAM_ICON

    # 2. Fetch Vouch Screenshot/Photo if present
    vouch_image_bytes = None
    if "photo" in msg and msg["photo"]:
        largest_photo = msg["photo"][-1]
        vouch_image_bytes = download_telegram_file(largest_photo["file_id"])

    # Build the Discord Embed
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
        file_index = len(files_to_upload)
        files_to_upload[f"files[{file_index}]"] = ("vouch.jpg", io.BytesIO(vouch_image_bytes), "image/jpeg")
        embed["image"] = {"url": "attachment://vouch.jpg"}

    discord_payload = {
        "embeds": [embed]
    }
    if DISCORD_BOT_USERNAME:
        discord_payload["username"] = DISCORD_BOT_USERNAME
    if DISCORD_BOT_AVATAR_URL:
        discord_payload["avatar_url"] = DISCORD_BOT_AVATAR_URL

    try:
        if files_to_upload:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                data={"payload_json": json.dumps(discord_payload)},
                files=files_to_upload,
                timeout=15,
            )
        else:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                json=discord_payload,
                timeout=15,
            )
        resp.raise_for_status()
        logger.info(f"Forwarded vouch #{message_id} from {author_name} to Discord.")
    except Exception as exc:
        logger.error(f"Failed to send webhook to Discord: {exc}")
        return jsonify({"status": "error", "detail": str(exc)}), 500

    return jsonify({"status": "ok", "vouch_id": message_id}), 200


def register_telegram_webhook():
    """Automatically registers the Render URL with Telegram Bot API on startup if configured."""
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
        logger.warning(f"Could not auto-register Telegram webhook: {exc}")


# Register webhook automatically when imported by Gunicorn on Render
register_telegram_webhook()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
