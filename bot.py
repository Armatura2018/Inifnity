#!/usr/bin/env python3
# Infinity — Telegram Business message change/deletion tracker
# Python 3.11+, aiogram 3, aiosqlite

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import (
    BusinessConnection,
    BusinessMessagesDeleted,
    FSInputFile,
    Message,
    Update,
    BotCommand,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/bot.db"))
DATA_DIR = DATABASE_PATH.parent
MEDIA_DIR = DATA_DIR / "media"
RETENTION_SECONDS = 3 * 24 * 60 * 60
CLEANUP_INTERVAL = 60 *  60
MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024  # Telegram cloud Bot API download limit
TEXT_LIMIT = 3900

if not BOT_TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is required")

DATA_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("infinity")

db: aiosqlite.Connection | None = None
db_lock = asyncio.Lock()


def utc_now() -> int:
    return int(time.time())


def fmt_time(value: int | None) -> str:
    if not value:
        return "неизвестно"
    return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime(
        "%d.%m.%Y %H:%M:%S"
    )


def safe_html(value: Any) -> str:
    return html.escape(str(value or ""), quote=False)


def truncate(value: str, limit: int = TEXT_LIMIT) -> str:
    value = value or ""
    if len(value) <= limit:
        return value
    return value[:limit] + "\n… (текст сокращён)"


def media_details(message: Message) -> dict[str, Any] | None:
    item = None
    kind = None

    if message.photo:
        item, kind = message.photo[-1], "photo"
    elif message.video:
        item, kind = message.video, "video"
    elif message.voice:
        item, kind = message.voice, "voice"
    elif message.video_note:
        item, kind = message.video_note, "video_note"
    elif message.document:
        item, kind = message.document, "document"
    elif message.animation:
        item, kind = message.animation, "animation"
    elif message.audio:
        item, kind = message.audio, "audio"
    elif message.sticker:
        item, kind = message.sticker, "sticker"

    if item is None:
        return None

    return {
        "type": kind,
        "file_id": item.file_id,
        "file_unique_id": item.file_unique_id,
        "file_size": getattr(item, "file_size", None),
        "file_name": getattr(item, "file_name", None),
    }


def sender_details(message: Message) -> tuple[str, str | None, int | None]:
    if message.from_user:
        user = message.from_user
        return user.full_name or "Неизвестный пользователь", user.username, user.id
    if message.sender_chat:
        return message.sender_chat.title or "Чат", message.sender_chat.username, message.sender_chat.id
    return "Неизвестный отправитель", None, None


def message_body(message: Message) -> str:
    # Store only actual text/caption. Media-only messages must not create
    # artificial placeholders in edit/delete notifications.
    return message.text or message.caption or ""


def quoted_text(label: str, body: str | None, limit: int = 1300) -> str:
    body = (body or "").strip()
    # Ignore placeholders written by older versions for media-only messages.
    legacy_placeholders = {
        "[Фотография]", "[Видео]", "[Голосовое сообщение]",
        "[Видеосообщение]", "[Документ]", "[GIF/анимация]",
        "[Аудио]", "[Стикер]", "[Вложение]", "[Сообщение без текста]",
    }
    if not body or body in legacy_placeholders:
        return ""
    return f"{label}:\n<blockquote>{safe_html(truncate(body, limit))}</blockquote>\n\n"


async def init_db() -> None:
    global db
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA busy_timeout=5000")
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            started_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS business_connections (
            connection_id TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            user_chat_id INTEGER NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 0,
            connected_at INTEGER,
            rights_json TEXT,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY(owner_id) REFERENCES users(user_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connections_owner
            ON business_connections(owner_id);

        CREATE TABLE IF NOT EXISTS messages (
            connection_id TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            sender_username TEXT,
            body TEXT,
            caption TEXT,
            sent_at INTEGER,
            media_type TEXT,
            media_file_id TEXT,
            media_unique_id TEXT,
            media_file_name TEXT,
            media_path TEXT,
            saved_at INTEGER NOT NULL,
            PRIMARY KEY(connection_id, chat_id, message_id),
            FOREIGN KEY(connection_id) REFERENCES business_connections(connection_id)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_messages_expiry
            ON messages(saved_at);
        CREATE INDEX IF NOT EXISTS idx_messages_owner
            ON messages(owner_id, connection_id, chat_id);

        CREATE TABLE IF NOT EXISTS processed_events (
            event_key TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            processed_at INTEGER NOT NULL,
            FOREIGN KEY(owner_id) REFERENCES users(user_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_events_expiry
            ON processed_events(processed_at);
        """
    )
    await db.commit()


async def db_execute(query: str, params: tuple = ()) -> None:
    assert db is not None
    async with db_lock:
        await db.execute(query, params)
        await db.commit()


async def db_fetchone(query: str, params: tuple = ()) -> aiosqlite.Row | None:
    assert db is not None
    async with db_lock:
        cursor = await db.execute(query, params)
        row = await cursor.fetchone()
        await cursor.close()
        return row


async def db_fetchall(query: str, params: tuple = ()) -> list[aiosqlite.Row]:
    assert db is not None
    async with db_lock:
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return rows


async def remember_user(user_id: int, username: str | None, first_name: str | None) -> None:
    await db_execute(
        """
        INSERT INTO users(user_id, username, first_name, started_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name
        """,
        (user_id, username, first_name, utc_now()),
    )


async def get_connection(connection_id: str) -> aiosqlite.Row | None:
    return await db_fetchone(
        "SELECT * FROM business_connections WHERE connection_id=?",
        (connection_id,),
    )


async def store_connection(connection: BusinessConnection) -> None:
    owner_id = connection.user.id
    rights = connection.rights.model_dump(exclude_none=True) if connection.rights else {}
    # A Business connection is associated with the Telegram account that connected it.
    await db_execute(
        """
        INSERT INTO users(user_id, username, first_name, started_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name
        """,
        (
            owner_id,
            connection.user.username,
            connection.user.first_name,
            utc_now(),
        ),
    )
    connected_at = (
        int(connection.date.timestamp())
        if hasattr(connection.date, "timestamp")
        else int(connection.date)
    )
    await db_execute(
        """
        INSERT INTO business_connections(
            connection_id, owner_id, user_chat_id, is_enabled,
            connected_at, rights_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(connection_id) DO UPDATE SET
            owner_id=excluded.owner_id,
            user_chat_id=excluded.user_chat_id,
            is_enabled=excluded.is_enabled,
            rights_json=excluded.rights_json,
            updated_at=excluded.updated_at
        """,
        (
            connection.id,
            owner_id,
            connection.user_chat_id,
            int(connection.is_enabled),
            connected_at,
            json.dumps(rights, ensure_ascii=False),
            utc_now(),
        ),
    )


async def save_media_copy(
    bot: Bot,
    owner_id: int,
    connection_id: str,
    chat_id: int,
    message_id: int,
    media: dict[str, Any] | None,
    old_row: aiosqlite.Row | None,
) -> str | None:
    if not media:
        return None

    old_path = old_row["media_path"] if old_row else None
    if (
        old_row
        and old_row["media_file_id"] == media["file_id"]
        and old_path
        and Path(old_path).is_file()
    ):
        return old_path

    size = media.get("file_size")
    if size and size > MAX_DOWNLOAD_BYTES:
        return None

    conn_hash = hashlib.sha256(connection_id.encode()).hexdigest()[:12]
    unique = re.sub(r"[^a-zA-Z0-9_-]", "", media.get("file_unique_id") or "file")[:60]
    folder = MEDIA_DIR / str(owner_id) / conn_hash
    folder.mkdir(parents=True, exist_ok=True)
    suffix = Path(media.get("file_name") or "").suffix[:12]
    if not suffix:
        suffix = {
            "photo": ".jpg",
            "video": ".mp4",
            "voice": ".ogg",
            "video_note": ".mp4",
            "animation": ".mp4",
            "audio": ".mp3",
            "sticker": ".webp",
        }.get(media["type"], ".bin")
    target = folder / f"{chat_id}_{message_id}_{unique}{suffix}"

    try:
        await bot.download(media["file_id"], destination=target)
        return str(target) if target.exists() else None
    except Exception as exc:
        # Do not log message text, file IDs, or user content.
        log.info("Media copy unavailable (%s)", type(exc).__name__)
        return None


async def save_business_message(bot: Bot, message: Message) -> None:
    connection_id = message.business_connection_id
    if not connection_id:
        return

    connection = await get_connection(connection_id)
    if not connection:
        log.warning("Business message arrived for an unknown connection")
        return
    if not connection["is_enabled"]:
        return

    owner_id = int(connection["owner_id"])
    chat_id = message.chat.id
    message_id = message.message_id
    old = await db_fetchone(
        """
        SELECT * FROM messages
        WHERE owner_id=? AND connection_id=? AND chat_id=? AND message_id=?
        """,
        (owner_id, connection_id, chat_id, message_id),
    )

    sender_name, username, sender_id = sender_details(message)
    media = media_details(message)
    media_path = await save_media_copy(
        bot, owner_id, connection_id, chat_id, message_id, media, old
    )
    sent_at = int(message.date.timestamp()) if message.date else utc_now()

    await db_execute(
        """
        INSERT INTO messages(
            connection_id, owner_id, chat_id, message_id,
            sender_id, sender_name, sender_username, body, caption,
            sent_at, media_type, media_file_id, media_unique_id,
            media_file_name, media_path, saved_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(connection_id, chat_id, message_id) DO UPDATE SET
            owner_id=excluded.owner_id,
            sender_id=excluded.sender_id,
            sender_name=excluded.sender_name,
            sender_username=excluded.sender_username,
            body=excluded.body,
            caption=excluded.caption,
            sent_at=excluded.sent_at,
            media_type=excluded.media_type,
            media_file_id=excluded.media_file_id,
            media_unique_id=excluded.media_unique_id,
            media_file_name=excluded.media_file_name,
            media_path=excluded.media_path
        """,
        (
            connection_id,
            owner_id,
            chat_id,
            message_id,
            sender_id,
            sender_name,
            username,
            message_body(message),
            message.caption,
            sent_at,
            media["type"] if media else None,
            media["file_id"] if media else None,
            media["file_unique_id"] if media else None,
            media["file_name"] if media else None,
            media_path,
            utc_now(),
        ),
    )

    # Remove a previous local copy if an edited message replaced/removed its attachment.
    previous_path = old["media_path"] if old else None
    if previous_path and previous_path != media_path:
        try:
            Path(previous_path).unlink(missing_ok=True)
        except OSError:
            log.warning("Could not remove replaced media copy")


async def claim_event(owner_id: int, event_key: str) -> bool:
    """Returns True only for a not-yet-processed event."""
    assert db is not None
    async with db_lock:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO processed_events(event_key, owner_id, processed_at)
            VALUES (?, ?, ?)
            """,
            (event_key, owner_id, utc_now()),
        )
        inserted = cursor.rowcount == 1
        await cursor.close()
        await db.commit()
        return inserted


async def mark_event_failed(event_key: str) -> None:
    await db_execute("DELETE FROM processed_events WHERE event_key=?", (event_key,))


async def send_saved_media(bot: Bot, chat_id: int, row: aiosqlite.Row) -> None:
    media_type = row["media_type"]
    file_id = row["media_file_id"]
    if not media_type or not file_id:
        return

    local_path = row["media_path"]
    media_input: Any = FSInputFile(local_path) if local_path and Path(local_path).is_file() else file_id
    # The original caption is shown in the quoted text section of the alert.
    # Do not duplicate it under the resent attachment.
    caption = None

    try:
        if media_type == "photo":
            await bot.send_photo(chat_id, media_input, caption=caption)
        elif media_type == "video":
            await bot.send_video(chat_id, media_input, caption=caption)
        elif media_type == "voice":
            await bot.send_voice(chat_id, media_input, caption=caption)
        elif media_type == "video_note":
            await bot.send_video_note(chat_id, media_input)
        elif media_type == "document":
            await bot.send_document(chat_id, media_input, caption=caption)
        elif media_type == "animation":
            await bot.send_animation(chat_id, media_input, caption=caption)
        elif media_type == "audio":
            await bot.send_audio(chat_id, media_input, caption=caption)
        elif media_type == "sticker":
            await bot.send_sticker(chat_id, media_input)
    except TelegramAPIError as exc:
        log.info("Could not resend saved media (%s)", type(exc).__name__)
        if local_path and Path(local_path).is_file() and media_input != file_id:
            # Local copy may be too large or unsupported; do not retry with the same payload.
            return


async def handle_edited_message(bot: Bot, message: Message) -> None:
    connection_id = message.business_connection_id
    if not connection_id:
        return
    connection = await get_connection(connection_id)
    if not connection or not connection["is_enabled"]:
        return

    owner_id = int(connection["owner_id"])
    chat_id = message.chat.id
    message_id = message.message_id
    old = await db_fetchone(
        """
        SELECT * FROM messages
        WHERE owner_id=? AND connection_id=? AND chat_id=? AND message_id=?
        """,
        (owner_id, connection_id, chat_id, message_id),
    )

    new_body = message_body(message)
    old_body = old["body"] if old else ""
    edit_time = int(message.edit_date.timestamp()) if message.edit_date else utc_now()
    fingerprint = hashlib.sha256(new_body.encode("utf-8", "replace")).hexdigest()[:16]
    event_key = f"edit:{connection_id}:{chat_id}:{message_id}:{edit_time}:{fingerprint}"

    if not await claim_event(owner_id, event_key):
        return

    notification_chat = int(connection["user_chat_id"])
    sender_name = old["sender_name"] if old else sender_details(message)[0]
    sender_username = old["sender_username"] if old else sender_details(message)[1]
    sender_label = sender_name
    if sender_username:
        sender_label += f" (@{sender_username})"

    text = (
        "✏️ <b>Сообщение отредактировано</b>\n\n"
        f"👤 Собеседник: {safe_html(sender_label)}\n\n"
        + quoted_text("📝 Первоначальный текст", old_body)
        + quoted_text("📝 Новая версия", new_body)
        + f"🕒 Время изменения: {fmt_time(edit_time)}"
    )

    try:
        await bot.send_message(notification_chat, text)
        # Store the new version, then resend its attachment only if it exists.
        await save_business_message(bot, message)
        updated = await db_fetchone(
            """
            SELECT * FROM messages
            WHERE owner_id=? AND connection_id=? AND chat_id=? AND message_id=?
            """,
            (owner_id, connection_id, chat_id, message_id),
        )
        if updated and updated["media_type"] and updated["media_file_id"]:
            await send_saved_media(bot, notification_chat, updated)
    except TelegramForbiddenError:
        log.warning("Owner chat is unavailable; ask the owner to send /start")
        await mark_event_failed(event_key)
    except TelegramAPIError as exc:
        log.warning("Edit notification failed (%s)", type(exc).__name__)
        await mark_event_failed(event_key)


async def handle_deleted_messages(
    bot: Bot, deleted: BusinessMessagesDeleted
) -> None:
    connection_id = deleted.business_connection_id
    connection = await get_connection(connection_id)
    if not connection:
        return

    owner_id = int(connection["owner_id"])
    if not connection["is_enabled"]:
        # Telegram can deliver a final queued event around a disconnect; still process it.
        pass

    notification_chat = int(connection["user_chat_id"])
    chat_id = deleted.chat.id
    for message_id in deleted.message_ids:
        event_key = f"delete:{connection_id}:{chat_id}:{message_id}"
        if not await claim_event(owner_id, event_key):
            continue

        row = await db_fetchone(
            """
            SELECT * FROM messages
            WHERE owner_id=? AND connection_id=? AND chat_id=? AND message_id=?
            """,
            (owner_id, connection_id, chat_id, message_id),
        )

        if row:
            sender = row["sender_name"] or "Неизвестный отправитель"
            if row["sender_username"]:
                sender += f" (@{row['sender_username']})"
            body = row["body"] or ""
            sent_at = int(row["sent_at"] or 0)
        else:
            sender = "Неизвестный отправитель"
            body = ""
            sent_at = 0

        text = (
            "🗑 <b>Сообщение удалено</b>\n\n"
            f"👤 Отправитель: {safe_html(sender)}\n\n"
            + quoted_text("📝 Оригинал", body, 2500)
            + f"📅 Отправлено: {fmt_time(sent_at)}\n\n"
            + f"🕒 Удалено: {fmt_time(utc_now())}"
        )

        try:
            await bot.send_message(notification_chat, text)
            if row and row["media_type"] and row["media_file_id"]:
                await send_saved_media(bot, notification_chat, row)
        except TelegramForbiddenError:
            log.warning("Owner chat is unavailable; ask the owner to send /start")
            await mark_event_failed(event_key)
        except TelegramAPIError as exc:
            log.warning("Delete notification failed (%s)", type(exc).__name__)
            await mark_event_failed(event_key)


async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        cutoff = utc_now() - RETENTION_SECONDS
        try:
            rows = await db_fetchall(
                "SELECT owner_id, media_path FROM messages WHERE saved_at < ?",
                (cutoff,),
            )
            # Paths are read from rows owned by the matching DB records; no cross-user globbing.
            for row in rows:
                path = row["media_path"]
                if path:
                    try:
                        Path(path).unlink(missing_ok=True)
                    except OSError:
                        log.warning("Could not remove an expired media file")

            await db_execute("DELETE FROM messages WHERE saved_at < ?", (cutoff,))
            await db_execute("DELETE FROM processed_events WHERE processed_at < ?", (cutoff,))
            log.info("Retention cleanup completed")
        except Exception as exc:
            log.error("Retention cleanup failed (%s)", type(exc).__name__)


START_TEXT = (
    "<b>Infinity 🐍</b>\n\n"
    "Сохраняю сообщения, чтобы присылать тебе уведомления об их удалении "
    "и редактировании. Если Telegram предоставляет медиафайл, пытаюсь "
    "сохранить его копию.\n\n"
    "<b>Для подключения:</b>\n"
    "1. Открой настройки Telegram.\n"
    "2. Перейди в раздел «Аккаунт».\n"
    "3. В разделе «Автоматизация чатов» найди бота @infinitysaving_bot"
)

HELP_TEXT = (
    "<b>Infinity — архив изменений сообщений</b>\n\n"
    "• Сохраняет новые Business-сообщения.\n"
    "• Присылает старую и новую версии при редактировании.\n"
    "• Присылает оригинал при удалении.\n"
    "• Сохраняет доступные фото, видео, голосовые, видеосообщения и документы.\n"
    "• Хранит записи и локальные копии не более 3 дней.\n\n"
    "Важно: сообщения, полученные до подключения, восстановить нельзя. "
    "Скачивание копий файлов через облачный Bot API ограничено 20 МБ; "
    "для более крупных файлов сохраняется file_id, если Telegram его предоставил.\n\n"
    "/start — приветствие и инструкция\n"
    "/status — состояние Business-подключений\n"
    "/help — эта справка"
)


async def cmd_start(message: Message) -> None:
    if not message.from_user or message.chat.type != "private":
        return
    await remember_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
    )
    await message.answer(START_TEXT)


async def cmd_help(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer(HELP_TEXT)


async def cmd_status(message: Message) -> None:
    if not message.from_user or message.chat.type != "private":
        return

    rows = await db_fetchall(
        """
        SELECT connection_id, is_enabled, connected_at, rights_json
        FROM business_connections
        WHERE owner_id=?
        ORDER BY updated_at DESC
        """,
        (message.from_user.id,),
    )

    if not rows:
        await message.answer(
            "Business-подключение не найдено.\n\n"
            "Сначала подключи Infinity в настройках Telegram Business."
        )
        return

    lines = ["<b>Статус Infinity</b>\n"]
    for index, row in enumerate(rows, 1):
        state = "🟢 Активно" if row["is_enabled"] else "🔴 Отключено"
        try:
            rights = json.loads(row["rights_json"] or "{}")
        except json.JSONDecodeError:
            rights = {}
        read_access = rights.get("can_read_messages")
        access_text = (
            "доступ к сообщениям подтверждён"
            if read_access
            else "проверь разрешение на чтение сообщений"
        )
        lines.append(
            f"{index}. {state}\n"
            f"   ID: <code>{safe_html(row['connection_id'])}</code>\n"
            f"   {access_text}\n"
            f"   Подключено: {fmt_time(row['connected_at'])}"
        )
    await message.answer("\n\n".join(lines))


async def on_business_connection(connection: BusinessConnection) -> None:
    try:
        await store_connection(connection)
        log.info(
            "Business connection state updated: enabled=%s",
            connection.is_enabled,
        )
    except Exception as exc:
        log.exception("Could not store Business connection (%s)", type(exc).__name__)


async def on_business_message(message: Message, bot: Bot) -> None:
    try:
        await save_business_message(bot, message)
    except Exception as exc:
        log.exception("Could not archive Business message (%s)", type(exc).__name__)


async def on_edited_business_message(message: Message, bot: Bot) -> None:
    try:
        await handle_edited_message(bot, message)
    except Exception as exc:
        log.exception("Could not process edited Business message (%s)", type(exc).__name__)


async def on_deleted_business_messages(
    deleted: BusinessMessagesDeleted, **kwargs: Any
) -> None:
    # aiogram passes the Telegram event positionally and Bot in context kwargs.
    # Avoid a named `bot` parameter to prevent positional/keyword collisions.
    bot = kwargs.get("bot")
    if not isinstance(bot, Bot):
        log.error("Bot instance is missing from aiogram handler context")
        return
    try:
        await handle_deleted_messages(bot, deleted)
    except Exception as exc:
        log.exception("Could not process deleted Business messages (%s)", type(exc).__name__)


async def main() -> None:
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_help, Command("help"))

    dp.business_connection.register(on_business_connection)
    dp.business_message.register(on_business_message)
    dp.edited_business_message.register(on_edited_business_message)
    dp.deleted_business_messages.register(on_deleted_business_messages)

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Запустить Infinity"),
            BotCommand(command="status", description="Статус Business-подключения"),
            BotCommand(command="help", description="Инструкция"),
        ]
    )

    cleanup_task = asyncio.create_task(cleanup_loop())
    try:
        # Preserve queued updates after restarts; aiogram polling reconnects with backoff.
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=True,
        )
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        if db is not None:
            await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Infinity stopped")
