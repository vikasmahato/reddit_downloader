#!/usr/bin/env python3
"""
Telegram Image Bot

This bot sends images from configured folders to any chat it is added to.
Configuration precedence:
1. Command line arguments
2. Environment variables
3. Entries in config.ini (section [telegram_bot])
4. Fallback to the downloader's default downloads folder

Recommended setup:
  set TELEGRAM_BOT_TOKEN=<token>
  python telegram_image_bot.py --config config.ini
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from collections import defaultdict
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    ChatMemberHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.error import Forbidden


IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".tiff",
    ".svg",
    ".jfif",
}

@dataclass
class BotSettings:
    token: str
    folders: Dict[str, Path]
    download_folder: Optional[Path] = None
    auto_send: "AutoSendSettings" = None
    chat_store_path: Path = None
    user_store_path: Path = None


@dataclass
class AutoSendSettings:
    enabled: bool
    interval_seconds: int
    mode: str  # "random" or "sequential"
    folders: List[str]


class ChatRegistry:
    def __init__(self, path: Path):
        self._path = path
        self._chats: Set[int] = set()
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = self._path.read_text(encoding="utf-8")
            if data:
                ids = json.loads(data)
                if isinstance(ids, list):
                    self._chats = {int(x) for x in ids}
        except Exception as exc:
            logging.warning("Failed to load chat registry: %s", exc)

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(sorted(self._chats)), encoding="utf-8")
        except Exception as exc:
            logging.warning("Failed to persist chat registry: %s", exc)

    async def add_chat(self, chat_id: int) -> None:
        async with self._lock:
            if chat_id not in self._chats:
                self._chats.add(chat_id)
                self._save()

    async def remove_chat(self, chat_id: int) -> None:
        async with self._lock:
            if chat_id in self._chats:
                self._chats.remove(chat_id)
                self._save()

    async def get_chats(self) -> List[int]:
        async with self._lock:
            return list(self._chats)


class UserRegistry:
    def __init__(self, path: Path):
        self._path = path
        self._users: Dict[int, Dict[str, object]] = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = self._path.read_text(encoding="utf-8")
            if data:
                raw_users = json.loads(data)
                if isinstance(raw_users, dict):
                    self._users = {int(k): v for k, v in raw_users.items()}
                elif isinstance(raw_users, list):
                    self._users = {int(item["id"]): item for item in raw_users if "id" in item}
        except Exception as exc:
            logging.warning("Failed to load user registry: %s", exc)

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = {str(user_id): data for user_id, data in self._users.items()}
            self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logging.warning("Failed to persist user registry: %s", exc)

    async def add_or_update_user(
        self,
        user_data: Dict[str, object],
        chat_info: Optional[Dict[str, object]] = None,
    ) -> None:
        user_id = int(user_data["id"])
        async with self._lock:
            existing = self._users.get(user_id, {})
            merged = {**existing, **user_data}

            merged["interaction_count"] = int(existing.get("interaction_count", 0)) + 1
            merged["last_interaction"] = datetime.utcnow().isoformat() + "Z"

            chats: List[Dict[str, object]] = existing.get("chats", [])
            if chat_info:
                chats = _merge_chat_info(chats, chat_info)
            merged["chats"] = chats

            self._users[user_id] = merged
            self._save()

    async def get_users(self) -> List[Dict[str, object]]:
        async with self._lock:
            return list(self._users.values())


class ImageManager:
    def __init__(self, folders: Dict[str, Path]):
        self._folders = folders
        self._positions: Dict[Tuple[int, str], int] = defaultdict(int)
        self._lock = asyncio.Lock()

    @property
    def folders(self) -> Dict[str, Path]:
        return self._folders

    async def get_next_image(self, folder_key: Optional[str], chat_id: int) -> Path:
        async with self._lock:
            key = self._resolve_folder_key(folder_key)
            files = self._scan_folder(self._folders[key])
            if not files:
                raise FileNotFoundError(f"No images found in folder '{key}'")

            position_key = (chat_id, key)
            index = self._positions[position_key] % len(files)
            image_path = files[index]
            self._positions[position_key] = (index + 1) % len(files)
            return image_path

    async def get_random_image(self, folder_key: Optional[str]) -> Path:
        async with self._lock:
            key = self._resolve_folder_key(folder_key)
            files = self._scan_folder(self._folders[key])
            if not files:
                raise FileNotFoundError(f"No images found in folder '{key}'")
            import random

            return random.choice(files)

    def _resolve_folder_key(self, folder_key: Optional[str]) -> str:
        if folder_key is None:
            if not self._folders:
                raise ValueError("No folders configured")
            return next(iter(self._folders))

        normalized = folder_key.strip().lower()
        for name in self._folders:
            if name.lower() == normalized:
                return name
        available = ", ".join(self._folders.keys()) or "None"
        raise KeyError(f"Folder '{folder_key}' not found. Available: {available}")

    def _scan_folder(self, folder: Path) -> List[Path]:
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder}")
        files: List[Path] = []
        for path in sorted(folder.rglob("*")):
            if path.suffix.lower() in IMAGE_EXTENSIONS and path.is_file():
                files.append(path)
        return files


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Telegram bot to share images from folders.")
    parser.add_argument("--config", default="telegram_bot.ini", help="Path to configuration file.")
    return parser


def resolve_bot_settings(args: argparse.Namespace) -> BotSettings:
    config = ConfigParser()
    config_path = Path(args.config).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config.read(config_path)

    token = resolve_token(config)
    folders = resolve_folders(config, config_path.parent)
    auto_send = resolve_auto_send(config, folders)

    download_folder = None
    if config.has_section("general"):
        default_download = config.get("general", "download_folder", fallback="downloads")
        download_folder = (config_path.parent / default_download).resolve()

    if not folders:
        if download_folder:
            folders = {"downloads": download_folder}
        else:
            raise ValueError(
                "No folders configured. Add a [folders] section to the config file."
            )

    chat_store = config.get(
        "auto_send", "chat_store", fallback="telegram_bot_chats.json"
    )
    chat_store_path = (config_path.parent / chat_store).resolve()

    user_store = config.get("bot", "user_store", fallback="telegram_bot_users.json")
    user_store_path = (config_path.parent / user_store).resolve()

    return BotSettings(
        token=token,
        folders=folders,
        download_folder=download_folder,
        auto_send=auto_send,
        chat_store_path=chat_store_path,
        user_store_path=user_store_path,
    )


def resolve_token(config: ConfigParser) -> str:
    if not config.has_section("bot"):
        raise ValueError("Missing [bot] section in config file.")
    token = config.get("bot", "token", fallback="").strip()
    if not token:
        raise ValueError("Bot token missing in config file under [bot].")
    return token


def resolve_folders(config: ConfigParser, base_dir: Path) -> Dict[str, Path]:
    folder_mapping: Dict[str, Path] = {}
    if config.has_section("folders"):
        for name, path_value in config.items("folders"):
            label, path = parse_folder_entry(f"{name}={path_value}", base_dir)
            unique_name = ensure_unique_name(label, folder_mapping)
            folder_mapping[unique_name] = path
    return folder_mapping


def resolve_auto_send(config: ConfigParser, folders: Dict[str, Path]) -> AutoSendSettings:
    if not config.has_section("auto_send"):
        return AutoSendSettings(False, 0, "random", [])

    enabled = config.getboolean("auto_send", "enabled", fallback=False)
    interval = config.getint("auto_send", "interval_seconds", fallback=3600)
    mode = config.get("auto_send", "mode", fallback="random").strip().lower()
    if mode not in {"random", "sequential"}:
        raise ValueError("auto_send.mode must be either 'random' or 'sequential'")

    folder_list_raw = config.get("auto_send", "folders", fallback="")
    folder_names = [
        name.strip()
        for name in folder_list_raw.replace("\n", ",").split(",")
        if name.strip()
    ]
    # Validate specified folders
    validated = []
    for name in folder_names:
        if name not in folders:
            raise ValueError(f"auto_send folder '{name}' not found in [folders] section")
        validated.append(name)

    return AutoSendSettings(enabled, interval, mode, validated)


def parse_folder_entry(entry: str, base_dir: Path) -> Tuple[str, Path]:
    raw = entry.strip()
    if not raw:
        raise ValueError("Empty folder entry.")
    if "=" in raw:
        label, path_str = raw.split("=", 1)
        label = label.strip() or None
        path_candidate = path_str.strip()
    else:
        label = None
        path_candidate = raw
    path = Path(path_candidate).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    if label is None:
        label = path.name or "folder"
    return label, path


def ensure_unique_name(name: str, existing: Dict[str, Path]) -> str:
    if name not in existing:
        return name
    counter = 2
    while True:
        candidate = f"{name}_{counter}"
        if candidate not in existing:
            return candidate
        counter += 1


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_user(update, context)
    manager: ImageManager = context.bot_data["image_manager"]
    folders = ", ".join(manager.folders.keys())
    message = (
        "👋 Hi! I'm your backup image bot.\n\n"
        "Use /folders to see available folders.\n"
        "Use /next <folder> to receive the next image from a folder.\n"
        "Use /random <folder> to receive a random image.\n\n"
        "If no folder is provided, I use the default."
    )
    if folders:
        message += f"\n\nAvailable folders: {folders}"
    await update.message.reply_text(message)
    await register_chat(update, context)


async def list_folders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_user(update, context)
    manager: ImageManager = context.bot_data["image_manager"]
    if not manager.folders:
        await update.message.reply_text("No folders configured yet.")
        return

    lines = ["📁 Available folders:"]
    for name, path in manager.folders.items():
        lines.append(f"• {name} → {path}")
    await update.message.reply_text("\n".join(lines))
    await register_chat(update, context)


async def send_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_user(update, context)
    folder_key = context.args[0] if context.args else None
    await register_chat(update, context)
    await _send_image(update, context, folder_key, random_mode=False)


async def send_random(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_user(update, context)
    folder_key = context.args[0] if context.args else None
    await register_chat(update, context)
    await _send_image(update, context, folder_key, random_mode=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_user(update, context)
    text = (update.message.text or "").strip().lower()
    await register_chat(update, context)
    if text in {"next", "send"}:
        await send_next(update, context)
    elif text in {"random", "shuffle"}:
        await send_random(update, context)
    else:
        await update.message.reply_text(
            "I can send images! Try /folders, /next, or /random."
        )


async def _send_image(
    update: Update, context: ContextTypes.DEFAULT_TYPE, folder_key: Optional[str], random_mode: bool
) -> None:
    manager: ImageManager = context.bot_data["image_manager"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    if not chat_id:
        return

    message = update.effective_message
    if not message:
        return

    try:
        await message.chat.send_action(action=ChatAction.UPLOAD_PHOTO)
        if random_mode:
            image_path = await manager.get_random_image(folder_key)
        else:
            image_path = await manager.get_next_image(folder_key, chat_id)

        caption_parts = [
            f"📸 {image_path.name}",
            f"Folder: {resolve_display_name(image_path, manager.folders)}",
        ]
        with image_path.open("rb") as image_file:
            await message.reply_photo(
                photo=image_file,
                caption="\n".join(caption_parts),
            )
    except KeyError as exc:
        await message.reply_text(str(exc))
    except FileNotFoundError as exc:
        await message.reply_text(str(exc))
    except Exception as exc:
        logging.exception("Failed to send image")
        await message.reply_text(f"⚠️ Something went wrong: {exc}")


def resolve_display_name(path: Path, folders: Dict[str, Path]) -> str:
    for name, folder_path in folders.items():
        try:
            path.relative_to(folder_path)
        except ValueError:
            continue
        return name
    return path.parent.name


async def register_user(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user_override=None,
) -> Optional[int]:
    user = user_override or update.effective_user
    if not user:
        return None
    registry: UserRegistry = context.bot_data["user_registry"]
    chat = update.effective_chat
    payload = {
        "id": user.id,
        "is_bot": user.is_bot,
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "username": user.username or "",
        "full_name": user.full_name if hasattr(user, "full_name") else "",
        "language_code": getattr(user, "language_code", None),
    }
    chat_info = None
    if chat:
        payload["last_chat_id"] = chat.id
        payload["last_chat_type"] = chat.type
        payload["last_chat_title"] = chat.title or ""
        chat_info = {
            "id": chat.id,
            "type": chat.type,
            "title": chat.title or "",
            "username": getattr(chat, "username", "") or "",
        }
    await registry.add_or_update_user(payload, chat_info)
    return user.id


def _merge_chat_info(
    existing: List[Dict[str, object]],
    new_info: Dict[str, object],
) -> List[Dict[str, object]]:
    new_id = int(new_info.get("id"))
    for idx, info in enumerate(existing):
        if int(info.get("id", 0)) == new_id:
            existing[idx] = {**info, **new_info}
            break
    else:
        existing.append(new_info)
    return existing


async def register_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    chat = update.effective_chat
    if not chat:
        return None
    registry: ChatRegistry = context.bot_data["chat_registry"]
    await registry.add_chat(chat.id)
    return chat.id


async def handle_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    member_update = update.my_chat_member
    if not chat or not member_update:
        return

    if member_update.from_user:
        await register_user(update, context, member_update.from_user)
    new_user = getattr(member_update.new_chat_member, "user", None)
    if new_user:
        await register_user(update, context, new_user)

    status = member_update.new_chat_member.status
    if status in {"member", "administrator", "creator"}:
        await register_chat(update, context)
    elif status in {"left", "kicked"}:
        registry: ChatRegistry = context.bot_data["chat_registry"]
        await registry.remove_chat(chat.id)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Telegram error: %s", context.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("⚠️ Unexpected error occurred.")


def build_application(settings: BotSettings) -> Application:
    application = Application.builder().token(settings.token).build()
    application.bot_data["image_manager"] = ImageManager(settings.folders)
    application.bot_data["auto_send_settings"] = settings.auto_send
    application.bot_data["chat_registry"] = ChatRegistry(settings.chat_store_path)
    application.bot_data["user_registry"] = UserRegistry(settings.user_store_path)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("folders", list_folders))
    application.add_handler(CommandHandler("next", send_next))
    application.add_handler(CommandHandler("random", send_random))
    application.add_handler(ChatMemberHandler(handle_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_error_handler(error_handler)

    schedule_auto_send(application)
    return application


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging()
    logging.info("Loading bot settings...")

    try:
        settings = resolve_bot_settings(args)
    except Exception as exc:
        logging.error("Configuration error: %s", exc)
        raise SystemExit(1) from exc

    application = build_application(settings)
    logging.info("Bot starting. Serving folders: %s", ", ".join(settings.folders.keys()))
    application.run_polling(stop_signals=None)


def schedule_auto_send(application: Application) -> None:
    auto_settings: AutoSendSettings = application.bot_data.get("auto_send_settings")
    if not auto_settings or not auto_settings.enabled:
        return

    interval = max(auto_settings.interval_seconds, 60)
    application.job_queue.run_repeating(
        auto_send_job,
        interval=interval,
        first=interval,
        name="auto-send-images",
    )


async def auto_send_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    auto_settings: AutoSendSettings = context.bot_data.get("auto_send_settings")
    manager: ImageManager = context.bot_data["image_manager"]
    registry: ChatRegistry = context.bot_data["chat_registry"]

    if not auto_settings or not auto_settings.enabled:
        return

    folders = auto_settings.folders or list(manager.folders.keys())
    if not folders:
        return

    chat_ids = await registry.get_chats()
    if not chat_ids:
        return

    for chat_id in chat_ids:
        for folder in folders:
            try:
                await context.bot.send_chat_action(chat_id, ChatAction.UPLOAD_PHOTO)
                if auto_settings.mode == "random":
                    image_path = await manager.get_random_image(folder)
                else:
                    image_path = await manager.get_next_image(folder, chat_id)

                caption = f"📸"
                with image_path.open("rb") as image_file:
                    await context.bot.send_photo(chat_id=chat_id, photo=image_file, caption=caption)
            except Forbidden:
                logging.info("Bot removed from chat %s; removing from registry.", chat_id)
                await registry.remove_chat(chat_id)
                break
            except FileNotFoundError as exc:
                logging.warning("Auto-send: %s", exc)
            except Exception as exc:
                logging.exception("Auto-send error for chat %s: %s", chat_id, exc)


if __name__ == "__main__":
    main()

