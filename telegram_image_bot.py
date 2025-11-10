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
import logging
from collections import defaultdict
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


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

    return BotSettings(token=token, folders=folders, download_folder=download_folder)


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


async def list_folders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    manager: ImageManager = context.bot_data["image_manager"]
    if not manager.folders:
        await update.message.reply_text("No folders configured yet.")
        return

    lines = ["📁 Available folders:"]
    for name, path in manager.folders.items():
        lines.append(f"• {name} → {path}")
    await update.message.reply_text("\n".join(lines))


async def send_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    folder_key = context.args[0] if context.args else None
    await _send_image(update, context, folder_key, random_mode=False)


async def send_random(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    folder_key = context.args[0] if context.args else None
    await _send_image(update, context, folder_key, random_mode=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip().lower()
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


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Telegram error: %s", context.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("⚠️ Unexpected error occurred.")


def build_application(settings: BotSettings) -> Application:
    application = Application.builder().token(settings.token).build()
    application.bot_data["image_manager"] = ImageManager(settings.folders)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("folders", list_folders))
    application.add_handler(CommandHandler("next", send_next))
    application.add_handler(CommandHandler("random", send_random))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_error_handler(error_handler)
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


if __name__ == "__main__":
    main()

