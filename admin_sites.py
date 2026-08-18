import os
import asyncio
import time
import logging
import io
from aiogram import Router, F, types, Bot
from aiogram.filters import Command
from aiogram.types import BufferedInputFile

from mass_gates.sitechk import (
    SITES_FILE,
    check_site_status,
    normalize_url,
    read_sites,
    write_sites,
    API_BASE_URL,
)

router = Router()

# Admin IDs (must match the rest of the bot)
ADMIN_IDS = {8502412301, 8952038376, 7814400733}

# Premium Emoji IDs (provided by owner)
EMOJI_RED_TICK   = "6147565374289220368"
EMOJI_BLUE_TICK  = "5278628026416909103"
EMOJI_LIGHTNING  = "5219745609631674840"
EMOJI_STAR       = "5359686514697576863"
EMOJI_FIRE       = "6186076099764555777"
EMOJI_CROWN      = "6338940587193930733"
EMOJI_EPIC       = "6052994304715002242"
EMOJI_DRAGON     = "5440636718262804952"
EMOJI_GUN        = "5440406404936524730"
EMOJI_WHITE_STAR = "5247131412032670246"

MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB cap for uploaded .txt files
MAX_CONCURRENT_SITE_CHECKS = 15  # higher = faster site audits
PROGRESS_BAR_LEN = 16
MIN_EDIT_INTERVAL = 0.3  # seconds (Telegram edit rate-limit guard)

_SEP = "━━━━━━━━━━━━━━━━"


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def _gather_input_urls(message: types.Message) -> list:
    """Extract URL lines from /addsites command text, reply, or .txt document."""
    raw_text = ""
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        raw_text += parts[1].strip() + "\n"
    if message.reply_to_message:
        if message.reply_to_message.text:
            raw_text += message.reply_to_message.text + "\n"
        if message.reply_to_message.caption:
            raw_text += message.reply_to_message.caption + "\n"

    document = message.document
    if document:
        if document.file_size > MAX_FILE_SIZE:
            await message.reply(
                f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
                f"<b>File too large.</b> Max 5 MB.",
                parse_mode="HTML",
            )
            return []
        try:
            file = await message.bot.get_file(document.file_id)
            byte_content = await file.download_as_bytearray()
            raw_text += byte_content.decode("utf-8", errors="ignore")
        except Exception as e:
            await message.reply(
                f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
                f"<b>Error reading file:</b> <code>{e}</code>",
                parse_mode="HTML",
            )
            return []

    if not raw_text.strip():
        return []

    return [ln.strip() for ln in raw_text.strip().splitlines() if ln.strip()]


async def _check_with_progress(
    urls: list,
    status_msg: types.Message,
    header: str,
) -> list:
    """Run check_site_status in parallel, update status_msg with a live progress bar.

    Returns the results list in the SAME order as `urls`.
    Each element is the tuple (url, status, data, resp) from sitechk.check_site_status.
    """
    total = len(urls)
    sem = asyncio.Semaphore(MAX_CONCURRENT_SITE_CHECKS)

    async def run_one(idx: int, url: str):
        async with sem:
            try:
                return (idx, url, await check_site_status(url))
            except Exception:
                return (idx, url, (url, "ERROR", None, None))

    tasks = [asyncio.create_task(run_one(i, u)) for i, u in enumerate(urls)]

    indexed_results: list = [None] * total
    done = 0
    live = 0
    dead = 0
    errors = 0
    last_edit = 0.0

    async def render(force: bool = False):
        nonlocal last_edit
        now = time.monotonic()
        if not force and (now - last_edit) < MIN_EDIT_INTERVAL:
            return
        pct = int(100 * done / total) if total else 0
        filled = int(PROGRESS_BAR_LEN * done / total) if total else 0
        bar = "█" * filled + "░" * (PROGRESS_BAR_LEN - filled)
        blue = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
        red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
        lightning = f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⚡</tg-emoji>'
        text = (
            f"{lightning} <b>{header}</b>\n"
            f"{_SEP}\n"
            f"<b>Progress:</b> <code>[{bar}]</code> "
            f"<b>{done}/{total}</b> <i>({pct}%)</i>\n"
            f"{blue} <b>Live ➛</b> <b>{live}</b>   "
            f"{red} <b>Dead ➛</b> <b>{dead}</b>   "
            f"<b>Errors ➛</b> <b>{errors}</b>\n"
            f"{_SEP}\n"
            f"<i>Live updates...</i>"
        )
        try:
            await status_msg.edit_text(text, parse_mode="HTML")
            last_edit = now
        except Exception:
            pass

    await render(force=True)

    for fut in asyncio.as_completed(tasks):
        idx, url, res = await fut
        if isinstance(res, tuple) and len(res) >= 2:
            status = res[1]
        else:
            status = "ERROR"
        indexed_results[idx] = (url, status, res[2] if len(res) > 2 else None,
                                res[3] if len(res) > 3 else None)
        if status == "KEEP":
            live += 1
        elif status == "REMOVE":
            dead += 1
        else:
            errors += 1
        done += 1
        await render(force=(done == total))

    return indexed_results


def _build_caption_report(
    total: int,
    working: int,
    dead: int,
    duplicates: int,
    errors: int,
    pool_after: int,
    mode_label: str,
) -> str:
    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    fire  = f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji>'
    red   = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
    blue  = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
    epic  = f'<tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji>'
    gun   = f'<tg-emoji emoji-id="{EMOJI_GUN}">🔫</tg-emoji>'

    return (
        f"{crown} <b>𝗦𝗜𝗧𝗘 {mode_label} 𝗗𝗢𝗡𝗘</b>\n"
        f"{_SEP}\n"
        f"{fire} <b>Tested ➛</b> <code>{total}</code>\n"
        f"{blue} <b>Working ➛</b> <b>{working}</b>\n"
        f"{red} <b>Dead ➛</b> <b>{dead}</b>\n"
        f"{gun} <b>Duplicates skipped ➛</b> <b>{duplicates}</b>\n"
        f"<b>Errors ➛</b> <b>{errors}</b>\n"
        f"{epic} <b>Pool now ➛</b> <b>{pool_after}</b> sites\n"
        f"{_SEP}\n"
        f"{crown} <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /addsites — Add new sites (check via API+proxies, skip duplicates)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.message(Command("addsites"))
async def addsites_command(message: types.Message):
    if not _is_admin(message.from_user.id):
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\nAdmin only command.",
            parse_mode="HTML",
        )
        return

    urls = await _gather_input_urls(message)
    if not urls:
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>No URLs found in input.</b>\n\n"
            f"Send <code>/addsites https://site1.myshopify.com https://site2.myshopify.com</code>\n"
            f"or reply to a .txt file with <code>/addsites</code>.",
            parse_mode="HTML",
        )
        return

    # Dedup against existing pool (normalized)
    existing_sites = await asyncio.to_thread(read_sites)
    existing_norm = {normalize_url(s) for s in existing_sites}

    # Dedup the input itself
    seen = set()
    unique_urls = []
    for u in urls:
        n = normalize_url(u)
        if n in seen:
            continue
        seen.add(n)
        unique_urls.append(u)

    duplicate_count = sum(1 for u in unique_urls if normalize_url(u) in existing_norm)
    new_urls = [u for u in unique_urls if normalize_url(u) not in existing_norm]

    if not new_urls:
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji> '
            f"<b>All {duplicate_count} URLs are already in sites.txt.</b>\n"
            f"Nothing to add.",
            parse_mode="HTML",
        )
        return

    lightning = f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⚡</tg-emoji>'
    dragon = f'<tg-emoji emoji-id="{EMOJI_DRAGON}">🐉</tg-emoji>'
    status_msg = await message.reply(
        f"{lightning} <b>Checking new sites...</b>\n"
        f"{_SEP}\n"
        f"{dragon} <b>To test ➛</b> <code>{len(new_urls)}</code>\n"
        f"<b>Duplicates (skipped) ➛</b> <code>{duplicate_count}</code>\n"
        f"{_SEP}\n"
        f"<i>Initializing checker...</i>",
        parse_mode="HTML",
    )

    results = await _check_with_progress(
        new_urls,
        status_msg,
        header="CHECKING NEW SITES",
    )

    working_urls = []
    dead_urls = []
    error_urls = []
    for url, status, _data, _resp in results:
        if status == "KEEP":
            working_urls.append(url)
        elif status == "REMOVE":
            dead_urls.append(url)
        else:
            error_urls.append(url)

    # Append working sites to existing pool (preserve existing order)
    final_pool = list(existing_sites)
    for u in working_urls:
        nu = normalize_url(u)
        if nu not in {normalize_url(x) for x in final_pool}:
            final_pool.append(u)

    pool_after = await asyncio.to_thread(write_sites, final_pool)

    caption = _build_caption_report(
        total=len(new_urls),
        working=len(working_urls),
        dead=len(dead_urls),
        duplicates=duplicate_count,
        errors=len(error_urls),
        pool_after=pool_after,
        mode_label="ADD",
    )

    # Send .txt of working sites
    if working_urls:
        buf = io.BytesIO()
        for u in working_urls:
            buf.write((u + "\n").encode("utf-8"))
        buf.seek(0)
        live_file = BufferedInputFile(
            file=buf.read(),
            filename=f"working_sites_{len(working_urls)}.txt",
        )
        try:
            await status_msg.delete()
        except Exception:
            pass
        await message.reply_document(
            document=live_file,
            caption=caption,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    else:
        try:
            await status_msg.edit_text(caption, parse_mode="HTML", disable_web_page_preview=True)
        except Exception:
            await message.reply(caption, parse_mode="HTML", disable_web_page_preview=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /checksite — Audit existing sites (keep working, remove dead)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.message(Command("checksite"))
async def checksite_command(message: types.Message):
    if not _is_admin(message.from_user.id):
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\nAdmin only command.",
            parse_mode="HTML",
        )
        return

    sites = await asyncio.to_thread(read_sites)
    if not sites:
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>sites.txt is empty.</b>\n\n"
            f"Use <code>/addsites</code> to add new sites first.",
            parse_mode="HTML",
        )
        return

    lightning = f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⚡</tg-emoji>'
    dragon = f'<tg-emoji emoji-id="{EMOJI_DRAGON}">🐉</tg-emoji>'
    status_msg = await message.reply(
        f"{lightning} <b>Auditing sites.txt...</b>\n"
        f"{_SEP}\n"
        f"{dragon} <b>Total ➛</b> <code>{len(sites)}</code>\n"
        f"{_SEP}\n"
        f"<i>Initializing checker...</i>",
        parse_mode="HTML",
    )

    results = await _check_with_progress(
        sites,
        status_msg,
        header="AUDITING SITES",
    )

    working_urls = []
    dead_count = 0
    error_count = 0
    for url, status, _data, _resp in results:
        if status == "KEEP":
            working_urls.append(url)
        elif status == "REMOVE":
            dead_count += 1
        else:
            error_count += 1

    pool_after = await asyncio.to_thread(write_sites, working_urls)

    caption = _build_caption_report(
        total=len(sites),
        working=len(working_urls),
        dead=dead_count,
        duplicates=0,
        errors=error_count,
        pool_after=pool_after,
        mode_label="AUDIT",
    )

    # Send .txt of working sites
    if working_urls:
        buf = io.BytesIO()
        for u in working_urls:
            buf.write((u + "\n").encode("utf-8"))
        buf.seek(0)
        live_file = BufferedInputFile(
            file=buf.read(),
            filename=f"working_sites_{len(working_urls)}.txt",
        )
        try:
            await status_msg.delete()
        except Exception:
            pass
        await message.reply_document(
            document=live_file,
            caption=caption,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    else:
        try:
            await status_msg.edit_text(caption, parse_mode="HTML", disable_web_page_preview=True)
        except Exception:
            await message.reply(caption, parse_mode="HTML", disable_web_page_preview=True)
