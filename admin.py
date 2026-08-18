import os
import sys
import asyncio
import subprocess
import logging
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import is_unlimited_msh, set_unlimited_msh
from sub import _resolve_user_id_sync, _ensure_user_for_admin_sync
from mass_gates.msh import MshForceStopCallback

router = Router()

# Admin IDs (must match the rest of the bot)
ADMIN_IDS = {7990874437,7634077852}

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

# Stop button emoji (matches the one used inside /msh progress UI)
BTN_STOP_EMOJI_ID = "6179444193518162239"

_SEP = "━━━━━━━━━━━━━━━━"

# Admin command groups: (category, emoji_id, [(cmd, description), ...])
ADMIN_GROUPS = [
    (
        "User Management",
        EMOJI_GUN,
        [
            ("/ban",      "&lt;user_id&gt;  ➛  Ban a user"),
            ("/unban",    "&lt;user_id&gt;  ➛  Unban a user"),
            ("/sub",      "&lt;user|@username&gt; &lt;plan&gt;  ➛  Manual subscribe (core/elite/root)"),
            ("/rsub",     "&lt;user|@username&gt;  ➛  Remove subscription"),
            ("/suball",   "➛  List every premium subscriber"),
            ("/unlimitedchk", "&lt;user|@username&gt; [on|off]  ➛  Toggle unlimited /msh for a user"),
        ],
    ),
    (
        "Credits &amp; Plans",
        EMOJI_FIRE,
        [
            ("/adcr",     "&lt;user_id&gt; &lt;amount&gt;  ➛  Add credits"),
            ("/rc",       "&lt;receipt_id&gt;  ➛  Remove credits via receipt"),
            ("/gencodes", "&lt;count&gt; &lt;days&gt;  ➛  Generate premium access code"),
            ("/sessions", "  ➛  View &amp; force-stop active MSH sessions"),
        ],
    ),
    (
        "Broadcast &amp; Status",
        EMOJI_EPIC,
        [
            ("/broad",    "➛  Reply to any msg → broadcast to all users"),
            ("/vps",      "➛  Server health (RAM, CPU, disk, uptime, proxy pool)"),
            ("/fb",       "➛  Reply to a hit → submit feedback (approval in channel)"),
        ],
    ),
    (
        "Sites",
        EMOJI_DRAGON,
        [
            ("/addsites", "➛  Check new URLs via API+proxies → add live, skip duplicates"),
            ("/checksite","➛  Audit sites.txt → keep live, remove dead"),
            ("/removeall","➛  Wipe every site from sites.txt at once"),
        ],
    ),
    (
        "Proxy Pool",
        EMOJI_LIGHTNING,
        [
            ("/prx",      "➛  Check pool → remove dead, keep live"),
            ("/prx",      "&lt;ip:port:user:pass ...&gt;  ➛  Check + add live ones to pool"),
            ("/prx",      "(reply to .txt)  ➛  Check + add live ones to pool"),
        ],
    ),
    (
        "System",
        EMOJI_CROWN,
        [
            ("/restart",  "➛  Restart the bot (spawns fresh process & exits current one)"),
        ],
    ),
] 


def _build_admin_text() -> str:
    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    blue_tick = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
    gun = f'<tg-emoji emoji-id="{EMOJI_GUN}">🔫</tg-emoji>'
    epic = f'<tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji>'
    fire = f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji>'

    lines = [
        f"{crown} <b>𝗔𝗗𝗠𝗜𝗡 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦</b> {crown}",
        _SEP,
        f"{gun} <b>Restricted:</b> Admin IDs only",
        f"{blue_tick} <b>Access:</b> Granted",
        _SEP,
    ]

    for cat_name, cat_emoji_id, entries in ADMIN_GROUPS:
        cat_emoji = f'<tg-emoji emoji-id="{cat_emoji_id}">✨</tg-emoji>'
        lines.append(f"{cat_emoji} <b>{cat_name}</b>")
        for cmd, desc in entries:
            lines.append(f"  <b>{fire} {cmd}</b>")
            lines.append(f"     <i>{desc}</i>")
        lines.append(_SEP)

    lines.append(
        f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> '
        f'<b><a href="https://t.me/blacklistedcarder1">Blacklisted Carder</a></b> '
        f'{epic}'
    )
    return "\n".join(lines)


def _build_admin_kb() -> InlineKeyboardMarkup:
    rows = []
    # Quick action shortcuts
    rows.append([
        InlineKeyboardButton(
            text="📊 𝗩𝗣𝗦",
            callback_data="admin_quick_vps",
            style="primary",
            icon_custom_emoji_id=EMOJI_LIGHTNING,
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            text="💎 𝗚𝗲𝗻𝗲𝗿𝗮𝘁𝗲 𝗖𝗼𝗱𝗲",
            callback_data="admin_quick_gcode",
            style="success",
            icon_custom_emoji_id=EMOJI_FIRE,
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            text="📣 𝗕𝗿𝗼𝗮𝗱𝗰𝗮𝘀𝘁",
            callback_data="admin_quick_broad",
            style="primary",
            icon_custom_emoji_id=EMOJI_EPIC,
        ),
        InlineKeyboardButton(
            text="⭐ 𝗦𝘂𝗯𝗮𝗹𝗹",
            callback_data="admin_quick_suball",
            style="primary",
            icon_custom_emoji_id=EMOJI_WHITE_STAR,
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            text="🔍 𝗔𝘂𝗱𝗶𝘁 𝗦𝗶𝘁𝗲𝘀",
            callback_data="admin_quick_checksite",
            style="primary",
            icon_custom_emoji_id=EMOJI_DRAGON,
        ),
        InlineKeyboardButton(
            text="⚡ 𝗣𝗿𝘅 𝗣𝗼𝗼𝗹",
            callback_data="admin_quick_prx",
            style="primary",
            icon_custom_emoji_id=EMOJI_LIGHTNING,
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            text="🔄 𝗥𝗲𝘀𝘁𝗮𝗿𝘁",
            callback_data="admin_quick_restart",
            style="danger",
            icon_custom_emoji_id=EMOJI_LIGHTNING,
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            text="« 𝗕𝗮𝗰𝗸",
            callback_data="back_main",
            style="danger",
            icon_custom_emoji_id=EMOJI_RED_TICK,
        ),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


_ADMIN_TEXT = _build_admin_text()
_ADMIN_KB = _build_admin_kb()


@router.message(Command("admin"))
async def admin_command(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
        await message.reply(
            f"{red} <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\n"
            f"This command is restricted to admins only.",
            parse_mode="HTML",
        )
        return
    await message.reply(
        text=_ADMIN_TEXT,
        parse_mode="HTML",
        reply_markup=_ADMIN_KB,
        disable_web_page_preview=True,
    )


# Quick action buttons — all admin-only
@router.callback_query(F.data == "admin_quick_vps")
async def admin_vps_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer("💎 Tap /vps to view server status.", show_alert=True)


@router.callback_query(F.data == "admin_quick_gcode")
async def admin_gcode_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer(
        "💎 Use:\n<code>/gencodes 10 1</code>\n"
        "→ 10 premium codes, 1 day each",
        show_alert=True,
    )


@router.callback_query(F.data == "admin_quick_broad")
async def admin_broad_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer(
        "📣 Reply to any message with /broad to broadcast.",
        show_alert=True,
    )


@router.callback_query(F.data == "admin_quick_suball")
async def admin_suball_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer("⭐ Tap /suball to list subscribers.", show_alert=True)


@router.callback_query(F.data == "admin_quick_checksite")
async def admin_checksite_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer("🔍 Tap /checksite to audit sites.txt.", show_alert=True)


@router.callback_query(F.data == "admin_quick_prx")
async def admin_prx_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer(
        "⚡ Use:\n"
        "<code>/prx</code> — check pool\n"
        "<code>/prx ip:port:user:pass ...</code> — add\n"
        "or reply to a .txt file with /prx",
        show_alert=True,
    )


@router.callback_query(F.data == "admin_quick_restart")
async def admin_restart_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    await callback.answer(
        "🔄 Tap /restart to spawn a fresh bot process & exit the current one.",
        show_alert=True,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /restart — Spawn a fresh bot process and exit current one
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.message(Command("restart"))
async def restart_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
        await message.reply(
            f"{red} <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\n"
            f"Admin only command.",
            parse_mode="HTML",
        )
        return

    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    lightning = f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⚡</tg-emoji>'
    epic = f'<tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji>'

    await message.reply(
        f"{crown} <b>𝗥𝗘𝗦𝗧𝗔𝗥𝗧𝗜𝗡𝗚 𝗕𝗢𝗧...</b>\n"
        f"{_SEP}\n"
        f"{lightning} <b>Stopping polling & spawning fresh process.</b>\n"
        f"{epic} <i>Bot will be back online in a moment.</i>\n"
        f"{_SEP}\n"
        f"{crown} <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    # Stop the current polling loop FIRST so Telegram releases the getUpdates
    # slot. Otherwise the new process and the old one collide → ConflictError.
    try:
        await router.parent_dispatcher.stop_polling()
    except Exception:
        try:
            from main import dp as _main_dp
            await _main_dp.stop_polling()
        except Exception:
            pass

    # Give Telegram a moment to release the slot before the new process polls
    await asyncio.sleep(2.0)

    args = [sys.executable] + sys.argv
    cwd = os.getcwd()

    try:
        if os.name == "nt":
            subprocess.Popen(
                args,
                cwd=cwd,
                stdin=None,
                stdout=None,
                stderr=None,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                | subprocess.DETACHED_PROCESS,
                close_fds=True,
            )
        else:
            subprocess.Popen(
                args,
                cwd=cwd,
                stdin=None,
                stdout=None,
                stderr=None,
                start_new_session=True,
                close_fds=True,
            )
    except Exception as e:
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>Restart failed:</b> <code>{e}</code>",
            parse_mode="HTML",
        )
        return

    os._exit(0)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /unlimitedchk — Toggle unlimited /msh for a user
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Usage:
#   /unlimitedchk <user_id|@username>          → toggle on/off
#   /unlimitedchk <user_id|@username> on       → force ON
#   /unlimitedchk <user_id|@username> off      → force OFF
#
# When ON, that user can run /msh with any number of cards and
# no credits are deducted (admins already have this by default).
@router.message(Command("unlimitedchk"))
async def unlimitedchk_command(message: types.Message):
    red   = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
    green = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
    star  = f'<tg-emoji emoji-id="{EMOJI_WHITE_STAR}">⭐</tg-emoji>'

    if message.from_user.id not in ADMIN_IDS:
        await message.reply(
            f"{red} <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\nAdmin only command.",
            parse_mode="HTML",
        )
        return

    args = message.text.split()[1:]
    if not args:
        await message.reply(
            f"{red} <b>𝗨𝘀𝗮𝗴𝗲:</b>\n"
            f"<code>/unlimitedchk &lt;user_id|@username&gt;</code> — toggle\n"
            f"<code>/unlimitedchk &lt;user_id|@username&gt; on</code> — force ON\n"
            f"<code>/unlimitedchk &lt;user_id|@username&gt; off</code> — force OFF",
            parse_mode="HTML",
        )
        return

    target_input = args[0]
    explicit = args[1].lower() if len(args) > 1 else None
    if explicit not in (None, "on", "off"):
        await message.reply(
            f"{red} <b>Invalid flag:</b> <code>{html_escape(explicit)}</code>\n"
            f"Use <code>on</code> or <code>off</code>, or omit to toggle.",
            parse_mode="HTML",
        )
        return

    # Resolve target → user_id (numeric or @username)
    target_id = await asyncio.to_thread(_resolve_user_id_sync, target_input)
    if not target_id and target_input.startswith("@"):
        try:
            chat = await message.bot.get_chat(target_input)
            target_id = chat.id
            try:
                await asyncio.to_thread(
                    _ensure_user_for_admin_sync,
                    target_id,
                    chat.username or "",
                    chat.first_name or "User",
                )
            except Exception as db_e:
                logging.error(f"[unlimitedchk] pre-create user failed: {db_e}")
        except Exception as tg_e:
            logging.warning(f"[unlimitedchk] could not resolve @username: {tg_e}")
    if not target_id:
        await message.reply(
            f"{red} <b>𝗖𝗼𝘂𝗹𝗱 𝗻𝗼𝘁 𝗳𝗶𝗻𝗱 𝘂𝘀𝗲𝗿.</b>\n"
            f"User must have <code>/start</code>ed the bot, or be reachable via <code>@username</code>.",
            parse_mode="HTML",
        )
        return

    # Decide new value
    current = await asyncio.to_thread(is_unlimited_msh, target_id)
    if explicit == "on":
        new_val = True
    elif explicit == "off":
        new_val = False
    else:
        new_val = not current

    # Admins are already unlimited — clarify that to the admin
    if target_id in ADMIN_IDS and not new_val:
        await message.reply(
            f"{star} <b>Note:</b> <code>{target_id}</code> is an admin and already has unlimited /msh.\n"
            f"Toggle ignored (admins can't be downgraded by this command).",
            parse_mode="HTML",
        )
        return

    await asyncio.to_thread(set_unlimited_msh, target_id, new_val)

    # Friendly display name
    display_name = str(target_id)
    try:
        chat = await message.bot.get_chat(target_id)
        display_name = (
            f'<a href="tg://user?id={target_id}">{html_escape(chat.first_name or chat.username or "User")}</a>'
        )
    except Exception:
        display_name = f'<code>{target_id}</code>'

    state_txt = "𝗘𝗡𝗔𝗕𝗟𝗘𝗗 ✅" if new_val else "𝗗𝗜𝗦𝗔𝗕𝗟𝗘𝗗 ❌"
    icon = green if new_val else red

    # Notify the user via DM (best-effort)
    try:
        await message.bot.send_message(
            chat_id=target_id,
            text=(
                f"{icon} <b>𝗠𝗦𝗛 𝗨𝗻𝗹𝗶𝗺𝗶𝘁𝗲𝗱 ➛ {state_txt}</b>\n\n"
                + ("You can now run <code>/msh</code> with any number of cards — no credits will be deducted."
                   if new_val else
                   "Your <code>/msh</code> unlimited access has been revoked. Credits will now be deducted as usual.")
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.warning(f"[unlimitedchk] could not DM user {target_id}: {e}")

    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    await message.reply(
        f"{icon} <b>𝗠𝗦𝗛 𝗨𝗻𝗹𝗶𝗺𝗶𝘁𝗲𝗱 ➛ {state_txt}</b>\n"
        f"{_SEP}\n"
        f"𝗨𝘀𝗲𝗿 ➛ {display_name}\n"
        f"<code>{target_id}</code>\n"
        f"𝗣𝗿𝗲𝘃𝗶𝗼𝘂𝘀 ➛ {'✅ ON' if current else '❌ OFF'}\n"
        f"𝗡𝗲𝘄 ➛ {'✅ ON' if new_val else '❌ OFF'}\n"
        f"{_SEP}\n"
        f"{crown}",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


def html_escape(s: str) -> str:
    """Minimal HTML escape for safe insertion into parse_mode='HTML' text."""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /sessions — List active /msh sessions + admin force-stop
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_sessions_text_and_kb():
    """
    Build the /sessions list payload from MSH_SESSIONS.
    Returns (text, InlineKeyboardMarkup | None).
    """
    import time as _time
    from mass_gates.msh import MSH_SESSIONS, MshForceStopCallback

    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    fire  = f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji>'
    red   = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'

    checking = [(sid, s) for sid, s in MSH_SESSIONS.items() if s.get("status") == "CHECKING"]
    finished = [(sid, s) for sid, s in MSH_SESSIONS.items() if s.get("status") in ("STOPPED", "FINISHED")]
    checking.sort(key=lambda kv: kv[1].get("start_time", 0), reverse=True)
    finished.sort(key=lambda kv: kv[1].get("start_time", 0), reverse=True)
    finished = finished[:5]

    lines = [f"{crown} <b>𝗔𝗰𝘁𝗶𝘃𝗲 𝗦𝗲𝘀𝘀𝗶𝗼𝗻𝘀</b>", _SEP]

    if checking:
        lines.append(f"{fire} <b>𝗠𝗦𝗛 𝗥𝘂𝗻𝗻𝗶𝗻𝗴 ➛</b> <code>{len(checking)}</code>")
        for idx, (sid, sess) in enumerate(checking, 1):
            user_obj = sess.get("user_obj")
            uid = sess.get("user_id", "?")
            name = html_escape(getattr(user_obj, "first_name", "") or "User")
            uname = getattr(user_obj, "username", None)
            user_link = (
                f'<a href="https://t.me/{html_escape(uname)}">{name}</a>'
                if uname else f'<a href="tg://user?id={uid}">{name}</a>'
            )

            checked  = sess.get("checked", 0)
            total    = sess.get("total", 0)
            approved = sess.get("approved", 0)
            charged  = sess.get("charged", 0)

            start_t = sess.get("start_time", 0)
            if start_t:
                elapsed = int(_time.time() - start_t)
                elapsed_str = f"{elapsed // 60}m {elapsed % 60}s" if elapsed >= 60 else f"{elapsed}s"
            else:
                elapsed_str = "—"

            lines.append(
                f"<b>{idx}.</b> {user_link} <code>{uid}</code>\n"
                f"    <b>Session ➛</b> <code>{sid}</code>\n"
                f"    <b>Progress ➛</b> <code>{checked}/{total}</code> · "
                f"✅<b>{approved}</b> 💎<b>{charged}</b>\n"
                f"    <b>Running ➛</b> <i>{elapsed_str}</i>"
            )
    else:
        lines.append(f"{red} <b>No active MSH sessions.</b>")

    lines.append(_SEP)

    if finished:
        lines.append(f"<i>Recent finished / stopped (last {len(finished)}):</i>")
        for idx, (sid, sess) in enumerate(finished, 1):
            user_obj = sess.get("user_obj")
            uid = sess.get("user_id", "?")
            name = html_escape(getattr(user_obj, "first_name", "") or "User")
            uname = getattr(user_obj, "username", None)
            user_link = (
                f'<a href="https://t.me/{html_escape(uname)}">{name}</a>'
                if uname else f'<a href="tg://user?id={uid}">{name}</a>'
            )
            lines.append(
                f"<b>{idx}.</b> {user_link} <code>{uid}</code> · "
                f"<code>{sid}</code> · <i>{sess.get('status', '?')}</i>"
            )
        lines.append(_SEP)

    lines.append(f"{crown} <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>")

    rows = []
    for sid, _ in checking:
        rows.append([
            InlineKeyboardButton(
                text=f"🛑 Stop {sid}",
                callback_data=MshForceStopCallback(session_id=sid).pack(),
                style="danger",
                icon_custom_emoji_id=BTN_STOP_EMOJI_ID,
            ),
        ])
    rows.append([
        InlineKeyboardButton(
            text="🔄 Refresh",
            callback_data="admin_refresh_sessions",
            style="primary",
            icon_custom_emoji_id=EMOJI_LIGHTNING,
        ),
    ])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("sessions"))
async def sessions_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply(
            f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji> '
            f"<b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱.</b>\n\nAdmin only command.",
            parse_mode="HTML",
        )
        return

    text, kb = _build_sessions_text_and_kb()
    await message.reply(
        text,
        parse_mode="HTML",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == "admin_refresh_sessions")
async def admin_refresh_sessions_cb(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return
    try:
        text, kb = _build_sessions_text_and_kb()
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        # Count running sessions for the alert text
        running = text.count("MSH Running")  # cheap heuristic
        await callback.answer("🔄 Refreshed")
    except Exception as e:
        try:
            await callback.answer(f"❌ {e}", show_alert=True)
        except Exception:
            pass


@router.callback_query(MshForceStopCallback.filter())
async def admin_force_stop_cb(callback: types.CallbackQuery, callback_data: MshForceStopCallback):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Admin only", show_alert=True)
        return

    from mass_gates.msh import MSH_SESSIONS, update_progress_message

    sid = callback_data.session_id
    sess = MSH_SESSIONS.get(sid)
    if not sess:
        try:
            await callback.answer(f"❌ Session {sid} not found", show_alert=True)
        except Exception:
            pass
        return

    if sess.get("status") in ("STOPPED", "FINISHED"):
        try:
            await callback.answer(f"⚠️ Session already {sess['status']}", show_alert=True)
        except Exception:
            pass
        # Still re-render so the button is gone if the session finished naturally
        try:
            text, kb = _build_sessions_text_and_kb()
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass
        return

    # ── Force-stop the session ──
    sess["status"] = "STOPPED"
    for task in sess.get("tasks", []):
        if not task.done():
            task.cancel()

    # Update the user's progress message so they see STOPPED
    try:
        await update_progress_message(callback.bot, sid)
    except Exception as e:
        logging.error(f"[admin-force-stop] update_progress_message failed: {e}")

    # Notify the admin
    try:
        user_obj = sess.get("user_obj")
        uid = sess.get("user_id", "?")
        name = getattr(user_obj, "first_name", "") or "User"
        await callback.answer(
            f"🛑 Stopped {sid}\n{name} ({uid})",
            show_alert=True,
        )
    except Exception:
        pass

    # Re-render the /sessions list with the stopped session removed
    try:
        text, kb = _build_sessions_text_and_kb()
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logging.error(f"[admin-force-stop] edit_text failed: {e}")
