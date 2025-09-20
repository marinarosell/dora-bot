import os
import sqlite3
import csv
import io
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from typing import Optional, Tuple, List

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated, MessageEntity, ReplyKeyboardMarkup
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ChatMemberHandler, filters, ContextTypes
)


# ---------------- Config & DB ----------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Madrid"))
MAX_HOURS = float(os.getenv("MAX_HOURS_WITHOUT_WALK", "6"))

# quiet hours (24h HH:MM)


def parse_hhmm(s):
    h, m = [int(x) for x in s.split(":")]
    return time(hour=h, minute=m)


QUIET_START = parse_hhmm(os.getenv("QUIET_START", "23:00"))
QUIET_END = parse_hhmm(os.getenv("QUIET_END", "07:30"))

DB_PATH = os.getenv("DB_PATH", "dora_telegram.db")


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS walks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            user_id INTEGER,
            user_name TEXT,
            ts_utc TEXT,
            poop TEXT
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS chats(
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            last_alert_utc TEXT
        )""")


init_db()


def now_utc():
    return datetime.now(tz=ZoneInfo("UTC"))


def last_walk_utc(chat_id: int) -> Optional[datetime]:
    with db() as conn:
        row = conn.execute(
            "SELECT ts_utc FROM walks WHERE chat_id=? ORDER BY ts_utc DESC LIMIT 1",
            (chat_id,)
        ).fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row["ts_utc"]).replace(tzinfo=ZoneInfo("UTC"))


def is_quiet(local_dt: datetime) -> bool:
    # quiet from QUIET_START to QUIET_END; supports crossing midnight
    start = local_dt.replace(
        hour=QUIET_START.hour, minute=QUIET_START.minute, second=0, microsecond=0)
    end = local_dt.replace(hour=QUIET_END.hour,
                           minute=QUIET_END.minute, second=0, microsecond=0)
    if QUIET_START <= QUIET_END:
        return start <= local_dt <= end
    else:
        # window wraps midnight
        return local_dt >= start or local_dt <= end

# ---------------- Core actions ----------------


async def log_walk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chats(chat_id, title) VALUES(?, ?)", (chat.id, chat.title))
        conn.execute(
            "INSERT INTO walks(chat_id, user_id, user_name, ts_utc, poop) VALUES(?, ?, ?, ?, NULL)",
            (chat.id, user.id, user.full_name, now_utc().isoformat())
        )
    await update.effective_message.reply_text(
        f"✅ Paseo por {user.first_name} guardado. Gracias!"
    )
    await send_poop_poll(update, context)


async def send_poop_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("👍 Normal", callback_data="poop_ok")],
        [InlineKeyboardButton("😕 Blanda", callback_data="poop_soft")],
        [InlineKeyboardButton("💧 Diarrea", callback_data="poop_diarrhea")],
        [InlineKeyboardButton("❌ No caca", callback_data="poop_none")],
    ]
    await update.effective_message.reply_text(
        "¿Cómo ha hecho la caca?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_poop_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    mapping = {
        "poop_ok": "Normal",
        "poop_soft": "Blanda",
        "poop_diarrhea": "Diarrea",
        "poop_none": "none"
    }
    val = mapping.get(q.data)
    if not val:
        return
    chat_id = q.message.chat_id
    user_id = q.from_user.id
    with db() as conn:
        conn.execute("""
            UPDATE walks
            SET poop=?
            WHERE id = (
                SELECT id FROM walks 
                WHERE chat_id=? AND user_id=?
                ORDER BY ts_utc DESC LIMIT 1
            )
        """, (val, chat_id, user_id))
    await q.edit_message_text(f"✅ Caca {val} guardada")

# ---------------- Stats & CSV ----------------


def chat_stats(chat_id: int) -> Tuple[int, Optional[datetime], Optional[datetime], float, dict]:
    with db() as conn:
        rows = conn.execute(
            "SELECT ts_utc, poop FROM walks WHERE chat_id=? ORDER BY ts_utc ASC",
            (chat_id,)
        ).fetchall()
    if not rows:
        return 0, None, None, 0.0, {}
    times = [datetime.fromisoformat(r["ts_utc"]).replace(
        tzinfo=ZoneInfo("UTC")) for r in rows]
    gaps = [(times[i] - times[i-1]).total_seconds() /
            3600.0 for i in range(1, len(times))]
    avg_gap = sum(gaps)/len(gaps) if gaps else 0.0
    poop_counts = {}
    for r in rows:
        poop_counts[r["poop"] or "unknown"] = poop_counts.get(
            r["poop"] or "unknown", 0) + 1
    return len(rows), times[0], times[-1], avg_gap, poop_counts


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    total, first, last, avg_gap, poop_counts = chat_stats(chat_id)
    if total == 0:
        await update.message.reply_text("No hay ningún paseo registrado aún.")
        return
    first_local = first.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
    last_local = last.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
    poop_str = ", ".join([f"{k}: {v}" for k, v in poop_counts.items()])
    await update.message.reply_text(
        f"📊 Paseos: {total}\n"
        f"Primero: {first_local}\n"
        f"Último: {last_local}\n"
        f"Tiempo medio entre paseos: {avg_gap:.1f} h\n"
        f"Cacas: {poop_str}"
    )


async def cmd_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with db() as conn:
        rows = conn.execute(
            "SELECT ts_utc, user_name, poop FROM walks WHERE chat_id=? ORDER BY ts_utc ASC",
            (chat_id,)
        ).fetchall()
    if not rows:
        await update.message.reply_text("No data to export.")
        return
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["timestamp_local", "timestamp_utc", "user", "poop"])
    for r in rows:
        ts_utc = datetime.fromisoformat(
            r["ts_utc"]).replace(tzinfo=ZoneInfo("UTC"))
        writer.writerow([ts_utc.astimezone(TZ).isoformat(timespec="minutes"),
                         ts_utc.isoformat(), r["user_name"], r["poop"] or ""])
    buf.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(buf.getvalue().encode()),
        filename="dora_walks.csv"
    )

# ---------------- Scheduling ----------------


async def overdue_check(context: ContextTypes.DEFAULT_TYPE):
    # called every 30 min by JobQueue
    for chat_id in list(context.application.chat_data.keys()):
        await maybe_alert_chat(context, int(chat_id))


async def maybe_alert_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    last = last_walk_utc(chat_id)
    if not last:
        return
    hours = (now_utc() - last).total_seconds() / 3600.0
    if hours < MAX_HOURS:
        return
    # throttle: one alert max every 6 hours
    with db() as conn:
        row = conn.execute(
            "SELECT last_alert_utc FROM chats WHERE chat_id=?", (chat_id,)).fetchone()
    last_alert = datetime.fromisoformat(row["last_alert_utc"]).replace(
        tzinfo=ZoneInfo("UTC")) if row and row["last_alert_utc"] else None

    local_now = now_utc().astimezone(TZ)
    if is_quiet(local_now):
        return
    if last_alert and (now_utc() - last_alert) < timedelta(hours=6):
        return

    text = f"⏰ Han pasado {hours:.1f}h desde la última salida de Dora. Alguien la puede sacar?"
    await context.bot.send_message(chat_id=chat_id, text=text)

    with db() as conn:
        conn.execute("UPDATE chats SET last_alert_utc=? WHERE chat_id=?",
                     (now_utc().isoformat(), chat_id))


async def daily_digest(context: ContextTypes.DEFAULT_TYPE):
    # simple morning digest for each chat the bot has seen
    for chat_id in list(context.application.chat_data.keys()):
        total, first, last, avg_gap, poop_counts = chat_stats(int(chat_id))
        if total == 0:
            continue
        last_s = last.astimezone(TZ).strftime("%H:%M %d-%m")
        poop_str = ", ".join([f"{k}: {v}" for k, v in poop_counts.items()])
        msg = (f"☀️ Resumen diario de ayer\n"
               f"Paseos: {total} | Last: {last_s}\n"
               f"Tiempo medio entre paseos: {avg_gap:.1f} h\n"
               f"Cacas: {poop_str}")
        await context.bot.send_message(chat_id=int(chat_id), text=msg)

# ---------------- Commands & message triggers ----------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # remember the chat so schedulers know where to post
    context.chat_data.setdefault(str(update.effective_chat.id), {})
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO chats(chat_id, title) VALUES(?, ?)",
                     (update.effective_chat.id, update.effective_chat.title))
    await update.message.reply_text(
        "Hola! Envía /paseo cuando saques a Dora, y luego selecciona cómo ha hecho la caca.")


async def cmd_walk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await log_walk(update, context)


async def keyword_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If privacy mode is OFF, detect messages like "walk", "paseo", etc.
    text = (update.message.text or "").strip().lower()
    triggers = {"walk", "out", "paseo", "salida",
                "he salido con dora", "sacado a dora"}
    if any(t in text.lower() for t in triggers):
        await log_walk(update, context)

# ---------------- Main ----------------


def main():
    app: Application = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("paseo", cmd_walk))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("csv", cmd_csv))
    app.add_handler(CallbackQueryHandler(handle_poop_vote, pattern=r"^poop_"))

    # Optional free-text trigger if privacy disabled
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, keyword_listener))

    # Schedule: overdue check every 30 min; daily digest at 08:30
    app.job_queue.run_repeating(overdue_check, interval=1800, first=60)
    app.job_queue.run_daily(
        daily_digest, time=time(hour=9, minute=00, tzinfo=TZ))

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
