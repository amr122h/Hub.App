"""
StarHub Bot — Phase 1
FastAPI webhook + Postgres double-entry ledger.

Endpoints:
  GET  /            -> health check
  POST /webhook     -> Telegram updates

Commands: /start /daily /balance /invite /help

Env vars required:
  BOT_TOKEN        - from @BotFather
  WEBHOOK_SECRET   - any random string (A-Z a-z 0-9 _ -)
  DATABASE_URL     - postgres://... (Railway provides this)
"""

import os
import logging
import datetime as dt
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import (
    create_engine, text, BigInteger, String, DateTime, Integer, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ---------------------------------------------------------------- config

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Railway gives postgres:// — SQLAlchemy wants postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DAILY_REWARD = 100          # Shards per daily claim
REFERRAL_BONUS = 250        # Shards to referrer
REFERRAL_WELCOME = 50       # Shards to new user

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("starhub")

# ---------------------------------------------------------------- db

Base = declarative_base()
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
Session = sessionmaker(bind=engine, future=True)


class Account(Base):
    """One row per user. Balance = SUM(ledger.amount) for this account."""
    __tablename__ = "accounts"
    id = __import__("sqlalchemy").Column(BigInteger, primary_key=True)  # telegram user id
    username = __import__("sqlalchemy").Column(String(64), nullable=True)
    first_name = __import__("sqlalchemy").Column(String(128), nullable=True)
    referred_by = __import__("sqlalchemy").Column(BigInteger, nullable=True)
    last_daily = __import__("sqlalchemy").Column(DateTime, nullable=True)
    created_at = __import__("sqlalchemy").Column(DateTime, default=dt.datetime.utcnow)


class Ledger(Base):
    """Append-only double-entry ledger. Never UPDATE, never DELETE."""
    __tablename__ = "ledger"
    id = __import__("sqlalchemy").Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = __import__("sqlalchemy").Column(BigInteger, nullable=False)
    amount = __import__("sqlalchemy").Column(BigInteger, nullable=False)  # signed
    kind = __import__("sqlalchemy").Column(String(32), nullable=False)    # daily|referral|welcome
    ref = __import__("sqlalchemy").Column(String(128), nullable=True)     # idempotency key
    created_at = __import__("sqlalchemy").Column(DateTime, default=dt.datetime.utcnow)

    __table_args__ = (
        Index("ix_ledger_account", "account_id"),
        Index("ix_ledger_ref", "ref", unique=True),
    )


def init_db():
    Base.metadata.create_all(engine)


def balance(uid: int) -> int:
    with Session() as s:
        row = s.execute(
            text("SELECT COALESCE(SUM(amount),0) FROM ledger WHERE account_id=:u"),
            {"u": uid},
        ).scalar()
    return int(row or 0)


def credit(uid: int, amount: int, kind: str, ref: str) -> bool:
    """Idempotent credit. Returns True if written, False if ref already used."""
    with Session() as s:
        exists = s.execute(
            text("SELECT 1 FROM ledger WHERE ref=:r"), {"r": ref}
        ).first()
        if exists:
            return False
        s.execute(
            text(
                "INSERT INTO ledger (account_id, amount, kind, ref, created_at) "
                "VALUES (:a, :m, :k, :r, :t)"
            ),
            {"a": uid, "m": amount, "k": kind, "r": ref, "t": dt.datetime.utcnow()},
        )
        s.commit()
    return True


# ---------------------------------------------------------------- telegram


async def tg(method: str, **payload):
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{API}/{method}", json=payload)
        return r.json()


async def send(chat_id: int, text_: str, **kw):
    return await tg("sendMessage", chat_id=chat_id, text=text_, parse_mode="HTML", **kw)


# ---------------------------------------------------------------- app


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not BOT_TOKEN:
        log.error("BOT_TOKEN missing")
    if not DATABASE_URL:
        log.error("DATABASE_URL missing")
    else:
        init_db()
        log.info("db ready")
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def health():
    return {"ok": True, "service": "starhub", "time": dt.datetime.utcnow().isoformat()}


@app.post("/webhook")
async def webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    # --- auth: reject anything not from Telegram
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="bad secret")

    update = await request.json()
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return JSONResponse({"ok": True})

    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    frm = msg.get("from", {})
    uid = frm.get("id")
    text_ = (msg.get("text") or "").strip()

    if not uid or not text_:
        return JSONResponse({"ok": True})

    try:
        await handle(chat_id, uid, frm, text_)
    except Exception as e:
        log.exception("handler failed: %s", e)

    return JSONResponse({"ok": True})


async def handle(chat_id: int, uid: int, frm: dict, text_: str):
    cmd = text_.split()[0].split("@")[0].lower()

    # --- ensure account exists
    with Session() as s:
        row = s.execute(text("SELECT id FROM accounts WHERE id=:u"), {"u": uid}).first()
        if not row:
            s.execute(
                text(
                    "INSERT INTO accounts (id, username, first_name, created_at) "
                    "VALUES (:i, :n, :f, :t)"
                ),
                {
                    "i": uid,
                    "n": frm.get("username"),
                    "f": frm.get("first_name"),
                    "t": dt.datetime.utcnow(),
                },
            )
            s.commit()

    if cmd == "/start":
        # referral: /start 123456
        parts = text_.split()
        if len(parts) > 1 and parts[1].isdigit():
            ref_uid = int(parts[1])
            if ref_uid != uid:
                with Session() as s:
                    cur = s.execute(
                        text("SELECT referred_by FROM accounts WHERE id=:u"), {"u": uid}
                    ).first()
                    if cur and cur[0] is None:
                        s.execute(
                            text("UPDATE accounts SET referred_by=:r WHERE id=:u"),
                            {"r": ref_uid, "u": uid},
                        )
                        s.commit()
                        credit(ref_uid, REFERRAL_BONUS, "referral", f"ref:{uid}")
                        credit(uid, REFERRAL_WELCOME, "welcome", f"welcome:{uid}")

        await send(
            chat_id,
            "👋 <b>Welcome to StarHub</b>\n\n"
            "Earn <b>Shards</b> for daily activity and referrals.\n\n"
            "• /daily — claim your daily Shards\n"
            "• /balance — check your total\n"
            "• /invite — get your referral link\n"
            "• /help — how it works\n\n"
            "<i>Shards are in-app points. Not Telegram Stars, not crypto, "
            "not withdrawable.</i>",
        )
        return

    if cmd == "/daily":
        with Session() as s:
            row = s.execute(
                text("SELECT last_daily FROM accounts WHERE id=:u"), {"u": uid}
            ).first()
        last = row[0] if row else None
        now = dt.datetime.utcnow()
        if last and (now - last) < dt.timedelta(hours=24):
            left = dt.timedelta(hours=24) - (now - last)
            h, m = divmod(int(left.total_seconds()) // 60, 60)
            await send(chat_id, f"⏳ Already claimed.\nCome back in <b>{h}h {m}m</b>.")
            return
        day = now.strftime("%Y-%m-%d")
        written = credit(uid, DAILY_REWARD, "daily", f"daily:{uid}:{day}")
        with Session() as s:
            s.execute(
                text("UPDATE accounts SET last_daily=:t WHERE id=:u"),
                {"t": now, "u": uid},
            )
            s.commit()
        if written:
            await send(
                chat_id,
                f"✅ <b>+{DAILY_REWARD} Shards</b>\n\n"
                f"Balance: <b>{balance(uid)}</b>",
            )
        else:
            await send(chat_id, "⏳ Already claimed today.")
        return

    if cmd == "/balance":
        await send(chat_id, f"💠 Your balance: <b>{balance(uid)} Shards</b>")
        return

    if cmd == "/invite":
        me = await tg("getMe")
        uname = me.get("result", {}).get("username", "")
        link = f"https://t.me/{uname}?start={uid}"
        await send(
            chat_id,
            f"🔗 <b>Your referral link</b>\n\n{link}\n\n"
            f"You get <b>{REFERRAL_BONUS}</b> Shards per friend.\n"
            f"They get <b>{REFERRAL_WELCOME}</b> to start.",
        )
        return

    if cmd == "/help":
        await send(
            chat_id,
            "<b>How StarHub works</b>\n\n"
            f"• /daily — {DAILY_REWARD} Shards every 24h\n"
            f"• /invite — {REFERRAL_BONUS} Shards per referral\n"
            "• /balance — your total\n\n"
            "<i>Shards are in-app points only. They have no monetary value, "
            "cannot be withdrawn, and are not Telegram Stars.</i>",
        )
        return

    await send(chat_id, "Unknown command. Try /help")
