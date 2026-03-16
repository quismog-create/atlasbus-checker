#!/usr/bin/env python3
"""
AtlasBus Telegram bot — inline keyboard UI.
Пользователь выбирает: откуда → куда → дата → видит рейсы → подписывается на занятые.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from pathlib import Path

import aiohttp
import asyncpg
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ─────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL       = os.getenv("DATABASE_URL", "")
CHECK_INTERVAL     = 120
SUBS_FILE          = Path("subscriptions.json")  # fallback если нет БД

ATLAS_API  = "https://atlasbus.by/api/search"
PROXY_URL  = os.getenv("PROXY_URL", "")  # http://user:pass@host:port
ATLAS_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9,en;q=0.8",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "x-application-source": "web, web",
    "x-application-version": "2.45.5",
    "x-project-identifier": "morda",
    "x-saas-partner-id": "atlas",
    "referer": "https://atlasbus.by/",
    "origin": "https://atlasbus.by",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Короткие ключи городов для callback_data (макс 64 байта!)
CITIES = {
    "Лида":   "c626081",
    "Минск":  "c625144",
    "Гродно": "c627532",
}
# Короткий код → полное название (для callback_data)
CITY_SHORT = {"L": "Лида", "M": "Минск", "G": "Гродно"}
CITY_TO_SHORT = {v: k for k, v in CITY_SHORT.items()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── MODELS ─────────────────────────────────────────────────────────────────────

@dataclass
class Watch:
    ride_id:   str
    from_id:   str
    to_id:     str
    date:      str
    from_name: str
    to_name:   str
    departure: str

Subscriptions = dict[str, list[Watch]]

notified: dict[tuple, int] = {}

# Кеш рейсов: (from_id, to_id, date_str) → {"rides": [...], "ts": float}
rides_cache: dict[tuple, dict] = {}
CACHE_TTL = 115  # секунд — не делаем повторный запрос если данные свежее этого

# Глобальный пул БД
db_pool: asyncpg.Pool | None = None


# ── PERSISTENCE ────────────────────────────────────────────────────────────────

async def init_db() -> bool:
    global db_pool
    if not DATABASE_URL:
        return False
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    chat_id   TEXT NOT NULL,
                    ride_id   TEXT NOT NULL,
                    from_id   TEXT NOT NULL,
                    to_id     TEXT NOT NULL,
                    date      TEXT NOT NULL,
                    from_name TEXT NOT NULL,
                    to_name   TEXT NOT NULL,
                    departure TEXT NOT NULL,
                    PRIMARY KEY (chat_id, ride_id)
                )
            """)
        log.info("БД подключена (PostgreSQL)")
        return True
    except Exception as e:
        log.error("init_db: %s", e)
        return False

async def db_load_subs() -> Subscriptions:
    if not db_pool:
        return _file_load_subs()
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM subscriptions")
        subs: Subscriptions = {}
        for r in rows:
            cid = r["chat_id"]
            if cid not in subs:
                subs[cid] = []
            subs[cid].append(Watch(
                ride_id=r["ride_id"], from_id=r["from_id"], to_id=r["to_id"],
                date=r["date"], from_name=r["from_name"], to_name=r["to_name"],
                departure=r["departure"],
            ))
        return subs
    except Exception as e:
        log.error("db_load_subs: %s", e)
        return {}

async def db_add_watch(chat_id: str, watch: Watch) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (chat_id, ride_id, from_id, to_id, date, from_name, to_name, departure)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT DO NOTHING
        """, chat_id, watch.ride_id, watch.from_id, watch.to_id,
             watch.date, watch.from_name, watch.to_name, watch.departure)

async def db_remove_watch(chat_id: str, ride_id: str) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM subscriptions WHERE chat_id=$1 AND ride_id=$2", chat_id, ride_id)

async def db_remove_by_route(chat_id: str, from_id: str, to_id: str, date_str: str) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM subscriptions WHERE chat_id=$1 AND from_id=$2 AND to_id=$3 AND date=$4",
            chat_id, from_id, to_id, date_str,
        )

# ── FILE PERSISTENCE FALLBACK ──────────────────────────────────────────────────

def _file_load_subs() -> Subscriptions:
    if not SUBS_FILE.exists():
        return {}
    try:
        raw = json.loads(SUBS_FILE.read_text())
        return {cid: [Watch(**w) for w in watches] for cid, watches in raw.items()}
    except Exception as e:
        log.error("file load_subs: %s", e)
        return {}

def _file_save_subs(subs: Subscriptions) -> None:
    data = {cid: [asdict(w) for w in watches] for cid, watches in subs.items()}
    SUBS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))

def save_subs(subs: Subscriptions) -> None:
    """Файловый fallback — используется только если нет БД."""
    if not db_pool:
        _file_save_subs(subs)


# ── TELEGRAM ───────────────────────────────────────────────────────────────────

async def tg(session: aiohttp.ClientSession, method: str, **kw) -> dict:
    async with session.post(f"{TG_API}/{method}", json=kw) as r:
        return await r.json()

async def send(session, chat_id, text, keyboard=None, parse_mode="HTML"):
    kw: dict = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if keyboard:
        kw["reply_markup"] = {"inline_keyboard": keyboard}
    res = await tg(session, "sendMessage", **kw)
    if not res.get("ok"):
        log.error("sendMessage: %s", res)
    return res

async def edit(session, chat_id, message_id, text, keyboard=None, parse_mode="HTML"):
    kw: dict = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
    if keyboard:
        kw["reply_markup"] = {"inline_keyboard": keyboard}
    res = await tg(session, "editMessageText", **kw)
    if not res.get("ok"):
        desc = res.get("description", "")
        if "message is not modified" not in desc:
            log.error("editMessageText: %s", res)

async def answer_cb(session, cb_id, text=""):
    await tg(session, "answerCallbackQuery", callback_query_id=cb_id, text=text)


# ── ATLAS API ──────────────────────────────────────────────────────────────────

import time as _time

async def fetch_rides_raw(session: aiohttp.ClientSession, from_id: str, to_id: str, date_str: str, force: bool = False) -> list[dict]:
    key = (from_id, to_id, date_str)
    cached = rides_cache.get(key)
    if not force and cached and (_time.monotonic() - cached["ts"]) < CACHE_TTL:
        log.info("fetch cache hit: %s→%s %s", from_id, to_id, date_str)
        return cached["rides"]

    params = {
        "from_id": from_id, "to_id": to_id,
        "date": date_str, "calendar_width": 1,
        "passengers": 1, "operatorId": "",
    }
    timeout = aiohttp.ClientTimeout(total=15)
    log.info("fetch: %s→%s %s", from_id, to_id, date_str)

    for attempt in range(3):
        if attempt > 0:
            wait = 5 * attempt
            log.info("retry %d after %ds...", attempt, wait)
            await asyncio.sleep(wait)
        try:
            async with session.get(ATLAS_API, params=params, headers=ATLAS_HEADERS, timeout=timeout, proxy=PROXY_URL or None) as r:
                if r.status == 429:
                    log.warning("429 на попытке %d", attempt + 1)
                    continue
                r.raise_for_status()
                data = await r.json(content_type=None)
                rides = data.get("rides", [])
                rides.sort(key=lambda x: x.get("departure", ""))
                rides_cache[key] = {"rides": rides, "ts": _time.monotonic()}
                log.info("fetch result: %d rides", len(rides))
                return rides
        except asyncio.TimeoutError:
            log.warning("timeout на попытке %d", attempt + 1)

    raise Exception("Сервис временно недоступен, попробуйте через минуту")

def get_cached_ride(from_id: str, to_id: str, date_str: str, idx: int) -> dict | None:
    cached = rides_cache.get((from_id, to_id, date_str))
    rides  = cached["rides"] if cached else []
    if 0 <= idx < len(rides):
        return rides[idx]
    return None


# ── KEYBOARDS ──────────────────────────────────────────────────────────────────
# Все callback_data строго <= 64 байта.
# Используем короткие коды городов: L=Лида, M=Минск, G=Гродно
# Дата: YYYYMMDD (8 символов)
# Индекс рейса: число

def short(city_name: str) -> str:
    return CITY_TO_SHORT[city_name]

def long(city_short: str) -> str:
    return CITY_SHORT[city_short]

def date_short(d: str) -> str:
    return d.replace("-", "")  # 2026-03-16 → 20260316

def date_long(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"  # 20260316 → 2026-03-16

def kb_main():
    return [
        [{"text": "🔍 Посмотреть рейсы", "callback_data": "from"}],
        [{"text": "📋 Мои подписки",      "callback_data": "subs"}],
    ]

def kb_from():
    buttons = [{"text": name, "callback_data": f"to:{short(name)}"} for name in CITIES]
    return [buttons, [{"text": "◀️ Назад", "callback_data": "main"}]]

def kb_to(from_short: str):
    from_name = long(from_short)
    buttons = [
        {"text": name, "callback_data": f"dt:{from_short}:{short(name)}"}
        for name in CITIES if name != from_name
    ]
    return [buttons, [{"text": "◀️ Назад", "callback_data": "from"}]]

def kb_dates(from_short: str, to_short: str):
    today = date.today()
    rows, row = [], []
    for i in range(7):
        d = today + timedelta(days=i)
        label = "Сегодня" if i == 0 else "Завтра" if i == 1 else d.strftime("%d.%m")
        row.append({"text": label, "callback_data": f"rl:{from_short}:{to_short}:{date_short(d.isoformat())}"})
        if len(row) == 4:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([{"text": "◀️ Назад", "callback_data": f"to:{from_short}"}])
    return rows

def kb_rides(rides: list[dict], from_short: str, to_short: str, ds: str):
    rows = []
    for i, r in enumerate(rides):
        dep   = datetime.fromisoformat(r["departure"])
        free  = r.get("freeSeats", 0)
        price = r.get("price", "?")
        if free > 0 and r.get("status") == "sale":
            label = f"✅ {dep.strftime('%H:%M')}  {free} мест  {price} BYN"
        else:
            label = f"🔴 {dep.strftime('%H:%M')}  нет мест"
        # r:{from_short}:{to_short}:{ds}:{idx}  — max ~20 chars, хорошо вписывается
        rows.append([{"text": label, "callback_data": f"r:{from_short}:{to_short}:{ds}:{i}"}])
    rows.append([{"text": "◀️ Назад", "callback_data": f"dt:{from_short}:{to_short}"}])
    return rows

def kb_ride_detail(ride_idx: int, ride: dict, from_short: str, to_short: str, ds: str, is_watching: bool):
    rows = []
    free = ride.get("freeSeats", 0)
    if free > 0 and ride.get("status") == "sale":
        r = ride
        url = (
            f"https://atlasbus.by/search"
            f"?from={r['from']['id']}&fromName={r['from']['desc']}"
            f"&to={r['to']['id']}&toName={r['to']['desc']}"
        )
        rows.append([{"text": "🎟 Купить билет", "url": url}])
    else:
        if is_watching:
            rows.append([{"text": "🔔 Слежу — отписаться", "callback_data": f"unsub:{from_short}:{to_short}:{ds}:{ride_idx}"}])
        else:
            rows.append([{"text": "🔔 Уведомить когда освободится", "callback_data": f"sub:{from_short}:{to_short}:{ds}:{ride_idx}"}])
    rows.append([{"text": "◀️ К рейсам", "callback_data": f"rl:{from_short}:{to_short}:{ds}"}])
    return rows

def kb_subs(watches: list[Watch]):
    rows = []
    for i, w in enumerate(watches):
        dep = datetime.fromisoformat(w.departure)
        fs  = short(w.from_name)
        ts  = short(w.to_name)
        ds  = date_short(w.date)
        rows.append([{"text": f"❌ {w.from_name}→{w.to_name} {dep.strftime('%d.%m %H:%M')}", "callback_data": f"unsub:{fs}:{ts}:{ds}:0"}])
    rows.append([{"text": "◀️ Главное меню", "callback_data": "main"}])
    return rows


# ── SCREENS ────────────────────────────────────────────────────────────────────

async def screen_main(session, chat_id, mid=None):
    text = "🚌 <b>AtlasBus — мониторинг мест</b>\n\nВыберите действие:"
    if mid:
        await edit(session, chat_id, mid, text, kb_main())
    else:
        await send(session, chat_id, text, kb_main())

async def screen_from(session, chat_id, mid):
    await edit(session, chat_id, mid, "Выберите <b>город отправления</b>:", kb_from())

async def screen_to(session, chat_id, mid, from_short: str):
    from_name = long(from_short)
    await edit(session, chat_id, mid, f"Откуда: <b>{from_name}</b>\n\nВыберите <b>город назначения</b>:", kb_to(from_short))

async def screen_dates(session, chat_id, mid, from_short: str, to_short: str):
    from_name = long(from_short)
    to_name   = long(to_short)
    await edit(session, chat_id, mid, f"Маршрут: <b>{from_name} → {to_name}</b>\n\nВыберите <b>дату</b>:", kb_dates(from_short, to_short))

async def screen_rides(session, chat_id, mid, from_short: str, to_short: str, ds: str):
    from_name = long(from_short)
    to_name   = long(to_short)
    from_id   = CITIES[from_name]
    to_id     = CITIES[to_name]
    date_str  = date_long(ds)
    date_label = date.fromisoformat(date_str).strftime("%d.%m.%Y")

    try:
        rides = await fetch_rides_raw(session, from_id, to_id, date_str)
    except Exception as e:
        await edit(session, chat_id, mid, f"❌ Ошибка загрузки: {e}",
                   [[{"text": "◀️ Назад", "callback_data": f"dt:{from_short}:{to_short}"}]])
        return

    if not rides:
        await edit(session, chat_id, mid,
                   f"Рейсов <b>{from_name} → {to_name}</b> на {date_label} не найдено.",
                   [[{"text": "◀️ Назад", "callback_data": f"dt:{from_short}:{to_short}"}]])
        return

    free_count = sum(1 for r in rides if r.get("freeSeats", 0) > 0 and r.get("status") == "sale")
    text = (
        f"🚌 <b>{from_name} → {to_name}</b>  📅 {date_label}\n"
        f"Всего рейсов: {len(rides)}  |  Со свободными местами: {free_count}\n\n"
        f"✅ — есть места   🔴 — нет мест (нажмите, чтобы подписаться)"
    )
    await edit(session, chat_id, mid, text, kb_rides(rides, from_short, to_short, ds))

async def screen_ride_detail(session, chat_id, mid, from_short, to_short, ds, ride_idx, subs):
    from_name = long(from_short)
    to_name   = long(to_short)
    from_id   = CITIES[from_name]
    to_id     = CITIES[to_name]
    date_str  = date_long(ds)

    ride = get_cached_ride(from_id, to_id, date_str, ride_idx)
    if not ride:
        # cache miss — refetch
        try:
            rides = await fetch_rides_raw(session, from_id, to_id, date_str)
            ride  = rides[ride_idx] if ride_idx < len(rides) else None
        except Exception:
            ride = None
    if not ride:
        await edit(session, chat_id, mid, "Рейс не найден.",
                   [[{"text": "◀️ Назад", "callback_data": f"rl:{from_short}:{to_short}:{ds}"}]])
        return

    dep   = datetime.fromisoformat(ride["departure"])
    arr   = datetime.fromisoformat(ride["arrival"])
    free  = ride.get("freeSeats", 0)
    price = ride.get("price", "?")

    if free > 0 and ride.get("status") == "sale":
        seat_line = f"💺 <b>Свободных мест: {free}</b>"
    else:
        seat_line = "🔴 <b>Мест нет</b>"

    pickup    = ride.get("rideStops", {}).get(from_name, ride.get("pickupStops", []))
    discharge = ride.get("rideStops", {}).get(to_name,   ride.get("dischargeStops", []))

    def fmt_stops(stops):
        return "\n".join(
            f"  • {s['desc']} ({datetime.fromisoformat(s['datetime']).strftime('%H:%M')})"
            for s in stops[:3]
        )

    text = (
        f"🚌 <b>{from_name} → {to_name}</b>\n"
        f"🕐 {dep.strftime('%d.%m.%Y %H:%M')} → {arr.strftime('%H:%M')}\n"
        f"💰 Цена: {price} BYN\n"
        f"{seat_line}"
    )
    if pickup:
        text += f"\n\n🟢 Посадка:\n{fmt_stops(pickup)}"
    if discharge:
        text += f"\n\n🔵 Высадка:\n{fmt_stops(discharge)}"

    cid_str     = str(chat_id)
    ride_id     = ride["id"]
    is_watching = any(w.ride_id == ride_id for w in subs.get(cid_str, []))
    await edit(session, chat_id, mid, text, kb_ride_detail(ride_idx, ride, from_short, to_short, ds, is_watching))

async def screen_subs(session, chat_id, mid, subs):
    watches = subs.get(str(chat_id), [])
    if not watches:
        text = "У вас нет активных подписок.\n\nВыберите рейс и нажмите «Уведомить когда освободится»."
        kb   = [[{"text": "◀️ Главное меню", "callback_data": "main"}]]
    else:
        lines = ["<b>Ваши подписки:</b>\n"]
        for i, w in enumerate(watches, 1):
            dep = datetime.fromisoformat(w.departure)
            lines.append(f"{i}. {w.from_name} → {w.to_name}  {dep.strftime('%d.%m.%Y %H:%M')}")
        text = "\n".join(lines)
        kb   = kb_subs(watches)
    await edit(session, chat_id, mid, text, kb)


# ── CALLBACK ROUTER ────────────────────────────────────────────────────────────

async def handle_callback(session, cb, subs: Subscriptions):
    chat_id = cb["message"]["chat"]["id"]
    mid     = cb["message"]["message_id"]
    data    = cb.get("data", "")
    cb_id   = cb["id"]

    log.info("CB [%s] %s", chat_id, data)
    await answer_cb(session, cb_id)

    p = data.split(":")

    if data == "main":
        await screen_main(session, chat_id, mid)

    elif data == "from":
        await screen_from(session, chat_id, mid)

    elif data == "subs":
        await screen_subs(session, chat_id, mid, subs)

    elif p[0] == "to":
        # to:{from_short}
        await screen_to(session, chat_id, mid, p[1])

    elif p[0] == "dt":
        # dt:{from_short}:{to_short}
        await screen_dates(session, chat_id, mid, p[1], p[2])

    elif p[0] == "rl":
        # rl:{from_short}:{to_short}:{ds}
        await screen_rides(session, chat_id, mid, p[1], p[2], p[3])

    elif p[0] == "r":
        # r:{from_short}:{to_short}:{ds}:{idx}
        await screen_ride_detail(session, chat_id, mid, p[1], p[2], p[3], int(p[4]), subs)

    elif p[0] == "sub":
        # sub:{from_short}:{to_short}:{ds}:{idx}
        from_short, to_short, ds, idx = p[1], p[2], p[3], int(p[4])
        from_name = long(from_short)
        to_name   = long(to_short)
        from_id   = CITIES[from_name]
        to_id     = CITIES[to_name]
        date_str  = date_long(ds)

        ride = get_cached_ride(from_id, to_id, date_str, idx)
        if not ride:
            try:
                rides = await fetch_rides_raw(session, from_id, to_id, date_str)
                ride  = rides[idx] if idx < len(rides) else None
            except Exception:
                ride = None
        if not ride:
            await edit(session, chat_id, mid, "Рейс не найден.")
            return

        watch = Watch(
            ride_id=ride["id"], from_id=from_id, to_id=to_id,
            date=date_str, from_name=from_name, to_name=to_name,
            departure=ride["departure"],
        )
        cid_str = str(chat_id)
        if cid_str not in subs:
            subs[cid_str] = []
        if not any(w.ride_id == ride["id"] for w in subs[cid_str]):
            subs[cid_str].append(watch)
            await db_add_watch(cid_str, watch)
            save_subs(subs)
            log.info("Sub added: chat=%s ride=%s", chat_id, ride["id"])

        dep = datetime.fromisoformat(ride["departure"])
        await edit(session, chat_id, mid,
            f"🔔 <b>Подписка оформлена!</b>\n\n"
            f"🚌 {from_name} → {to_name}\n"
            f"🕐 {dep.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Буду проверять каждые {CHECK_INTERVAL} с и пришлю уведомление как только появится место.",
            [[{"text": "📋 Мои подписки",  "callback_data": "subs"}],
             [{"text": "◀️ Главное меню",  "callback_data": "main"}]],
        )

    elif p[0] == "unsub":
        # unsub:{from_short}:{to_short}:{ds}:{idx}
        from_short, to_short, ds, idx = p[1], p[2], p[3], int(p[4])
        from_id  = CITIES[long(from_short)]
        to_id    = CITIES[long(to_short)]
        date_str = date_long(ds)

        ride = get_cached_ride(from_id, to_id, date_str, idx)
        cid_str = str(chat_id)
        if ride:
            ride_id = ride["id"]
            before  = len(subs.get(cid_str, []))
            subs[cid_str] = [w for w in subs.get(cid_str, []) if w.ride_id != ride_id]
            if len(subs[cid_str]) < before:
                await db_remove_watch(cid_str, ride_id)
                save_subs(subs)
                notified.pop((cid_str, ride_id), None)
        else:
            subs[cid_str] = [
                w for w in subs.get(cid_str, [])
                if not (w.from_id == from_id and w.to_id == to_id and w.date == date_str)
            ]
            await db_remove_by_route(cid_str, from_id, to_id, date_str)
            save_subs(subs)
        await screen_subs(session, chat_id, mid, subs)


# ── CHECKER LOOP ───────────────────────────────────────────────────────────────

async def check_all(session: aiohttp.ClientSession, subs: Subscriptions) -> None:
    total = sum(len(v) for v in subs.values())
    if not total:
        return
    log.info("--- проверка %d подписок(и) ---", total)

    # Группируем все подписки по уникальному маршруту — один запрос на маршрут
    route_map: dict[tuple, list[tuple[str, Watch]]] = {}
    for cid_str, watches in list(subs.items()):
        for watch in watches:
            key = (watch.from_id, watch.to_id, watch.date)
            route_map.setdefault(key, []).append((cid_str, watch))

    for i, ((from_id, to_id, date_str), entries) in enumerate(route_map.items()):
        if i > 0:
            await asyncio.sleep(5)  # пауза между разными маршрутами

        try:
            rides = await fetch_rides_raw(session, from_id, to_id, date_str, force=True)
        except Exception as e:
            log.warning("check error %s→%s %s: %s", from_id, to_id, date_str, e)
            await asyncio.sleep(15)
            continue

        # Один запрос — проверяем всех подписчиков на этот маршрут
        for cid_str, watch in entries:
            ride = next((r for r in rides if r["id"] == watch.ride_id), None)
            if not ride:
                continue

            free = ride.get("freeSeats", 0)
            key  = (cid_str, watch.ride_id)

            log.info("  [%s] %s→%s %s: %d мест", cid_str, watch.from_name, watch.to_name, watch.departure[:16], free)

            if free > 0 and ride.get("status") == "sale":
                if notified.get(key, 0) == 0:
                    dep = datetime.fromisoformat(ride["departure"])
                    url = (
                        f"https://atlasbus.by/search"
                        f"?from={ride['from']['id']}&fromName={ride['from']['desc']}"
                        f"&to={ride['to']['id']}&toName={ride['to']['desc']}"
                    )
                    await send(
                        session, cid_str,
                        f"✅ <b>Место освободилось!</b>\n\n"
                        f"🚌 {watch.from_name} → {watch.to_name}\n"
                        f"🕐 {dep.strftime('%d.%m.%Y %H:%M')}\n"
                        f"💺 Свободных мест: <b>{free}</b>\n"
                        f"💰 Цена: {ride.get('price','?')} BYN\n\n"
                        f"🔗 <a href='{url}'>Купить билет</a>",
                        [[{"text": "🎟 Купить билет", "url": url}]],
                    )
                    log.info("Notified %s: %s (%d seats)", cid_str, watch.ride_id, free)
                notified[key] = free
            else:
                notified.pop(key, None)


# ── POLLING ────────────────────────────────────────────────────────────────────

async def process_update(session, upd, subs):
    if "callback_query" in upd:
        try:
            await handle_callback(session, upd["callback_query"], subs)
        except Exception as e:
            log.exception("handle_callback: %s", e)
        return

    msg = upd.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    res = await send(session, chat_id, "🚌 <b>AtlasBus — мониторинг мест</b>\n\nВыберите действие:", kb_main())

async def poll_updates(session: aiohttp.ClientSession, subs: Subscriptions) -> None:
    offset = 0
    log.info("Polling started.")
    while True:
        try:
            res = await tg(session, "getUpdates", offset=offset, timeout=30,
                           allowed_updates=["message", "callback_query"])
            if res.get("ok"):
                for upd in res.get("result", []):
                    offset = upd["update_id"] + 1
                    asyncio.create_task(process_update(session, upd, subs))
        except Exception as e:
            log.warning("getUpdates: %s", e)
            await asyncio.sleep(5)


# ── MAIN ───────────────────────────────────────────────────────────────────────

async def main():
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN не задан. Все env vars: %s", list(os.environ.keys()))
        return

    await init_db()
    subs = await db_load_subs()
    log.info("Загружено подписок: %d", sum(len(v) for v in subs.values()))
    log.info("Прокси: %s", PROXY_URL if PROXY_URL else "не задан")

    # Создаём сессию с cookie jar чтобы сайт не считал нас ботом
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.CookieJar()) as session:
        # Получаем cookies с главной страницы
        try:
            async with session.get("https://atlasbus.by/", headers=ATLAS_HEADERS, timeout=aiohttp.ClientTimeout(total=10), proxy=PROXY_URL or None) as r:
                log.info("Главная страница: %d, cookies: %d", r.status, len(session.cookie_jar))
        except Exception as e:
            log.warning("Не удалось получить cookies: %s", e)

        me = await tg(session, "getMe")
        if not me.get("ok"):
            log.error("Неверный токен: %s", me)
            return
        log.info("Бот @%s запущен. Интервал: %dс.", me["result"]["username"], CHECK_INTERVAL)

        async def checker():
            while True:
                await asyncio.sleep(CHECK_INTERVAL)
                await check_all(session, subs)

        await asyncio.gather(poll_updates(session, subs), checker())


if __name__ == "__main__":
    asyncio.run(main())
