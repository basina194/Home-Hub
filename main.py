# main.py
import os
import re
import json
import base64
import asyncio
import datetime
import time
import uuid
import logging
import secrets
from typing import List, Optional
from contextlib import asynccontextmanager

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("NexusCloud")

# Zmienne środowiskowe (konfigurowane w panelu chmury)
APP_PIN = os.environ.get("APP_PIN", "1234")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HA_URL = os.environ.get("HA_URL", "").rstrip("/")
HA_TOKEN = os.environ.get("HA_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
WIFE_TELEGRAM_ID = os.environ.get("WIFE_TELEGRAM_ID", "9216839954")
BARTOSZ_TELEGRAM_ID = os.environ.get("BARTOSZ_TELEGRAM_ID", "7939517140")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
LATITUDE = os.environ.get("LATITUDE", "52.7676")
LONGITUDE = os.environ.get("LONGITUDE", "17.7558")

# Agent AI Gemini
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")


def get_poland_time() -> datetime.datetime:
    """Zwraca aktualny czas w Polsce z poprawnym uwzględnieniem czasu letniego (CEST) i zimowego (CET)."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    year = now_utc.year
    d_mar = datetime.date(year, 3, 31)
    dst_start = d_mar - datetime.timedelta(days=(d_mar.weekday() + 1) % 7)
    d_oct = datetime.date(year, 10, 31)
    dst_end = d_oct - datetime.timedelta(days=(d_oct.weekday() + 1) % 7)
    today = now_utc.date()
    offset = 2 if (dst_start <= today < dst_end) else 1
    return now_utc.astimezone(datetime.timezone(datetime.timedelta(hours=offset)))


# --- OCHRONA PRZED BRUTE-FORCE I WERYFIKACJA PIN ---
PIN_FAILED_ATTEMPTS: dict[str, list[float]] = {}
PIN_BLOCKED_IPS: dict[str, float] = {}
MAX_PIN_FAILURES = 5
PIN_LOCKOUT_SECONDS = 900  # 15 minut blokady IP po 5 błędnych próbach
PIN_WINDOW_SECONDS = 60    # w oknie 1 minuty


def get_client_ip(request: Request) -> str:
    """Pobiera rzeczywisty adres IP klienta uwzględniając proxy (Render, Cloudflare itp.)."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def verify_pin(request: Request, x_app_pin: Optional[str] = Header(None)) -> None:
    """Weryfikuje nagłówek x-app-pin z odpornością na ataki czasowe i blokadą brute-force."""
    client_ip = get_client_ip(request)
    now = time.time()

    # Sprawdzenie czy IP jest zablokowane
    if client_ip in PIN_BLOCKED_IPS:
        blocked_until = PIN_BLOCKED_IPS[client_ip]
        if now < blocked_until:
            remaining = int(blocked_until - now)
            logger.warning("Odrzucono zapytanie ze zablokowanego IP %s (pozostało %ds)", client_ip, remaining)
            raise HTTPException(
                status_code=429,
                detail=f"Zbyt wiele błędnych prób logowania. Dostęp zablokowany na {remaining} sekund."
            )
        else:
            del PIN_BLOCKED_IPS[client_ip]
            PIN_FAILED_ATTEMPTS.pop(client_ip, None)

    # Bezpieczne porównanie stałoczasowe (Timing Attack Prevention)
    is_valid = secrets.compare_digest(x_app_pin or "", APP_PIN) if APP_PIN else False
    if not is_valid:
        attempts = PIN_FAILED_ATTEMPTS.setdefault(client_ip, [])
        attempts = [t for t in attempts if now - t < PIN_WINDOW_SECONDS]
        attempts.append(now)
        PIN_FAILED_ATTEMPTS[client_ip] = attempts

        if len(attempts) >= MAX_PIN_FAILURES:
            PIN_BLOCKED_IPS[client_ip] = now + PIN_LOCKOUT_SECONDS
            logger.warning("ZABLOKOWANO IP %s na 15 minut z powodu %d błędnych prób PIN", client_ip, len(attempts))
            raise HTTPException(
                status_code=429,
                detail="Zbyt wiele błędnych prób logowania. Adres IP został zablokowany na 15 minut."
            )

        logger.warning("Błędny kod PIN z IP %s (próba %d/%d)", client_ip, len(attempts), MAX_PIN_FAILURES)
        raise HTTPException(status_code=401, detail="Błędny kod PIN.")

    # Reset historii po udanym logowaniu
    if client_ip in PIN_FAILED_ATTEMPTS:
        del PIN_FAILED_ATTEMPTS[client_ip]


def supabase_headers() -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(status_code=500, detail="Brak konfiguracji SUPABASE_URL / SUPABASE_KEY.")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }


HTTP_CLIENT: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    """Zwraca współdzieloną instancję klienta HTTP ze stałą pulą połączeń (Connection Pool)
    dla eliminacji narzutu ponawiania uścisków dłoni TLS."""
    global HTTP_CLIENT
    if HTTP_CLIENT is None or HTTP_CLIENT.is_closed:
        HTTP_CLIENT = httpx.AsyncClient(
            timeout=25.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40)
        )
    return HTTP_CLIENT


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast_refresh(self, module: str) -> None:
        for connection in list(self.active_connections):
            try:
                await connection.send_json({"action": "refresh", "module": module})
            except Exception:
                self.disconnect(connection)


ws_manager = ConnectionManager()


async def send_telegram_alert(message: str, category: str = "Wspólne") -> None:
    """Wysyła alert/powiadomienie na Telegramie w zależności od kategorii zadania:
    - 'Śmieci' lub 'Wspólne' -> otrzymują obydwoje domowników (Bartosz i Daria)
    - 'Bartosz' -> wyłącznie Bartosz
    - 'Daria' -> wyłącznie Daria
    """
    if not TELEGRAM_BOT_TOKEN:
        return

    cat = (category or "Wspólne").strip()
    norm = cat.lower()

    bartosz_targets = set()
    daria_targets = set()

    if BARTOSZ_TELEGRAM_ID:
        bartosz_targets.add(str(BARTOSZ_TELEGRAM_ID).strip())
    if TELEGRAM_CHAT_ID and str(TELEGRAM_CHAT_ID).strip() != WIFE_TELEGRAM_ID:
        bartosz_targets.add(str(TELEGRAM_CHAT_ID).strip())

    if WIFE_TELEGRAM_ID:
        daria_targets.add(str(WIFE_TELEGRAM_ID).strip())

    # Pobierz dynamicznie zarejestrowanych subskrybentów (chat_id + name)
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{SUPABASE_URL}/rest/v1/telegram_subscribers?select=chat_id,name", headers=supabase_headers())
            if resp.status_code == 200:
                for row in resp.json():
                    cid = str(row.get("chat_id", "")).strip()
                    name = str(row.get("name", "")).strip().lower()
                    if not cid:
                        continue
                    if "daria" in name or cid == WIFE_TELEGRAM_ID:
                        daria_targets.add(cid)
                    else:
                        bartosz_targets.add(cid)
    except Exception as exc:
        logger.warning("Błąd odczytu telegram_subscribers: %s", exc)

    if norm == "bartosz":
        targets = bartosz_targets
    elif norm == "daria":
        targets = daria_targets
    else:
        # 'Śmieci', 'Wspólne' i inne zadania ogólne -> do obojga
        targets = bartosz_targets.union(daria_targets)

    if not targets:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=8.0) as client:
        for tid in targets:
            try:
                await client.post(url, json={"chat_id": tid, "text": message, "parse_mode": "HTML"})
            except Exception as exc:
                logger.error("Błąd wysyłki Telegram do %s (kat: %s): %s", tid, category, exc)


SUNDAY_SUMMARY_CACHE_FILE = ".last_sunday_summary"


def was_sunday_summary_sent(sunday_date_str: str) -> bool:
    try:
        if os.path.exists(SUNDAY_SUMMARY_CACHE_FILE):
            with open(SUNDAY_SUMMARY_CACHE_FILE, "r", encoding="utf-8") as f:
                return f.read().strip() == sunday_date_str
    except Exception:
        pass
    return False


def mark_sunday_summary_sent(sunday_date_str: str) -> None:
    try:
        with open(SUNDAY_SUMMARY_CACHE_FILE, "w", encoding="utf-8") as f:
            f.write(sunday_date_str)
    except Exception:
        pass


async def send_sunday_weekly_summary(now: datetime.datetime):
    """Wysyła w każdą niedzielę o 19:00 spersonalizowane podsumowanie wydarzeń
    na cały nadchodzący tydzień (od poniedziałku do niedzieli).
    """
    if not SUPABASE_URL or not SUPABASE_KEY or not TELEGRAM_BOT_TOKEN:
        return

    today = now.date()
    monday = today + datetime.timedelta(days=1)
    next_sunday = today + datetime.timedelta(days=7)

    url = (
        f"{SUPABASE_URL}/rest/v1/home_calendar"
        f"?event_date=gte.{monday}&event_date=lte.{next_sunday}"
        f"&order=event_date.asc,event_time.asc"
    )

    try:
        client = get_http_client()
        resp = await client.get(url, headers=supabase_headers())
        if resp.status_code != 200:
            return
        events = resp.json()
        if not isinstance(events, list):
            return

        cat_icons = {"Bartosz": "🧔", "Daria": "🌸", "Wspólne": "🏡", "Śmieci": "🗑️"}
        days_names = ["Poniedziałek", "Wtorek", "Środa", "Czwartek", "Piątek", "Sobota", "Niedziela"]

        def build_user_summary(user_name: str, allowed_categories: set) -> str:
            user_events = [e for e in events if e.get("category", "Wspólne") in allowed_categories]
            header = (
                f"🗓️ <b>PLAN NA NOWY TYDZIEŃ ({monday.strftime('%d.%m')} – {next_sunday.strftime('%d.%m')}):</b>\n"
                f"Cześć <b>{user_name}</b>! Oto Twoje plany i wydarzenia na nadchodzący tydzień:\n\n"
            )
            if not user_events:
                return (
                    f"{header}"
                    f"✨ <i>Brak zaplanowanych wydarzeń na ten tydzień. Czas na odpoczynek i spokój!</i>"
                )

            events_by_date = {}
            for e in user_events:
                d_str = e.get("event_date", "")
                events_by_date.setdefault(d_str, []).append(e)

            body_lines = []
            for d_str in sorted(events_by_date.keys()):
                try:
                    d_obj = datetime.date.fromisoformat(d_str)
                    d_name = days_names[d_obj.weekday()]
                    date_label = f"📅 <b>{d_name} ({d_obj.strftime('%d.%m')}):</b>"
                except Exception:
                    date_label = f"📅 <b>{d_str}:</b>"

                body_lines.append(date_label)
                for item in events_by_date[d_str]:
                    t = item.get("title", "Wydarzenie")
                    tm = item.get("event_time", "").strip()
                    c = item.get("category", "Wspólne")
                    ic = cat_icons.get(c, "📌")
                    time_str = f" o godz. <b>{tm}</b>" if tm else " (cały dzień)"
                    body_lines.append(f"  • {ic} <b>{t}</b>{time_str} <i>[{c}]</i>")
                body_lines.append("")

            footer = "💪 <i>Życzę udanego i spokojnego tygodnia!</i>"
            return header + "\n".join(body_lines) + "\n" + footer

        summary_bartosz = build_user_summary("Bartosz", {"Bartosz", "Wspólne", "Śmieci"})
        summary_daria = build_user_summary("Daria", {"Daria", "Wspólne", "Śmieci"})

        if BARTOSZ_TELEGRAM_ID:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": BARTOSZ_TELEGRAM_ID, "text": summary_bartosz, "parse_mode": "HTML"}
            )
        if TELEGRAM_CHAT_ID and str(TELEGRAM_CHAT_ID) != str(BARTOSZ_TELEGRAM_ID) and str(TELEGRAM_CHAT_ID) != str(WIFE_TELEGRAM_ID):
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": summary_bartosz, "parse_mode": "HTML"}
            )

        if WIFE_TELEGRAM_ID:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": WIFE_TELEGRAM_ID, "text": summary_daria, "parse_mode": "HTML"}
            )

        logger.info("Pomyślnie wysłano niedzielne podsumowanie tygodniowe do domowników.")
    except Exception as exc:
        logger.error("Błąd podczas wysyłania niedzielnego podsumowania: %s", exc)


async def check_and_send_calendar_reminders():
    """Sprawdza reguły powiadomień:
    - W każdą niedzielę o 19:00: podsumowanie na cały nadchodzący tydzień
    - Dla śmieci: 1 dzień przed o 9:00 oraz w dniu wywozu rano (od godz. 6:00)
    - Dla pozostałych wydarzeń:
        - Jeżeli jest podana godzina: dokładnie 24h przed rozpoczęciem
        - Jeżeli brak godziny: dzień przed o 9:00 rano
        - Oraz na 1h przed rozpoczęciem (o ile podano godzinę)
    """
    if not SUPABASE_URL or not SUPABASE_KEY or not TELEGRAM_BOT_TOKEN:
        return

    now = get_poland_time()
    today = now.date()

    # 1. Niedzielne podsumowanie tygodnia o 19:00
    if now.weekday() == 6 and now.hour >= 19:
        today_str = str(today)
        if not was_sunday_summary_sent(today_str):
            await send_sunday_weekly_summary(now)
            mark_sunday_summary_sent(today_str)

    # 2. Sprawdzamy okno od dziś do 2 dni do przodu dla bieżących przypomnień
    url = f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today}&event_date=lte.{today + datetime.timedelta(days=2)}&order=event_date.asc"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=supabase_headers())
            if resp.status_code != 200:
                return
            events = resp.json()
            if not isinstance(events, list):
                return

            cat_icons = {"Bartosz": "🧔", "Daria": "🌸", "Wspólne": "🏡", "Śmieci": "🗑️"}

            for ev in events:
                ev_id = ev.get("id")
                title = ev.get("title", "Wydarzenie")
                category = ev.get("category", "Wspólne")
                is_waste = (category == "Śmieci")
                notified = set(ev.get("notified_milestones") or [])
                new_milestones = set()

                try:
                    ev_date = datetime.date.fromisoformat(ev.get("event_date", str(today)))
                except Exception:
                    continue

                days_until = (ev_date - today).days
                ev_time = str(ev.get("event_time") or "").strip()
                icon = cat_icons.get(category, "📌")

                # --- 1. REGUŁY DLA ŚMIECI ---
                if is_waste:
                    # 1 dzień przed o 9:00 rano
                    if days_until == 1 and now.hour >= 9 and "smieci_1d_before" not in notified:
                        msg = (
                            f"🗑️ <b>PRZYPOMNIENIE: WYWÓZ ŚMIECI JUTRO!</b>\n\n"
                            f"• Frakcja: <b>{title}</b>\n"
                            f"• Data odbioru: <b>{ev_date}</b>\n\n"
                            f"👉 <i>Pamiętajcie o wystawieniu odpowiedniego kubła lub worków przed posesję!</i>"
                        )
                        await send_telegram_alert(msg, category="Śmieci")
                        new_milestones.add("smieci_1d_before")

                    # W dniu wydarzenia rano (od godz. 6:00)
                    elif days_until == 0 and "smieci_day_of" not in notified and now.hour >= 6:
                        msg = (
                            f"🚨 <b>WYWÓZ ŚMIECI DZISIAJ RANO!</b>\n\n"
                            f"• Frakcja: <b>{title}</b>\n\n"
                            f"👉 <i>Upewnijcie się, że odpady są wystawione przy drodze przed przyjazdem śmieciarki!</i>"
                        )
                        await send_telegram_alert(msg, category="Śmieci")
                        new_milestones.add("smieci_day_of")

                # --- 2. REGUŁY DLA POZOSTAŁYCH WYDARZEŃ ---
                else:
                    # A) Gdy jest podana godzina -> dokładnie 24h przed rozpoczęciem
                    if ev_time and "24h_before" not in notified:
                        m_time = re.match(r"^(\d{1,2}):(\d{2})$", ev_time)
                        if m_time:
                            h = int(m_time.group(1))
                            m = int(m_time.group(2))
                            ev_dt = datetime.datetime.combine(ev_date, datetime.time(h, m)).replace(tzinfo=now.tzinfo)
                            diff_hours = (ev_dt - now).total_seconds() / 3600.0
                            if 0 < diff_hours <= 24.1:
                                msg = (
                                    f"⏰ <b>Przypomnienie (za 24h):</b>\n"
                                    f"• {icon} <b>{title}</b> <i>[{category}]</i>\n"
                                    f"• Termin: <b>{ev_date}</b> o godz. <b>{ev_time}</b>"
                                )
                                await send_telegram_alert(msg, category=category)
                                new_milestones.add("24h_before")

                    # B) Gdy NIE ma podanej godziny -> dzień przed o 9:00 rano
                    elif not ev_time and days_until == 1 and now.hour >= 9 and "1d_before" not in notified:
                        msg = (
                            f"⏰ <b>Przypomnienie na JUTRO:</b>\n"
                            f"• {icon} <b>{title}</b> <i>[{category}]</i>\n"
                            f"• Termin: <b>{ev_date}</b> (Cały dzień)"
                        )
                        await send_telegram_alert(msg, category=category)
                        new_milestones.add("1d_before")

                    # C) Przypomnienie 1h przed rozpoczęciem (o ile podano godzinę)
                    if ev_time and "1h_before" not in notified:
                        m_time = re.match(r"^(\d{1,2}):(\d{2})$", ev_time)
                        if m_time:
                            h = int(m_time.group(1))
                            m = int(m_time.group(2))
                            ev_dt = datetime.datetime.combine(ev_date, datetime.time(h, m)).replace(tzinfo=now.tzinfo)
                            diff_minutes = (ev_dt - now).total_seconds() / 60.0
                            if 0 <= diff_minutes <= 65:
                                msg = (
                                    f"🔔 <b>PRZYPOMNIENIE (ZA GODZINĘ):</b>\n"
                                    f"• {icon} <b>{title}</b> <i>[{category}]</i>\n"
                                    f"• Godzina rozpoczęcia: <b>{ev_time}</b>!"
                                )
                                await send_telegram_alert(msg, category=category)
                                new_milestones.add("1h_before")

                # Zapisujemy odhaczone powiadomienia w Supabase
                if new_milestones:
                    updated_milestones = list(notified.union(new_milestones))
                    await client.patch(
                        f"{SUPABASE_URL}/rest/v1/home_calendar?id=eq.{ev_id}",
                        headers=supabase_headers(),
                        json={"notified_milestones": updated_milestones}
                    )
    except Exception as exc:
        logger.error("Błąd podczas sprawdzania przypomnień kalendarza: %s", exc)


async def calendar_reminder_loop():
    """Cykliczna pętla sprawdzająca przypomnienia co 60 sekund."""
    logger.info("Uruchomiono pętlę automatycznych przypomnień kalendarza (Telegram).")
    while True:
        try:
            await check_and_send_calendar_reminders()
        except Exception as exc:
            logger.error("Błąd w pętli przypomnień: %s", exc)
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global HTTP_CLIENT
    HTTP_CLIENT = httpx.AsyncClient(
        timeout=25.0,
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=40)
    )
    reminder_task = asyncio.create_task(calendar_reminder_loop())
    yield
    reminder_task.cancel()
    if HTTP_CLIENT and not HTTP_CLIENT.is_closed:
        await HTTP_CLIENT.aclose()


app = FastAPI(title="Home Hub", lifespan=lifespan)

# CORS Middleware (obsługa localhost, 127.0.0.1, LiveServer i urządzeń w sieci)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Security Headers Middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")


class CalendarEventCreate(BaseModel):
    title: str
    event_date: str
    event_time: Optional[str] = ""
    category: str = "Wspólne"
    created_by: str = "Aplikacja"


class ShoppingItemCreate(BaseModel):
    item: str
    quantity: Optional[str] = "1"
    category: str = "Wszystko"
    added_by: str = "Aplikacja"


class BizTaskCreate(BaseModel):
    client_name: str
    task_type: str
    due_date: str
    priority: str = "Normalny"


class HAToggleRequest(BaseModel):
    entity_id: str


@app.get("/")
async def serve_pwa():
    return FileResponse(
        "static/index.html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )


@app.head("/")
async def serve_pwa_head():
    """Obsługa zapytań HEAD z monitoringu i cronów (0 bajtów)."""
    return Response(content=b"", media_type="text/html")


# --- ULTRA-LEKKIE ENDPOINTY KEEP-ALIVE DLA CRON-JOB.ORG / UPTIME ---
@app.get("/ping")
@app.head("/ping")
@app.get("/healthz")
@app.head("/healthz")
@app.get("/api/cron/keepalive")
@app.head("/api/cron/keepalive")
async def keepalive_ping():
    """Ultra-lekki endpoint (2 bajty 'OK') dedykowany pod cron-job.org / UptimeRobot.
    Utrzymuje darmowy serwer Render wybudzonym bez ryzyka przekroczenia limitu rozmiaru odpowiedzi.
    """
    return Response(content="OK", media_type="text/plain")


@app.get("/manifest.json")
async def serve_manifest():
    return FileResponse("static/manifest.json")


@app.get("/sw.js")
async def serve_sw():
    return FileResponse(
        "static/sw.js",
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Weryfikacja PIN dla połączeń WebSocket
    pin = websocket.query_params.get("pin", "")
    if not APP_PIN or not secrets.compare_digest(pin, APP_PIN):
        await websocket.close(code=1008)  # 1008: Policy Violation
        return

    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# --- POGODA (OPEN-METEO) ---
@app.get("/api/home/weather", dependencies=[Depends(verify_pin)])
async def get_weather():
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LATITUDE}&longitude={LONGITUDE}"
        f"&current=temperature_2m,wind_speed_10m,weather_code"
        f"&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max"
        f"&timezone=Europe%2FWarsaw&forecast_days=7"
    )
    client = get_http_client()
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


# --- LISTA ZAKUPÓW (SUPABASE) ---
@app.get("/api/home/shopping", dependencies=[Depends(verify_pin)])
async def list_shopping():
    url = f"{SUPABASE_URL}/rest/v1/home_shopping?select=*&order=is_bought.asc,id.desc&limit=50"
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, headers=supabase_headers())
        resp.raise_for_status()
        return resp.json()


@app.post("/api/home/shopping", dependencies=[Depends(verify_pin)])
async def add_shopping(payload: ShoppingItemCreate):
    url = f"{SUPABASE_URL}/rest/v1/home_shopping"
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, headers=supabase_headers(), json=payload.model_dump())
        resp.raise_for_status()
    await ws_manager.broadcast_refresh("shopping")
    return {"status": "ok"}


@app.patch("/api/home/shopping/{item_id}/toggle", dependencies=[Depends(verify_pin)])
async def toggle_shopping(item_id: int):
    async with httpx.AsyncClient(timeout=5.0) as client:
        get_url = f"{SUPABASE_URL}/rest/v1/home_shopping?id=eq.{item_id}&select=is_bought"
        curr = (await client.get(get_url, headers=supabase_headers())).json()
        if not curr:
            raise HTTPException(status_code=404, detail="Nie znaleziono pozycji.")
        new_val = not curr[0]["is_bought"]
        patch_url = f"{SUPABASE_URL}/rest/v1/home_shopping?id=eq.{item_id}"
        await client.patch(patch_url, headers=supabase_headers(), json={"is_bought": new_val})
    await ws_manager.broadcast_refresh("shopping")
    return {"status": "ok"}


@app.delete("/api/home/shopping/clear-bought", dependencies=[Depends(verify_pin)])
async def clear_bought():
    url = f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.true"
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.delete(url, headers=supabase_headers())
    await ws_manager.broadcast_refresh("shopping")
    return {"status": "ok"}


@app.delete("/api/home/shopping/{item_id}", dependencies=[Depends(verify_pin)])
async def delete_shopping_item(item_id: int):
    url = f"{SUPABASE_URL}/rest/v1/home_shopping?id=eq.{item_id}"
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.delete(url, headers=supabase_headers())
    await ws_manager.broadcast_refresh("shopping")
    return {"status": "ok"}


# --- MODUŁ KALENDARZ (SUPABASE) ---
@app.get("/api/home/calendar", dependencies=[Depends(verify_pin)])
async def list_calendar_events():
    url = f"{SUPABASE_URL}/rest/v1/home_calendar?select=*&order=event_date.asc,event_time.asc&limit=100"
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, headers=supabase_headers())
        resp.raise_for_status()
        return resp.json()


@app.post("/api/home/calendar", dependencies=[Depends(verify_pin)])
async def add_calendar_event(payload: CalendarEventCreate):
    url = f"{SUPABASE_URL}/rest/v1/home_calendar"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, headers=supabase_headers(), json=payload.model_dump())
        resp.raise_for_status()
    await ws_manager.broadcast_refresh("calendar")
    return {"status": "ok"}


@app.delete("/api/home/calendar/{event_id}", dependencies=[Depends(verify_pin)])
async def delete_calendar_event(event_id: int):
    url = f"{SUPABASE_URL}/rest/v1/home_calendar?id=eq.{event_id}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.delete(url, headers=supabase_headers())
        resp.raise_for_status()
    await ws_manager.broadcast_refresh("calendar")
    return {"status": "ok"}


@app.api_route("/api/home/reminders/trigger", methods=["GET", "POST"])
@app.api_route("/api/cron/reminders", methods=["GET", "POST"])
async def trigger_reminders_cron(request: Request):
    """Sprawdzenie i wysyłka przypomnień kalendarza.
    Obsługuje GET i POST z zewnętrznego crona (np. cron-job.org) oraz wywołania z aplikacji.
    Zwraca ultra-krótką odpowiedź tekstową 2 bajty ('OK'), co zapobiega błędowi 'zbyt duży rozmiar danych wyjściowych'.
    """
    await check_and_send_calendar_reminders()
    return Response(content="OK", media_type="text/plain")


# --- MODUŁ FIRMA (SUPABASE) ---
@app.get("/api/biz/tasks", dependencies=[Depends(verify_pin)])
async def list_biz_tasks():
    url = f"{SUPABASE_URL}/rest/v1/biz_tasks?select=*&order=status.desc,due_date.asc"
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, headers=supabase_headers())
        resp.raise_for_status()
        return resp.json()


@app.post("/api/biz/tasks", dependencies=[Depends(verify_pin)])
async def create_biz_task(payload: BizTaskCreate):
    url = f"{SUPABASE_URL}/rest/v1/biz_tasks"
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, headers=supabase_headers(), json=payload.model_dump())
        resp.raise_for_status()
    if payload.priority == "Krytyczny":
        await send_telegram_alert(
            f"🚨 <b>Pilne zadanie firmowe:</b>\nKlient: {payload.client_name}\nTyp: {payload.task_type}\nTermin: {payload.due_date}",
            category="Bartosz"
        )
    await ws_manager.broadcast_refresh("biz")
    return {"status": "ok"}


@app.patch("/api/biz/tasks/{task_id}/done", dependencies=[Depends(verify_pin)])
async def toggle_biz_task(task_id: int):
    async with httpx.AsyncClient(timeout=5.0) as client:
        get_url = f"{SUPABASE_URL}/rest/v1/biz_tasks?id=eq.{task_id}&select=status"
        curr = (await client.get(get_url, headers=supabase_headers())).json()
        if not curr:
            raise HTTPException(status_code=404, detail="Nie znaleziono zadania.")
        new_status = "Done" if curr[0]["status"] != "Done" else "Todo"
        patch_url = f"{SUPABASE_URL}/rest/v1/biz_tasks?id=eq.{task_id}"
        await client.patch(patch_url, headers=supabase_headers(), json={"status": new_status})
    await ws_manager.broadcast_refresh("biz")
    return {"status": "ok"}


# --- HOME ASSISTANT (GOTOWE NA PODPIĘCIE ZDALNEGO URL) ---
@app.get("/api/home/ha/states", dependencies=[Depends(verify_pin)])
async def get_ha_states():
    if not HA_URL or not HA_TOKEN:
        return [
            {"entity_id": "light.salon", "state": "on", "attributes": {"friendly_name": "Salon Światło (Tryb Demo)"}},
            {"entity_id": "sensor.temp_dom", "state": "22.1 °C", "attributes": {"friendly_name": "Temperatura Dom (Tryb Demo)"}}
        ]
    headers = {"Authorization": f"Bearer {HA_TOKEN}"}
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{HA_URL}/api/states", headers=headers)
        resp.raise_for_status()
        allowed = ("light.", "switch.", "sensor.")
        return [s for s in resp.json() if s.get("entity_id", "").startswith(allowed)][:10]


# Słownik polskich liczebników głównych
POLISH_NUM_WORDS = {
    'jeden': '1', 'jedna': '1', 'jedno': '1',
    'dwa': '2', 'dwie': '2', 'dwoje': '2',
    'trzy': '3', 'troje': '3',
    'cztery': '4', 'czworo': '4',
    'pięć': '5', 'piec': '5',
    'sześć': '6', 'szesc': '6',
    'siedem': '7',
    'osiem': '8',
    'dziewięć': '9', 'dziewiec': '9',
    'dziesięć': '10', 'dziesiec': '10',
    'jedenaście': '11', 'jedenascie': '11',
    'dwanaście': '12', 'dwanascie': '12',
    'trzynaście': '13', 'trzynascie': '13',
    'czternaście': '14', 'czternascie': '14',
    'piętnaście': '15', 'pietnascie': '15',
    'szesnaście': '16', 'szesnascie': '16',
    'siedemnaście': '17', 'siedemnascie': '17',
    'osiemnaście': '18', 'osiemnascie': '18',
    'dziewiętnaście': '19', 'dziewietnascie': '19',
    'dwadzieścia': '20', 'dwadziescia': '20',
    'trzydzieści': '30', 'trzydziesci': '30',
    'czterdzieści': '40', 'czterdziesci': '40',
    'pięćdziesiąt': '50', 'piecdziesiat': '50',
    'pół': '0.5', 'pol': '0.5',
}

KNOWN_SHOPPING_UNITS = [
    'x', 'szt', 'sztuk', 'sztuki', 'sztukę', 'sztuke', 'op', 'opak', 'opakowanie', 'opakowania', 'opakowań', 'opakowan',
    'kg', 'g', 'dag', 'l', 'litr', 'litry', 'litrow', 'litrów', 'butelka', 'butelki', 'butelek',
    'puszka', 'puszki', 'puszek', 'karton', 'kartony', 'kartonów', 'kartonow', 'zgrzewka', 'zgrzewki', 'zgrzewek',
    'pęczek', 'peczek', 'pęczki', 'bochenek', 'bochenki'
]


def parse_polish_item(raw: str) -> tuple[str, str]:
    """Inteligentne rozpoznawanie ilości i nazwy w języku polskim:
    - '2 mleka' -> ('mleka', '2')
    - '5 jajek' -> ('jajek', '5')
    - 'jajka 10' -> ('jajka', '10')
    - 'chleb 2' -> ('chleb', '2')
    - '2x chleb' -> ('chleb', '2 szt.')
    - '1.5 kg mąki' -> ('mąki', '1.5 kg')
    - 'woda 6 butelek' -> ('woda', '6 butelek')
    - 'mleko - 2 opakowania' -> ('mleko', '2 opakowania')
    """
    raw = raw.strip()
    if not raw:
        return "", "1"

    # 1. Sprawdź myślnik: 'towar - ilość'
    if " - " in raw:
        parts = raw.split(" - ", 1)
        return parts[0].strip(), parts[1].strip()

    known_units = set(KNOWN_SHOPPING_UNITS)

    # 2. Liczba na początku: '2 mleka', '2x chleb', '2 kg ziemniaków', '1.5l wody'
    m_start = re.match(r'^(\d+(?:[.,]\d+)?)\s*([a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]{1,10})?\s+(.+)$', raw)
    if m_start:
        num = m_start.group(1).replace(",", ".")
        unit = m_start.group(2)
        rest = m_start.group(3).strip()
        if unit and unit.lower() in known_units:
            qty = f"{num} {unit}" if unit.lower() != 'x' else f"{num} szt."
            return rest, qty
        elif not unit:
            return rest, num

    # 3. Liczba na końcu: 'jajka 10', 'mleko 2', 'chleb 2 szt', 'woda 6 butelek'
    m_end = re.match(r'^(.+?)\s+(\d+(?:[.,]\d+)?)\s*([a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]{1,10})?$', raw)
    if m_end:
        item = m_end.group(1).strip()
        num = m_end.group(2).replace(",", ".")
        unit = m_end.group(3)
        if unit and unit.lower() in known_units:
            return item, f"{num} {unit}"
        elif not unit:
            return item, num

    return raw, "1"


def split_and_parse_shopping(raw_text: str) -> list[tuple[str, str]]:
    """Rozbija tekst zakupów na poszczególne produkty z ilościami, nawet gdy nie ma przecinków
    (np. '2 jajka 3 mleka 5 bułek i sok' -> [('jajka', '2'), ('mleka', '3'), ('bułek', '5'), ('sok', '1')])
    oraz zamienia liczebniki słowne ('trzy mleka i chleb' -> [('mleka', '3'), ('chleb', '1')]).
    """
    cleaned = raw_text.strip()
    for prefix in ['/kup ', 'kup ', '/dodaj ', 'dodaj ']:
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    # 1. Zamiana słownych liczebników na cyfry
    for word, num in POLISH_NUM_WORDS.items():
        cleaned = re.sub(rf'\b{word}\b', num, cleaned, flags=re.I)

    # 2. Zamiana spójników na przecinek
    cleaned = re.sub(r'\s+(?:i|oraz|\+|a także|a takze)\s+', ', ', cleaned, flags=re.I)

    # 3. Wstaw przecinek przed liczbą, która zaczyna kolejną pozycję
    unit_pattern = '|'.join(KNOWN_SHOPPING_UNITS)
    cleaned = re.sub(
        rf'([^\W\d_]+)\s+(?=\d+(?:[.,]\d+)?\s+(?!{unit_pattern}\b)[^\W\d_]+)',
        r'\1, ',
        cleaned,
        flags=re.I
    )
    cleaned = re.sub(
        rf'([^\W\d_]+)\s+(?=\d+(?:[.,]\d+)?\s+(?:{unit_pattern})\s+[^\W\d_]+)',
        r'\1, ',
        cleaned,
        flags=re.I
    )

    chunks = [c.strip() for c in re.split(r'[,;\n]+', cleaned) if c.strip()]
    results = []
    for chunk in chunks:
        item_name, qty = parse_polish_item(chunk)
        if item_name:
            results.append((item_name, qty))
    return results


POLISH_CALENDAR_MONTHS = {
    'stycz': 1, 'lut': 2, 'marz': 3, 'kwiet': 4, 'maj': 5, 'czerwc': 6,
    'lip': 7, 'sierp': 8, 'wrześ': 9, 'wrzes': 9, 'październik': 10, 'pazdziernik': 10,
    'listopad': 11, 'grudzi': 12
}


def parse_calendar_input(text: str, default_user: str = "Bartosz") -> tuple[str, str, str, str]:
    """Inteligentne rozpoznawanie daty, godziny, tytułu i kategorii:
    - 'kalendarz 15.11 dentysta 16:30' -> ('Dentysta', '2026-11-15', '16:30', 'Bartosz')
    - 'Dodaj kalednarz wspolny szpital miejski 9 listopad usg' -> ('Szpital miejski usg', '2026-11-09', '', 'Wspólne')
    - 'śmieci jutro bio' -> ('Bio', '2026-11-03', '', 'Śmieci')
    - 'termin 20.11 wyjazd z rodzicami Daria' -> ('Wyjazd z rodzicami', '2026-11-20', '', 'Daria')
    """
    now = datetime.date.today()
    cleaned = text.strip()
    
    # Obsługa prefixów (w tym z literówkami i 'dodaj')
    for prefix in [
        "dodaj do kalendarza", "dodaj kalendarz", "dodaj kalednarz", "dodaj termin", "dodaj do terminu",
        "dodaj wydarzenie", "dodaj wizytę", "dodaj wizyte", "dodaj spotkanie", "dodaj śmieci", "dodaj smieci",
        "/kalendarz", "kalendarz", "/kalednarz", "kalednarz", "/termin", "termin",
        "/smieci", "smieci", "/śmieci", "śmieci", "/wydarzenie", "wydarzenie",
        "/spotkanie", "spotkanie", "/wizyta", "wizyta", "/event", "event"
    ]:
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    lower = cleaned.lower()
    cat = "Wspólne"
    if any(k in lower for k in ['śmieci', 'smieci', 'bio', 'plastik', 'papier', 'zmieszane', 'odpady']):
        cat = "Śmieci"
    elif any(k in lower for k in ['daria', 'darii']):
        cat = "Daria"
    elif any(k in lower for k in ['bartosz', 'bartosza', 'bartek', 'bartka']):
        cat = "Bartosz"
    elif any(k in lower for k in ['wspólne', 'wspolne', 'wspólny', 'wspolny', 'razem', 'dom']):
        cat = "Wspólne"
    else:
        cat = "Daria" if default_user.lower() == "daria" else "Bartosz"

    event_date = str(now)
    time_str = ""

    # Szukaj godziny HH:MM
    m_time = re.search(r'\b([01]?\d|2[0-3]):([0-5]\d)\b', cleaned)
    if m_time:
        time_str = m_time.group(0)
        cleaned = cleaned.replace(time_str, "").strip()

    # Szukaj daty słownej lub numerycznej
    if "pojutrze" in lower:
        event_date = str(now + datetime.timedelta(days=2))
        cleaned = re.sub(r'\bpojutrze\b', '', cleaned, flags=re.I).strip()
    elif "jutro" in lower:
        event_date = str(now + datetime.timedelta(days=1))
        cleaned = re.sub(r'\bjutro\b', '', cleaned, flags=re.I).strip()
    elif "dzisiaj" in lower or "dziś" in lower:
        event_date = str(now)
        cleaned = re.sub(r'\b(dzisiaj|dziś)\b', '', cleaned, flags=re.I).strip()
    else:
        # Sprawdź polskie miesiące słownie, np. '9 listopad' lub '9 listopada'
        m_month = re.search(r'\b(\d{1,2})\s+([a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]+)(?:\s+(\d{4}))?\b', cleaned)
        found_month = False
        if m_month:
            d_val = int(m_month.group(1))
            m_word = m_month.group(2).lower()
            for prefix_m, m_num in POLISH_CALENDAR_MONTHS.items():
                if m_word.startswith(prefix_m):
                    y_val = int(m_month.group(3)) if m_month.group(3) else now.year
                    event_date = f"{y_val:04d}-{m_num:02d}-{d_val:02d}"
                    cleaned = cleaned.replace(m_month.group(0), "").strip()
                    found_month = True
                    break
        if not found_month:
            m_date = re.search(r'\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{4}))?\b', cleaned)
            if m_date:
                day = int(m_date.group(1))
                month = int(m_date.group(2))
                year = int(m_date.group(3)) if m_date.group(3) else now.year
                event_date = f"{year:04d}-{month:02d}-{day:02d}"
                cleaned = cleaned.replace(m_date.group(0), "").strip()

    # Usunięcie zbędnych słów kluczowych z tytułu
    cleaned = re.sub(r'\b(daria|darii|bartosz|bartosza|bartek|bartka|wspólne|wspolne|wspólny|wspolny)\b', '', cleaned, flags=re.I).strip()
    cleaned = re.sub(r'^(o|w|na|ze|z|dla|do)\s+', '', cleaned, flags=re.I).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned.capitalize() or "Wydarzenie", event_date, time_str, cat


async def reply_telegram(chat_id: int | str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        client = get_http_client()
        await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
    except Exception as exc:
        logger.error("Błąd wysyłki Telegram reply: %s", exc)


async def download_telegram_file(file_id: str) -> tuple[bytes, str]:
    """Pobiera zawartość pliku (zdjęcie, audio) bezpośrednio z Telegram API."""
    if not TELEGRAM_BOT_TOKEN or not file_id:
        return b"", ""
    try:
        client = get_http_client()
        info_res = await client.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile",
            params={"file_id": file_id}
        )
        data = info_res.json()
        if not data.get("ok"):
            return b"", ""
        file_path = data["result"]["file_path"]
        file_res = await client.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}")
        return file_res.content, file_path
    except Exception as exc:
        logger.error("Błąd pobierania pliku Telegram (%s): %s", file_id, exc)
        return b"", ""


# --- PAMIĘĆ OCZEKUJĄCYCH ZATWIERDZEŃ WYDARZEŃ KALENDARZA ---
PENDING_CALENDAR_ACTIONS: dict[str, dict] = {}
LAST_PENDING_PER_CHAT: dict[str, str] = {}


def cleanup_pending_actions() -> None:
    now_ts = time.time()
    expired = [k for k, v in PENDING_CALENDAR_ACTIONS.items() if now_ts - v.get("created_at", 0) > 7200]
    for k in expired:
        PENDING_CALENDAR_ACTIONS.pop(k, None)


def format_events_summary(events: list[dict]) -> str:
    cat_icons = {"Bartosz": "🧔", "Daria": "🌸", "Wspólne": "🏡", "Śmieci": "🗑️"}
    lines = []
    for idx, ev in enumerate(events, 1):
        cat = ev.get("category", "Wspólne")
        icon = cat_icons.get(cat, "📌")
        title = ev.get("title", "Wydarzenie")
        d = ev.get("event_date", "")
        t = ev.get("event_time", "")
        time_part = f" o godz. <b>{t}</b>" if t else ""
        if len(events) == 1:
            lines.append(f"• {icon} <b>{title}</b>\n  📅 Data: <b>{d}</b>{time_part}\n  🏷️ Kategoria: <b>{cat}</b>")
        else:
            lines.append(f"{idx}. {icon} <b>{title}</b> — <b>{d}</b>{time_part} <i>[{cat}]</i>")
    return "\n".join(lines)


async def send_telegram_with_keyboard(chat_id: int | str, text: str, inline_keyboard: list) -> Optional[dict]:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        client = get_http_client()
        resp = await client.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": {"inline_keyboard": inline_keyboard}
        })
        if resp.status_code == 200:
            return resp.json().get("result")
    except Exception as exc:
        logger.error("Błąd wysyłki Telegram z klawiaturą: %s", exc)
    return None


async def edit_telegram_message(
    chat_id: int | str,
    message_id: int,
    text: str,
    remove_keyboard: bool = True,
    inline_keyboard: Optional[list] = None
) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id or not message_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if inline_keyboard is not None:
        payload["reply_markup"] = {"inline_keyboard": inline_keyboard}
    elif remove_keyboard:
        payload["reply_markup"] = {"inline_keyboard": []}
    try:
        client = get_http_client()
        await client.post(url, json=payload)
    except Exception as exc:
        logger.error("Błąd edycji wiadomości Telegram: %s", exc)


async def answer_telegram_callback(callback_query_id: str, text: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN or not callback_query_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        client = get_http_client()
        await client.post(url, json={"callback_query_id": callback_query_id, "text": text})
    except Exception as exc:
        logger.error("Błąd answerCallbackQuery: %s", exc)


async def ask_calendar_confirmation(
    chat_id: int | str,
    sender: str,
    events: list[dict],
    intro_msg: str = ""
) -> None:
    if not events:
        if intro_msg:
            await reply_telegram(chat_id, intro_msg)
        return

    cleanup_pending_actions()
    token = uuid.uuid4().hex[:10]
    chat_id_str = str(chat_id)

    PENDING_CALENDAR_ACTIONS[token] = {
        "sender": sender,
        "chat_id": chat_id_str,
        "created_at": time.time(),
        "events": events
    }
    LAST_PENDING_PER_CHAT[chat_id_str] = token

    summary = format_events_summary(events)
    count = len(events)

    msg_lines = []
    if intro_msg and intro_msg.strip() != "Zrobione!":
        msg_lines.append(intro_msg.strip())
        msg_lines.append("")

    if count == 1:
        msg_lines.append("📋 <b>Wykryto wydarzenie do kalendarza:</b>")
    else:
        msg_lines.append(f"📋 <b>Wykryto {count} wydarzeń / terminów:</b>")

    msg_lines.append(summary)
    msg_lines.append("")
    msg_lines.append("❓ <i>Czy zatwierdzasz dodanie do kalendarza?</i>")
    msg_lines.append("<i>(Kliknij przycisk poniżej lub odpisz 'tak' / 'nie')</i>")

    full_text = "\n".join(msg_lines)

    btn_ok = "✅ Zatwierdź i dodaj" if count == 1 else f"✅ Zatwierdź wszystkie ({count})"
    keyboard = [
        [
            {"text": btn_ok, "callback_data": f"cal_ok:{token}"},
            {"text": "❌ Odrzuć", "callback_data": f"cal_no:{token}"}
        ]
    ]
    await send_telegram_with_keyboard(chat_id, full_text, keyboard)


async def handle_calendar_callback(
    callback_query_id: str,
    chat_id: int | str,
    message_id: Optional[int],
    data: str,
    user_name: str
) -> None:
    if data.startswith("cal_ok:"):
        token = data.split(":", 1)[1]
        pending = PENDING_CALENDAR_ACTIONS.pop(token, None)
        chat_id_str = str(chat_id)
        if LAST_PENDING_PER_CHAT.get(chat_id_str) == token:
            LAST_PENDING_PER_CHAT.pop(chat_id_str, None)

        if not pending:
            await answer_telegram_callback(callback_query_id, "Wpis już przetworzony lub wygasł.")
            if message_id:
                await edit_telegram_message(chat_id, message_id, "⚠️ <i>To wydarzenie zostało już wcześniej zatwierdzone lub wygasło.</i>")
            return

        events = pending.get("events", [])
        sender = pending.get("sender", user_name)

        async with httpx.AsyncClient(timeout=10.0) as client:
            for ev in events:
                await client.post(
                    f"{SUPABASE_URL}/rest/v1/home_calendar",
                    headers=supabase_headers(),
                    json={
                        "title": ev.get("title", "Wydarzenie"),
                        "event_date": ev.get("event_date"),
                        "event_time": ev.get("event_time", ""),
                        "category": ev.get("category", "Wspólne"),
                        "created_by": f"TG ({sender})"
                    }
                )
        await ws_manager.broadcast_refresh("calendar")
        await answer_telegram_callback(callback_query_id, "Dodano do kalendarza!")

        summary = format_events_summary(events)
        edit_text = (
            f"✅ <b>ZATWIERDZONO! Dodano do kalendarza ({user_name}):</b>\n\n"
            f"{summary}\n\n"
            f"🔔 <i>Powiadomienia i przypomnienia Telegram zostały zaplanowane.</i>"
        )
        if message_id:
            await edit_telegram_message(chat_id, message_id, edit_text)
        else:
            await reply_telegram(chat_id, edit_text)

    elif data.startswith("cal_no:"):
        token = data.split(":", 1)[1]
        PENDING_CALENDAR_ACTIONS.pop(token, None)
        chat_id_str = str(chat_id)
        if LAST_PENDING_PER_CHAT.get(chat_id_str) == token:
            LAST_PENDING_PER_CHAT.pop(chat_id_str, None)

        await answer_telegram_callback(callback_query_id, "Anulowano.")
        edit_text = f"❌ <b>Odrzucono dodawanie ({user_name}).</b>\nŻadne wydarzenia nie zostały zapisane w kalendarzu."
        if message_id:
            await edit_telegram_message(chat_id, message_id, edit_text)
        else:
            await reply_telegram(chat_id, edit_text)

# --- INTERAKTYWNY TRYB SKLEPOWY TELEGRAM ---
def build_shop_keyboard(items: list[dict]) -> tuple[str, list]:
    """Buduje czytelną wiadomość i klawiaturę Inline dla trybu sklepowego.
    Każda pozycja (nawet z ilością np. '2 sztuki') jest odznaczana jednym kliknięciem.
    """
    if not items:
        return "🎉 <b>Lista zakupów jest pusta!</b>\nWszystko zostało już kupione. Dobra robota! ✨", []

    text = (
        f"🛒 <b>TRYB SKLEPOWY ({len(items)} do kupienia):</b>\n"
        f"<i>Dotknij produktu, aby potwierdzić zakup (np. mleko 2 szt. skreśla się jednym kliknięciem):</i>"
    )

    keyboard = []
    for it in items:
        item_id = it.get("id")
        name = it.get("item", "Produkt")
        qty = str(it.get("quantity") or "1").strip()
        qty_str = f" ({qty})" if qty and qty != "1" else ""
        btn_title = f"🛒 {name}{qty_str}"
        if len(btn_title) > 36:
            btn_title = btn_title[:33] + "..."
        keyboard.append([{"text": btn_title, "callback_data": f"shop_buy:{item_id}"}])

    keyboard.append([
        {"text": "🔄 Odśwież listę", "callback_data": "shop_refresh"},
        {"text": "🏁 Zakończ zakupy", "callback_data": "shop_done"}
    ])
    return text, keyboard


async def send_shop_mode(chat_id: int | str, user_name: str) -> None:
    """Wysyła interaktywny widok trybu sklepowego na Telegramie."""
    url = f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&order=id.asc"
    try:
        client = get_http_client()
        resp = await client.get(url, headers=supabase_headers())
        items = resp.json() if resp.status_code == 200 else []
        text, keyboard = build_shop_keyboard(items)
        if keyboard:
            await send_telegram_with_keyboard(chat_id, text, keyboard)
        else:
            await reply_telegram(chat_id, text)
    except Exception as exc:
        logger.error("Błąd podczas uruchamiania trybu sklepowego: %s", exc)
        await reply_telegram(chat_id, "Wystąpił błąd podczas ładowania listy zakupów.")


async def handle_shop_callback(
    callback_query_id: str,
    chat_id: int | str,
    message_id: Optional[int],
    data: str,
    user_name: str
) -> None:
    """Obsługuje interakcje w trybie sklepowym (zakup, odświeżenie, zakończenie)."""
    client = get_http_client()

    if data == "shop_refresh":
        url = f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&order=id.asc"
        resp = await client.get(url, headers=supabase_headers())
        items = resp.json() if resp.status_code == 200 else []
        text, keyboard = build_shop_keyboard(items)
        await answer_telegram_callback(callback_query_id, "Zaktualizowano listę")
        if message_id:
            await edit_telegram_message(
                chat_id,
                message_id,
                text,
                remove_keyboard=(len(keyboard) == 0),
                inline_keyboard=keyboard
            )
        return

    if data == "shop_done":
        await answer_telegram_callback(callback_query_id, "Zakończono zakupy!")
        finish_text = (
            f"🛒 <b>Zakupy zakończone przez {user_name}!</b>\n"
            f"Lista została zsynchronizowana z aplikacją domową Home Hub. 👏"
        )
        if message_id:
            await edit_telegram_message(chat_id, message_id, finish_text, remove_keyboard=True)
        return

    if data.startswith("shop_buy:"):
        item_id_str = data.split(":", 1)[1]
        try:
            item_id = int(item_id_str)
        except ValueError:
            await answer_telegram_callback(callback_query_id, "Błąd identyfikatora produktu.")
            return

        # 1. Pobierz dane o produkcie przed oznaczeniem
        get_url = f"{SUPABASE_URL}/rest/v1/home_shopping?id=eq.{item_id}&select=item,quantity,is_bought"
        get_resp = await client.get(get_url, headers=supabase_headers())
        bought_item_name = "Produkt"
        if get_resp.status_code == 200 and get_resp.json():
            item_data = get_resp.json()[0]
            if item_data.get("is_bought"):
                await answer_telegram_callback(callback_query_id, "Produkt został już wcześniej odhaczony!")
                return
            raw_name = item_data.get("item", "Produkt")
            raw_qty = str(item_data.get("quantity") or "1").strip()
            bought_item_name = f"{raw_name} ({raw_qty})" if raw_qty and raw_qty != "1" else raw_name

        # 2. Oznacz produkt jako kupiony (jednorazowe potwierdzenie całej pozycji)
        patch_url = f"{SUPABASE_URL}/rest/v1/home_shopping?id=eq.{item_id}"
        await client.patch(patch_url, headers=supabase_headers(), json={"is_bought": True})

        # 3. Odśwież natychmiast frontend PWA przez WebSocket (0ms opóźnienia u Darii i Bartosza)
        await ws_manager.broadcast_refresh("shopping")

        # 4. Wyświetl toast w Telegramie
        await answer_telegram_callback(callback_query_id, f"✅ Kupiono: {bought_item_name}!")

        # 5. Pobierz pozostałe niekupione pozycje i zaktualizuj wiadomość w Telegramie
        url_rem = f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&order=id.asc"
        resp_rem = await client.get(url_rem, headers=supabase_headers())
        remaining_items = resp_rem.json() if resp_rem.status_code == 200 else []

        if not remaining_items:
            final_text = (
                f"🎉 <b>WSZYSTKIE ZAKUPY ZROBIONE!</b>\n"
                f"Ostatnio kupiono: <b>{bought_item_name}</b> (przez: <b>{user_name}</b>).\n"
                f"Koszyk jest pusty. Dobra robota! 👏"
            )
            if message_id:
                await edit_telegram_message(chat_id, message_id, final_text, remove_keyboard=True)
        else:
            text, keyboard = build_shop_keyboard(remaining_items)
            text += f"\n\n✅ <i>Ostatnio odhaczono: <b>{bought_item_name}</b> ({user_name})</i>"
            if message_id:
                await edit_telegram_message(
                    chat_id,
                    message_id,
                    text,
                    remove_keyboard=False,
                    inline_keyboard=keyboard
                )


def select_models_for_task(user_prompt: str, media_parts: Optional[list] = None) -> list[str]:
    """Inteligentny dobór kaskady modeli Gemini w oparciu o poziom skomplikowania zadania oraz limity konta:
    - Modele Lite (gemini-3.5-flash-lite, gemini-flash-lite-latest, gemini-3.1-flash-lite):
      Limit konta: 500 zapytań/dzień (RPD), 15 zapytań/minutę (RPM).
    - Modele Top (gemini-3.8-flash, gemini-3.6-flash, gemini-flash-latest):
      Limit konta: 20 zapytań/dzień (RPD), 5 zapytań/minutę (RPM).

    Strategia:
    1. Zadania złożone (zdjęcia paragonów/lodówki/harmonogramów, nagrania głosowe audio, rozbudowane zapytania):
       Startujemy od modeli Top (3.8 Flash, 3.6 Flash) dla najwyższej precyzji OCR i transkrypcji.
       Gdy napotkamy limit (429 Quota Exceeded) lub przeciążenie (503 High Demand) -> automatycznie i bezbłędnie
       przechodzimy na sprawdzony Lite (3.5 Flash Lite, flash-lite-latest), który także świetnie radzi sobie z multimodalem.
    2. Zadania proste (krótkie wiadomości tekstowe, zakupy, pojedynczy termin, odznaczanie kupionego):
       Startujemy bezpośrednio z puli Lite (500 zapytań/dzień!), oszczędzając limit 20 zapytań modeli Top.
       W razie niedostępności Lite przechodzimy na Top.
    """
    has_media = bool(media_parts and len(media_parts) > 0)
    prompt_len = len(user_prompt.strip()) if user_prompt else 0

    complex_keywords = [
        "zaplanuj", "jadłospis", "jadlospis", "przepis", "ugotuj", "porównaj", "porownaj",
        "przeanalizuj", "harmonogram", "rozpisz", "co polecasz", "dlaczego", "opowiedz", "podsumuj"
    ]
    is_complex_query = any(k in user_prompt.lower() for k in complex_keywords) if user_prompt else False
    is_long_text = prompt_len > 120

    is_heavy_task = has_media or is_complex_query or is_long_text

    if is_heavy_task:
        return [
            "gemini-3.8-flash",
            "gemini-3.6-flash",
            "gemini-flash-latest",
            "gemini-3.5-flash-lite",
            "gemini-flash-lite-latest",
            "gemini-3.1-flash-lite"
        ]
    else:
        return [
            "gemini-3.5-flash-lite",
            "gemini-flash-lite-latest",
            "gemini-3.1-flash-lite",
            "gemini-3.8-flash",
            "gemini-3.6-flash"
        ]


async def run_gemini_agent(
    user_prompt: str,
    sender: str,
    media_parts: Optional[list] = None
) -> tuple[str, bool, list[dict]]:
    """Wywołuje Agenta Gemini z kontekstem aktualnego stanu domu i adaptacyjnym doborem modelu."""
    if not GEMINI_API_KEY:
        return "", False, []

    today_str = str(datetime.date.today())
    now_time = datetime.datetime.now().strftime("%H:%M")
    client = get_http_client()

    # 1. Równoległe, zwięzłe pobranie stanu z Supabase (oszczędność ~300ms i 80% tokenów)
    shopping_text = "brak"
    calendar_text = "brak"

    try:
        s_task = client.get(
            f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&select=item,quantity&order=id.asc&limit=15",
            headers=supabase_headers()
        )
        c_task = client.get(
            f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today_str}&select=title,event_date,event_time,category&order=event_date.asc&limit=10",
            headers=supabase_headers()
        )
        s_res, c_res = await asyncio.gather(s_task, c_task, return_exceptions=True)

        if not isinstance(s_res, Exception) and s_res.status_code == 200:
            items = s_res.json()
            if items:
                shopping_text = ", ".join([f"{it['item']} ({it.get('quantity') or '1'})" for it in items])

        if not isinstance(c_res, Exception) and c_res.status_code == 200:
            evs = c_res.json()
            if evs:
                calendar_text = "; ".join([f"{ev['event_date']} {ev.get('event_time') or ''}: {ev['title']} [{ev.get('category')}]" for ev in evs])
    except Exception as e:
        logger.warning("Błąd szybkiego odczytu kontekstu: %s", e)

    # Zoptymalizowany, ultra-zwięzły prompt (zmniejsza zużycie tokenów o ponad 60%)
    system_prompt = f"""Asystent domowy Home Hub dla Bartosza i Darii (Gąsawa, 88-410). Dziś: {today_str} {now_time}.
Aktywne zakupy: {shopping_text}.
Nadchodzący kalendarz: {calendar_text}.
Instrukcje:
1. Zakupy -> 'add_shopping' (nazwy, ilości).
2. Kupione rzeczy ('kupiłem', 'skreśl') -> 'complete_shopping'.
3. Terminy/wydarzenia/śmieci (tekst/foto/głos) -> 'add_calendar' ('calendar_event' lub 'calendar_events', kategoria: Bartosz/Daria/Wspólne/Śmieci).
4. W polu 'reply' napisz zwięzłą, naturalną odpowiedź (1-2 zdania, emoji)."""

    generation_config = {
        "temperature": 0.2,
        "max_output_tokens": 400,
        "response_mime_type": "application/json",
        "response_schema": {
            "type": "OBJECT",
            "properties": {
                "actions": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "action": {
                                "type": "STRING",
                                "enum": ["add_shopping", "complete_shopping", "add_calendar", "delete_calendar", "none"]
                            },
                            "shopping_items": {
                                "type": "ARRAY",
                                "items": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "item": {"type": "STRING"},
                                        "quantity": {"type": "STRING"}
                                    },
                                    "required": ["item"]
                                }
                            },
                            "complete_items": {
                                "type": "ARRAY",
                                "items": {"type": "STRING"}
                            },
                            "calendar_event": {
                                "type": "OBJECT",
                                "properties": {
                                    "title": {"type": "STRING"},
                                    "event_date": {"type": "STRING"},
                                    "event_time": {"type": "STRING"},
                                    "category": {"type": "STRING", "enum": ["Bartosz", "Daria", "Wspólne", "Śmieci"]}
                                },
                                "required": ["title", "event_date", "category"]
                            },
                            "calendar_events": {
                                "type": "ARRAY",
                                "items": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "title": {"type": "STRING"},
                                        "event_date": {"type": "STRING"},
                                        "event_time": {"type": "STRING"},
                                        "category": {"type": "STRING", "enum": ["Bartosz", "Daria", "Wspólne", "Śmieci"]}
                                    },
                                    "required": ["title", "event_date", "category"]
                                }
                            },
                            "delete_calendar_query": {"type": "STRING"}
                        },
                        "required": ["action"]
                    }
                },
                "reply": {"type": "STRING"}
            },
            "required": ["actions", "reply"]
        }
    }

    parts = []
    if media_parts:
        parts.extend(media_parts)
    parts.append({"text": f"Nadawca: {sender}\nTreść: {user_prompt or 'Przeanalizuj przesłany załącznik'}"})

    models_to_try = select_models_for_task(user_prompt, media_parts)
    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": parts}],
        "generation_config": generation_config
    }

    try:
        resp = None
        used_model = None
        for m in models_to_try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={GEMINI_API_KEY}"
            try:
                r = await client.post(url, json=payload, timeout=20.0)
                if r.status_code == 200:
                    resp = r
                    used_model = m
                    logger.info("Zapytanie AI pomyślnie zrealizowane przez model: %s", m)
                    break
                elif r.status_code == 429:
                    logger.warning("Model %s osiągnął limit (429 Quota Exceeded). Przechodzę do kolejnego...", m)
                elif r.status_code == 503:
                    logger.warning("Model %s jest przeciążony (503 High Demand). Przechodzę do kolejnego...", m)
                else:
                    logger.warning("Model %s zwrócił kod %s (%s). Próbuję kolejnego...", m, r.status_code, r.text[:100])
            except (httpx.TimeoutException, httpx.RequestError) as net_err:
                logger.warning("Timeout/błąd sieciowy dla %s: %s. Próbuję kolejnego...", m, net_err)

        if not resp or resp.status_code != 200:
            logger.error("Wszystkie modele w kaskadzie Gemini wyczerpały limity lub napotkały błąd!")
            return "", False, []

        data = resp.json()
        raw_json = data["candidates"][0]["content"]["parts"][0]["text"]
        result = json.loads(raw_json)

        actions = result.get("actions", [])
        reply_text = result.get("reply", "Zrobione!")

        refresh_shopping = False
        refresh_calendar = False
        proposed_calendar_events = []
        db_tasks = []

        for act in actions:
            action_type = act.get("action")

            if action_type == "add_shopping" and act.get("shopping_items"):
                for s_item in act["shopping_items"]:
                    it_name = s_item.get("item", "").strip()
                    it_qty = s_item.get("quantity", "1").strip() or "1"
                    if it_name:
                        db_tasks.append(client.post(
                            f"{SUPABASE_URL}/rest/v1/home_shopping",
                            headers=supabase_headers(),
                            json={"item": it_name, "quantity": it_qty, "category": "Wszystko", "added_by": f"AI: {sender}"}
                        ))
                        refresh_shopping = True

            elif action_type == "complete_shopping" and act.get("complete_items"):
                for c_name in act["complete_items"]:
                    db_tasks.append(client.patch(
                        f"{SUPABASE_URL}/rest/v1/home_shopping?item=ilike.*{c_name.strip()}*&is_bought=eq.false",
                        headers=supabase_headers(),
                        json={"is_bought": True}
                    ))
                    refresh_shopping = True

            elif action_type == "add_calendar":
                if act.get("calendar_event"):
                    ev_item = act["calendar_event"]
                    if ev_item.get("title") and ev_item.get("event_date"):
                        proposed_calendar_events.append(ev_item)
                if act.get("calendar_events"):
                    for ev_item in act["calendar_events"]:
                        if ev_item.get("title") and ev_item.get("event_date"):
                            proposed_calendar_events.append(ev_item)

            elif action_type == "delete_calendar" and act.get("delete_calendar_query"):
                query = act["delete_calendar_query"].strip()
                db_tasks.append(client.delete(
                    f"{SUPABASE_URL}/rest/v1/home_calendar?title=ilike.*{query}*",
                    headers=supabase_headers()
                ))
                refresh_calendar = True

        if db_tasks:
            await asyncio.gather(*db_tasks, return_exceptions=True)

        if refresh_shopping:
            await ws_manager.broadcast_refresh("shopping")
        if refresh_calendar:
            await ws_manager.broadcast_refresh("calendar")

        return reply_text, True, proposed_calendar_events

    except Exception as exc:
        logger.error("Błąd wykonania Agenta Gemini: %s", exc)
        return "", False, []


def check_telegram_authorization(user_id_str: str, chat_id_str: str) -> tuple[bool, str]:
    """Weryfikuje czy nadawca jest uprawnionym domownikiem (Daria lub Bartosz).
    Wszyscy inni są bezwzględnie blokowani.
    """
    valid_daria = {str(WIFE_TELEGRAM_ID).strip()} - {"", "None"}
    valid_bartosz = {str(BARTOSZ_TELEGRAM_ID).strip(), str(TELEGRAM_CHAT_ID).strip()} - {"", "None"}

    u_str = str(user_id_str or "").strip()
    c_str = str(chat_id_str or "").strip()

    if (u_str and u_str in valid_daria) or (c_str and c_str in valid_daria):
        return True, "Daria"
    if (u_str and u_str in valid_bartosz) or (c_str and c_str in valid_bartosz):
        return True, "Bartosz"
    return False, ""


# --- TELEGRAM WEBHOOK ---
@app.post("/api/webhook/telegram")
async def telegram_webhook(request: Request, update: dict):
    # Weryfikacja Secret Token Telegrama (jeśli skonfigurowany w środowisku)
    if TELEGRAM_WEBHOOK_SECRET:
        secret_header = request.headers.get("x-telegram-bot-api-secret-token", "")
        if not secrets.compare_digest(secret_header, TELEGRAM_WEBHOOK_SECRET):
            logger.warning("Odrzucono zapytanie do webhooka Telegram: nieprawidłowy nagłówek Secret Token")
            raise HTTPException(status_code=403, detail="Brak uprawnień.")

    # 0. Obsługa kliknięć w przyciski Inline (callback_query)
    if "callback_query" in update:
        cq = update["callback_query"]
        cq_id = str(cq.get("id", ""))
        data = str(cq.get("data", ""))
        msg = cq.get("message", {})
        chat_id = msg.get("chat", {}).get("id")
        message_id = msg.get("message_id")
        from_user = cq.get("from", {})
        from_id_str = str(from_user.get("id", ""))
        chat_id_str = str(chat_id) if chat_id else ""

        is_auth, user_name = check_telegram_authorization(from_id_str, chat_id_str)
        if not is_auth:
            logger.warning("ZABLOKOWANO nieautoryzowany callback Telegram od ID: %s (chat %s)", from_id_str, chat_id_str)
            await answer_telegram_callback(cq_id, "⛔ Brak dostępu. Bot jest prywatny.")
            return {"ok": True}

        if data.startswith("shop_"):
            await handle_shop_callback(cq_id, chat_id, message_id, data, user_name)
            return {"ok": True}

        await handle_calendar_callback(cq_id, chat_id, message_id, data, user_name)
        return {"ok": True}

    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")

    if not chat_id:
        return {"ok": True}

    text = message.get("text", "").strip()
    caption = message.get("caption", "").strip()
    photo = message.get("photo")
    voice = message.get("voice")

    if not text and not photo and not voice:
        return {"ok": True}

    raw_sender = message.get("from", {})
    user_id_str = str(raw_sender.get("id", ""))
    chat_id_str = str(chat_id)

    # Ścisła autoryzacja nadawcy: Daria lub Bartosz
    is_auth, sender = check_telegram_authorization(user_id_str, chat_id_str)
    if not is_auth:
        logger.warning(
            "ODRZUCONO nieautoryzowaną wiadomość Telegram od nieznanego użytkownika: user_id=%s, chat_id=%s",
            user_id_str,
            chat_id_str
        )
        await reply_telegram(chat_id, "⛔ Dostęp zablokowany. Ten bot jest prywatnym asystentem domowym.")
        return {"ok": True}

    # Bezpieczna rejestracja/aktualizacja subskrybenta powiadomień w Supabase
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/telegram_subscribers",
                headers=supabase_headers(),
                json={"chat_id": chat_id_str, "name": sender}
            )
    except Exception:
        pass

    logger.info("Odebrano Telegram od %s (user %s, chat %s): text=%s, photo=%s, voice=%s", sender, user_id_str, chat_id, bool(text), bool(photo), bool(voice))

    # Sprawdzenie czy użytkownik odpowiada tekstowo na oczekujące zatwierdzenie kalendarza
    clean_text = text.lower().strip()
    last_token = LAST_PENDING_PER_CHAT.get(chat_id_str)
    if last_token and last_token in PENDING_CALENDAR_ACTIONS:
        if clean_text in ["tak", "zatwierdź", "zatwierdz", "dodaj", "ok", "jasne", "potwierdzam", "zapisz", "pewnie", "tak jest"]:
            pending = PENDING_CALENDAR_ACTIONS.pop(last_token, None)
            LAST_PENDING_PER_CHAT.pop(chat_id_str, None)
            if pending:
                events = pending.get("events", [])
                async with httpx.AsyncClient(timeout=10.0) as client:
                    for ev in events:
                        await client.post(
                            f"{SUPABASE_URL}/rest/v1/home_calendar",
                            headers=supabase_headers(),
                            json={
                                "title": ev.get("title", "Wydarzenie"),
                                "event_date": ev.get("event_date"),
                                "event_time": ev.get("event_time", ""),
                                "category": ev.get("category", "Wspólne"),
                                "created_by": f"TG ({sender})"
                            }
                        )
                await ws_manager.broadcast_refresh("calendar")
                summary = format_events_summary(events)
                await reply_telegram(
                    chat_id,
                    f"✅ <b>ZATWIERDZONO! Dodano do kalendarza ({sender}):</b>\n\n{summary}\n\n🔔 <i>Powiadomienia i przypomnienia Telegram zostały zaplanowane.</i>"
                )
                return {"ok": True}

        elif clean_text in ["nie", "anuluj", "odrzuć", "odrzuc", "skasuj", "nie dodawaj", "stop", "wstrzymaj"]:
            PENDING_CALENDAR_ACTIONS.pop(last_token, None)
            LAST_PENDING_PER_CHAT.pop(chat_id_str, None)
            await reply_telegram(chat_id, f"❌ <b>Odrzucono dodawanie ({sender}).</b>\nŻadne wydarzenia nie zostały zapisane w kalendarzu.")
            return {"ok": True}

    # 1. Obsługa zdjęć (Multimodal - lodówka, paragon, harmonogram śmieci z kartki)
    if photo:
        file_id = photo[-1]["file_id"]
        content, _ = await download_telegram_file(file_id)
        if content:
            b64_img = base64.b64encode(content).decode("utf-8")
            media_parts = [{"inline_data": {"mime_type": "image/jpeg", "data": b64_img}}]
            prompt = caption or "Przeanalizuj to zdjęcie. Jeśli to lista zakupów lub paragon/lodówka - dodaj brakujące produkty. Jeśli to harmonogram śmieci - dodaj terminy do kalendarza."
            reply, success, pending_events = await run_gemini_agent(prompt, sender, media_parts)
            if success:
                if pending_events:
                    await ask_calendar_confirmation(chat_id, sender, pending_events, intro_msg=reply)
                elif reply:
                    await reply_telegram(chat_id, reply)
                return {"ok": True}

    # 2. Obsługa notatek głosowych (Voice notes)
    if voice:
        file_id = voice["file_id"]
        content, _ = await download_telegram_file(file_id)
        if content:
            b64_audio = base64.b64encode(content).decode("utf-8")
            media_parts = [{"inline_data": {"mime_type": "audio/ogg", "data": b64_audio}}]
            reply, success, pending_events = await run_gemini_agent("Wiadomość głosowa. Wykonaj dyspozycje domownika.", sender, media_parts)
            if success:
                if pending_events:
                    await ask_calendar_confirmation(chat_id, sender, pending_events, intro_msg=reply)
                elif reply:
                    await reply_telegram(chat_id, reply)
                return {"ok": True}

    # 3. Komendy systemowe / pomoc
    if text.lower() in ("/start", "/help", "/pomoc"):
        help_msg = (
            f"👋 Cześć <b>{sender}</b>! Jestem <b>Agentem AI Home Hub (Gemini 3.5)</b> 🧠🏠\n\n"
            "Możesz ze mną rozmawiać zupełnie swobodnie:\n\n"
            "🛒 <b>Zakupy:</b>\n"
            "• <i>'Kup 2 mleka, 5 bułek i sok'</i>\n"
            "• <i>'Dopisz składniki na pizzę dla 4 osób'</i>\n"
            "• <i>'Kupiłem masło i chleb, skreśl je'</i>\n\n"
            "📅 <b>Kalendarz & Śmieci:</b>\n"
            "• <i>'Dodaj kalendarz wspólny szpital miejski 9 listopad usg'</i>\n"
            "• <i>'Jutro o 16:30 dentysta dla Darii'</i>\n"
            "• <i>'Wywóz plastiku w piątek'</i>\n"
            "• <i>'Kiedy mamy najbliższy wywóz bio?'</i>\n\n"
            "📸 <b>Zdjęcia & 🎙️ Głos:</b>\n"
            "• Prześlij <b>zdjęcie lodówki</b> lub <b>harmonogramu śmieci</b> z kartki!\n"
            "• Nagraj <b>notatkę głosową</b> w biegu lub z auta!\n\n"
            "Aplikacja: https://home-hub-sewt.onrender.com/"
        )
        await reply_telegram(chat_id, help_msg)
        return {"ok": True}

    # 4. Szybkie skróty podglądu i interaktywny TRYB SKLEPOWY
    SHOP_TRIGGERS = (
        "/sklep", "sklep", "/zakupy", "zakupy", "/lista", "lista",
        "tryb sklepowy", "idę do sklepu", "ide do sklepu", "jestem w sklepie",
        "co kupić", "co kupic", "co mamy kupić", "co mamy kupic", "pokaż zakupy", "pokaz zakupy"
    )
    if text.lower().strip() in SHOP_TRIGGERS:
        await send_shop_mode(chat_id, sender)
        return {"ok": True}

    if text.lower() in ("/kalendarz", "kalendarz", "/terminy", "terminy", "/wydarzenia", "wydarzenia"):
        today_str = str(datetime.date.today())
        url = f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today_str}&order=event_date.asc,event_time.asc&limit=15"
        client = get_http_client()
        resp = await client.get(url, headers=supabase_headers())
        events = resp.json() if resp.status_code == 200 else []

        if not events:
            await reply_telegram(chat_id, "📅 <b>Kalendarz jest pusty!</b> Brak nadchodzących wydarzeń.")
        else:
            cat_icons = {"Bartosz": "🧔", "Daria": "🌸", "Wspólne": "🏡", "Śmieci": "🗑️"}
            lines = ["📅 <b>Nadchodzące wydarzenia i terminy:</b>"]
            for ev in events:
                icon = cat_icons.get(ev.get("category", "Wspólne"), "📌")
                time_str = f" ({ev.get('event_time')})" if ev.get("event_time") else ""
                lines.append(f"• {icon} <b>{ev.get('event_date')}{time_str}</b>: {ev.get('title')} <i>[{ev.get('category')}]</i>")
            await reply_telegram(chat_id, "\n".join(lines))
        return {"ok": True}

    # Słowa kluczowe wskazujące na zamiar kalendarza / wydarzenia
    CALENDAR_KEYWORDS = [
        "kalendarz", "kalednarz", "termin", "wydarzen", "spotkan", "wizyt",
        "lekarz", "szpital", "doktor", "badani", "usg", "dentyst", "stomatolog",
        "urodzin", "imienin", "rocznic",
        "śmieci", "smieci", "odpad", "wywóz", "wywoz", "odbior", "odbiór", "bio", "plastik", "papier", "szkło", "szklo",
        "listopad", "grudzi", "stycz", "lut", "marz", "kwiet", "maj", "czerwc", "lip", "sierp", "wrześ", "wrzes", "październik", "pazdziernik",
        "jutro", "pojutrze", "dzisiaj", "dziś"
    ]
    has_calendar_keyword = any(k in text.lower() for k in CALENDAR_KEYWORDS)

    # 5. SZYBKA ŚCIEŻKA (FAST-PATH) - Natychmiastowa obsługa (<50ms) bez zużywania limitów/tokenów AI
    # A. Bezpośrednie komendy kalendarza (np. "termin dentysta jutro 16:00", "Dodaj kalednarz ...", "śmieci piątek")
    CALENDAR_PREFIXES = [
        "/kalendarz", "kalendarz", "/kalednarz", "kalednarz",
        "/termin", "termin", "/smieci", "smieci", "/śmieci", "śmieci",
        "/wydarzenie", "wydarzenie", "/spotkanie", "spotkanie", "/wizyta", "wizyta",
        "dodaj kalendarz", "dodaj kalednarz", "dodaj do kalendarza", "dodaj termin",
        "dodaj do terminu", "dodaj wydarzenie", "dodaj wizytę", "dodaj wizyte",
        "dodaj spotkanie", "dodaj śmieci", "dodaj smieci"
    ]
    is_calendar_prefix = any(text.lower().startswith(p) for p in CALENDAR_PREFIXES)

    if is_calendar_prefix:
        title, ev_date, ev_time, cat = parse_calendar_input(text, sender)
        pending_events = [{
            "title": title,
            "event_date": ev_date,
            "event_time": ev_time,
            "category": cat
        }]
        await ask_calendar_confirmation(chat_id, sender, pending_events)
        return {"ok": True}

    # B. Bezpośrednie komendy zakupów (np. "kup 2 mleka, 5 bułek i masło", "/kup chleb")
    # WAŻNE: Szybka ścieżka zakupów uruchamia się TYLKO gdy tekst NIE zawiera słów kluczowych kalendarza!
    is_shopping_prefix = False
    if not has_calendar_keyword:
        is_shopping_prefix = any(text.lower().startswith(p) for p in [
            "/kup ", "kup ", "/dodaj do listy ", "dodaj do listy ", "/dodaj do zakupów ", "dodaj do zakupów ",
            "/dodaj ", "dodaj "
        ])

    if is_shopping_prefix:
        parsed_items = split_and_parse_shopping(text)
        if parsed_items:
            client = get_http_client()
            url = f"{SUPABASE_URL}/rest/v1/home_shopping"
            db_tasks = [
                client.post(
                    url,
                    headers=supabase_headers(),
                    json={"item": item_name, "quantity": qty, "category": "Wszystko", "added_by": f"TG: {sender}"}
                )
                for item_name, qty in parsed_items if item_name
            ]
            if db_tasks:
                await asyncio.gather(*db_tasks, return_exceptions=True)
                await ws_manager.broadcast_refresh("shopping")
                added_summary = [f"{it} ({q})" if q != "1" else it for it, q in parsed_items if it]
                await reply_telegram(chat_id, f"✅ <b>Dodano do listy ({sender}):</b>\n• " + "\n• ".join(added_summary))
                return {"ok": True}

    # 6. GŁÓWNY SILNIK: AGENT AI GEMINI (dla konwersacji, pytań kulinarnych, skreślania kupionego, planowania)
    reply, success, pending_events = await run_gemini_agent(text, sender)
    if success:
        if pending_events:
            await ask_calendar_confirmation(chat_id, sender, pending_events, intro_msg=reply)
        elif reply:
            await reply_telegram(chat_id, reply)
        return {"ok": True}

    # 7. AWARYJNY FALLBACK (gdyby Gemini było offline, a użytkownik wpisał zakupy bez słów kalendarzowych)
    if not has_calendar_keyword:
        parsed_items = split_and_parse_shopping(text)
        if parsed_items:
            client = get_http_client()
            url = f"{SUPABASE_URL}/rest/v1/home_shopping"
            db_tasks = [
                client.post(
                    url,
                    headers=supabase_headers(),
                    json={"item": item_name, "quantity": qty, "category": "Wszystko", "added_by": f"TG: {sender}"}
                )
                for item_name, qty in parsed_items if item_name
            ]
            if db_tasks:
                await asyncio.gather(*db_tasks, return_exceptions=True)
                await ws_manager.broadcast_refresh("shopping")
                added_summary = [f"{it} ({q})" if q != "1" else it for it, q in parsed_items if it]
                await reply_telegram(chat_id, f"✅ <b>Dodano do listy ({sender}):</b>\n• " + "\n• ".join(added_summary))

    return {"ok": True}


# --- WHATSAPP WEBHOOK (TWILIO) ---
@app.post("/api/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    if not TWILIO_AUTH_TOKEN:
        logger.warning("Zablokowano próbę wywołania /api/webhook/whatsapp: integracja Twilio nie jest skonfigurowana.")
        raise HTTPException(status_code=404, detail="Webhook wyłączony.")

    body = ""
    sender = "WhatsApp"
    try:
        form = await request.form()
        body = str(form.get("Body", "")).strip()
        sender = str(form.get("ProfileName") or form.get("From", "WhatsApp")).replace("whatsapp:", "")
    except Exception:
        raw_body = (await request.body()).decode("utf-8")
        from urllib.parse import parse_qs
        parsed = parse_qs(raw_body)
        body = parsed.get("Body", [""])[0].strip()
        sender = parsed.get("ProfileName", parsed.get("From", ["WhatsApp"]))[0].replace("whatsapp:", "")

    logger.info("Odebrano wiadomość WhatsApp od %s: %s", sender, body)

    if not body:
        return Response(content='<Response></Response>', media_type="application/xml")

    # Obsługa: "kup mleko", "/kup mleko" lub bezpośrednio "mleko, chleb"
    text = body
    if text.lower().startswith("/kup "):
        text = text[5:].strip()
    elif text.lower().startswith("kup "):
        text = text[4:].strip()

    items_raw = [i.strip() for i in text.split(",") if i.strip()]
    if not items_raw:
        return Response(content='<Response></Response>', media_type="application/xml")

    url = f"{SUPABASE_URL}/rest/v1/home_shopping"
    added_summary = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for raw in items_raw:
            item_name = raw
            qty = "1"
            if " - " in raw:
                parts = raw.split(" - ", 1)
                item_name = parts[0].strip()
                qty = parts[1].strip()
            elif "x " in raw and raw.split("x ", 1)[0].isdigit():
                parts = raw.split("x ", 1)
                qty = f"{parts[0]} szt."
                item_name = parts[1].strip()

            added_summary.append(f"{item_name} ({qty})" if qty != "1" else item_name)
            await client.post(
                url,
                headers=supabase_headers(),
                json={"item": item_name, "quantity": qty, "category": "Wszystko", "added_by": f"WA: {sender}"}
            )

    await ws_manager.broadcast_refresh("shopping")

    reply_text = f"✅ Dodano do listy zakupów: {', '.join(added_summary)}"
    twiml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{reply_text}</Message></Response>'
    return Response(content=twiml, media_type="application/xml")

