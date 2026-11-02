# main.py
import os
import re
import json
import base64
import asyncio
import datetime
import logging
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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "9062746866:AAE4nnHqbNokIEdFnLVZs3SI8bzYsv01ZuQ")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
WIFE_TELEGRAM_ID = "9216839954"
BARTOSZ_TELEGRAM_ID = "7939517140"
LATITUDE = os.environ.get("LATITUDE", "52.53")
LONGITUDE = os.environ.get("LONGITUDE", "17.60")

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


def verify_pin(x_app_pin: Optional[str] = Header(None)) -> None:
    if x_app_pin != APP_PIN:
        raise HTTPException(status_code=401, detail="Błędny kod PIN.")


def supabase_headers() -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(status_code=500, detail="Brak konfiguracji SUPABASE_URL / SUPABASE_KEY.")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }


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


async def check_and_send_calendar_reminders():
    """Sprawdza reguły powiadomień:
    - Dla śmieci: 1 dzień przed oraz w dniu wywozu rano (od godz. 6:00)
    - Dla pozostałych wydarzeń: 7 dni przed, 1 dzień przed oraz na 1h przed wydarzeniem (o ile podano godzinę)
    """
    if not SUPABASE_URL or not SUPABASE_KEY or not TELEGRAM_BOT_TOKEN:
        return

    now = get_poland_time()
    today = now.date()
    # Sprawdzamy okno od dziś do 8 dni do przodu
    url = f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today}&event_date=lte.{today + datetime.timedelta(days=8)}&order=event_date.asc"

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
                    # 1 dzień przed
                    if days_until == 1 and "smieci_1d_before" not in notified:
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
                    time_info = f" o godz. <b>{ev_time}</b>" if ev_time else ""

                    # 7 dni przed
                    if days_until == 7 and "7d_before" not in notified:
                        msg = (
                            f"📅 <b>Przypomnienie (za 7 dni):</b>\n"
                            f"• {icon} <b>{title}</b> <i>[{category}]</i>\n"
                            f"• Termin: <b>{ev_date}</b>{time_info}"
                        )
                        await send_telegram_alert(msg, category=category)
                        new_milestones.add("7d_before")

                    # 1 dzień przed
                    if days_until == 1 and "1d_before" not in notified:
                        msg = (
                            f"⏰ <b>Przypomnienie na JUTRO:</b>\n"
                            f"• {icon} <b>{title}</b> <i>[{category}]</i>\n"
                            f"• Termin: <b>{ev_date}</b>{time_info}"
                        )
                        await send_telegram_alert(msg, category=category)
                        new_milestones.add("1d_before")

                    # 1h przed wydarzeniem (o ile podano godzinę)
                    if days_until == 0 and ev_time and "1h_before" not in notified:
                        m_time = re.match(r"^(\d{1,2}):(\d{2})$", ev_time)
                        if m_time:
                            h = int(m_time.group(1))
                            m = int(m_time.group(2))
                            ev_dt = datetime.datetime.combine(ev_date, datetime.time(h, m)).replace(tzinfo=now.tzinfo)
                            diff_minutes = (ev_dt - now).total_seconds() / 60
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
    # Start tła dla przypomnień
    reminder_task = asyncio.create_task(calendar_reminder_loop())
    yield
    reminder_task.cancel()


app = FastAPI(title="Nexus Cloud Hub", lifespan=lifespan)

# CORS Middleware (obsługa localhost, 127.0.0.1, LiveServer i urządzeń w sieci)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        f"&current=temperature_2m,wind_speed_10m"
        f"&daily=precipitation_probability_max&timezone=Europe%2FWarsaw&forecast_days=1"
    )
    async with httpx.AsyncClient(timeout=5.0) as client:
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


@app.post("/api/home/reminders/trigger", dependencies=[Depends(verify_pin)])
async def trigger_reminders_manually():
    """Ręczne sprawdzenie i wysyłka przypomnień kalendarza."""
    await check_and_send_calendar_reminders()
    return {"status": "ok", "message": "Pomyślnie sprawdzono przypomnienia"}


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


def parse_calendar_input(text: str, default_user: str = "Bartosz") -> tuple[str, str, str, str]:
    """Inteligentne rozpoznawanie daty, godziny, tytułu i kategorii:
    - 'kalendarz 15.11 dentysta 16:30' -> ('Dentysta', '2026-11-15', '16:30', 'Bartosz')
    - 'śmieci jutro bio' -> ('Bio', '2026-11-03', '', 'Śmieci')
    - 'termin 20.11 wyjazd z rodzicami Daria' -> ('Wyjazd z rodzicami', '2026-11-20', '', 'Daria')
    """
    now = datetime.date.today()
    cleaned = text.strip()
    for prefix in [
        "/kalendarz ", "kalendarz ", "/termin ", "termin ", "/smieci ", "smieci ",
        "/śmieci ", "śmieci ", "/wydarzenie ", "wydarzenie ", "/event ", "event "
    ]:
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    lower = cleaned.lower()
    cat = "Wspólne"
    if any(k in lower for k in ['śmieci', 'smieci', 'bio', 'plastik', 'papier', 'zmieszane', 'odpady']):
        cat = "Śmieci"
    elif 'daria' in lower:
        cat = "Daria"
    elif any(k in lower for k in ['bartosz', 'bartek']):
        cat = "Bartosz"
    elif any(k in lower for k in ['wspólne', 'wspolne', 'razem', 'dom']):
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
        m_date = re.search(r'\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{4}))?\b', cleaned)
        if m_date:
            day = int(m_date.group(1))
            month = int(m_date.group(2))
            year = int(m_date.group(3)) if m_date.group(3) else now.year
            event_date = f"{year:04d}-{month:02d}-{day:02d}"
            cleaned = cleaned.replace(m_date.group(0), "").strip()

    # Usunięcie zbędnych słów kluczowych z tytułu
    cleaned = re.sub(r'\b(daria|bartosz|bartek|wspólne|wspolne)\b', '', cleaned, flags=re.I).strip()
    cleaned = re.sub(r'^(o|w|na|ze|z|dla)\s+', '', cleaned, flags=re.I).strip()
    return cleaned.capitalize() or "Wydarzenie", event_date, time_str, cat


async def reply_telegram(chat_id: int | str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
    except Exception as exc:
        logger.error("Błąd wysyłki Telegram reply: %s", exc)


async def download_telegram_file(file_id: str) -> tuple[bytes, str]:
    """Pobiera zawartość pliku (zdjęcie, audio) bezpośrednio z Telegram API."""
    if not TELEGRAM_BOT_TOKEN or not file_id:
        return b"", ""
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
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


async def run_gemini_agent(
    user_prompt: str,
    sender: str,
    media_parts: Optional[list] = None
) -> tuple[str, bool]:
    """Wywołuje Agenta Gemini (gemini-3.5-flash) z kontekstem aktualnego stanu domu.
    Wykonuje akcje na bazie Supabase (zakupy, kalendarz, odznaczanie kupionych).
    Zwraca (odpowiedź_dla_użytkownika, czy_sukces).
    """
    if not GEMINI_API_KEY:
        return "", False

    today_str = str(datetime.date.today())
    now_time = datetime.datetime.now().strftime("%H:%M")

    # 1. Pobierz aktualny stan z Supabase
    shopping_text = "Brak aktywnych pozycji (wszystko kupione)."
    calendar_text = "Brak nadchodzących wydarzeń."

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            s_res = await client.get(f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&order=id.asc", headers=supabase_headers())
            if s_res.status_code == 200 and s_res.json():
                items = s_res.json()
                shopping_text = "\n".join([f"- {it.get('item')} ({it.get('quantity', '1')}) [ID: {it.get('id')}]" for it in items])

            c_res = await client.get(f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today_str}&order=event_date.asc,event_time.asc&limit=25", headers=supabase_headers())
            if c_res.status_code == 200 and c_res.json():
                evs = c_res.json()
                calendar_text = "\n".join([f"- {ev.get('event_date')} {ev.get('event_time', '')}: {ev.get('title')} [{ev.get('category')}] [ID: {ev.get('id')}]" for ev in evs])
    except Exception as e:
        logger.warning("Nie udało się pobrać pełnego kontekstu dla Gemini: %s", e)

    system_prompt = f"""Jesteś osobistym, inteligentnym asystentem domowym Nexus Home Hub dla Bartosza i jego żony Darii.
Dzisiejsza data: {today_str}. Aktualna godzina: {now_time}.
Lokalizacja: Gniezno, Polska.

AKTUALNY STAN TWOJEGO DOMU:
[Lista zakupów - aktualnie potrzebne]:
{shopping_text}

[Kalendarz i harmonogram - nadchodzące dni]:
{calendar_text}

ZASADY DZIAŁANIA:
1. Rozpoznawaj intencje użytkownika (zakupy, kalendarz, wywóz śmieci, skreślanie kupionych rzeczy, pytania kulinarne, domowe).
2. Jeśli użytkownik dodaje zakupy, użyj 'add_shopping' z nazwami i ilościami (np. 2 mleka, 5 jajek).
3. Jeśli użytkownik mówi, że coś kupił (np. 'kupiłem mleko', 'skreśl chleb'), użyj 'complete_shopping' z nazwami kupionych rzeczy.
4. Jeśli użytkownik dodaje wydarzenie lub termin (np. dentysta, wizyta, urodziny, wywóz plastiku/bio), użyj 'add_calendar'.
   - Kategoria musi być jedną z: 'Bartosz', 'Daria', 'Wspólne', 'Śmieci'.
   - Jeśli to śmieci/odpady -> kategoria 'Śmieci'.
   - Jeśli z kontekstu to sprawa żony -> 'Daria'. Jeśli męża -> 'Bartosz'. W przeciwnym razie -> 'Wspólne'.
5. W polu 'reply' napisz naturalną, życzliwą, zwięzłą odpowiedź po polsku dla domownika (używaj emoji).
"""

    generation_config = {
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

    models_to_try = [GEMINI_MODEL, "gemini-3.1-flash-lite"]
    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": parts}],
        "generation_config": generation_config
    }

    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            resp = None
            for m in models_to_try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={GEMINI_API_KEY}"
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    break
                logger.warning("Model %s zwrócił status %s, próbuję alternatywnego...", m, resp.status_code)

            if not resp or resp.status_code != 200:
                logger.error("Błąd odpowiedzi Gemini (%s): %s", resp.status_code if resp else "None", resp.text if resp else "")
                return "", False

            data = resp.json()
            raw_json = data["candidates"][0]["content"]["parts"][0]["text"]
            result = json.loads(raw_json)

            actions = result.get("actions", [])
            reply_text = result.get("reply", "Zrobione!")

            refresh_shopping = False
            refresh_calendar = False

            for act in actions:
                action_type = act.get("action")

                if action_type == "add_shopping" and act.get("shopping_items"):
                    for s_item in act["shopping_items"]:
                        it_name = s_item.get("item", "").strip()
                        it_qty = s_item.get("quantity", "1").strip() or "1"
                        if it_name:
                            await client.post(
                                f"{SUPABASE_URL}/rest/v1/home_shopping",
                                headers=supabase_headers(),
                                json={"item": it_name, "quantity": it_qty, "category": "Wszystko", "added_by": f"AI: {sender}"}
                            )
                            refresh_shopping = True

                elif action_type == "complete_shopping" and act.get("complete_items"):
                    for c_name in act["complete_items"]:
                        await client.patch(
                            f"{SUPABASE_URL}/rest/v1/home_shopping?item=ilike.*{c_name.strip()}*&is_bought=eq.false",
                            headers=supabase_headers(),
                            json={"is_bought": True}
                        )
                        refresh_shopping = True

                elif action_type == "add_calendar" and act.get("calendar_event"):
                    ev = act["calendar_event"]
                    await client.post(
                        f"{SUPABASE_URL}/rest/v1/home_calendar",
                        headers=supabase_headers(),
                        json={
                            "title": ev.get("title", "Wydarzenie"),
                            "event_date": ev.get("event_date", today_str),
                            "event_time": ev.get("event_time", ""),
                            "category": ev.get("category", "Wspólne"),
                            "created_by": f"AI: {sender}"
                        }
                    )
                    refresh_calendar = True

                elif action_type == "delete_calendar" and act.get("delete_calendar_query"):
                    query = act["delete_calendar_query"].strip()
                    await client.delete(
                        f"{SUPABASE_URL}/rest/v1/home_calendar?title=ilike.*{query}*",
                        headers=supabase_headers()
                    )
                    refresh_calendar = True

            if refresh_shopping:
                await ws_manager.broadcast_refresh("shopping")
            if refresh_calendar:
                await ws_manager.broadcast_refresh("calendar")

            return reply_text, True

    except Exception as exc:
        logger.error("Błąd wykonania Agenta Gemini: %s", exc)
        return "", False


# --- TELEGRAM WEBHOOK ---
@app.post("/api/webhook/telegram")
async def telegram_webhook(update: dict):
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

    # Rozpoznanie: Daria / Bartosz
    if user_id_str == WIFE_TELEGRAM_ID or chat_id_str == WIFE_TELEGRAM_ID:
        sender = "Daria"
    else:
        sender = "Bartosz"

    # Automatyczna rejestracja subskrybenta powiadomień w Supabase
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

    # 1. Obsługa zdjęć (Multimodal - lodówka, paragon, harmonogram śmieci z kartki)
    if photo:
        file_id = photo[-1]["file_id"]
        content, _ = await download_telegram_file(file_id)
        if content:
            b64_img = base64.b64encode(content).decode("utf-8")
            media_parts = [{"inline_data": {"mime_type": "image/jpeg", "data": b64_img}}]
            prompt = caption or "Przeanalizuj to zdjęcie. Jeśli to lista zakupów lub paragon/lodówka - dodaj brakujące produkty. Jeśli to harmonogram śmieci - dodaj terminy do kalendarza."
            reply, success = await run_gemini_agent(prompt, sender, media_parts)
            if success and reply:
                await reply_telegram(chat_id, reply)
                return {"ok": True}

    # 2. Obsługa notatek głosowych (Voice notes)
    if voice:
        file_id = voice["file_id"]
        content, _ = await download_telegram_file(file_id)
        if content:
            b64_audio = base64.b64encode(content).decode("utf-8")
            media_parts = [{"inline_data": {"mime_type": "audio/ogg", "data": b64_audio}}]
            reply, success = await run_gemini_agent("Wiadomość głosowa. Wykonaj dyspozycje domownika.", sender, media_parts)
            if success and reply:
                await reply_telegram(chat_id, reply)
                return {"ok": True}

    # 3. Komendy systemowe / pomoc
    if text.lower() in ("/start", "/help", "/pomoc"):
        help_msg = (
            f"👋 Cześć <b>{sender}</b>! Jestem <b>Agentem AI Nexus Home Hub (Gemini 3.5)</b> 🧠🏠\n\n"
            "Możesz ze mną rozmawiać zupełnie swobodnie:\n\n"
            "🛒 <b>Zakupy:</b>\n"
            "• <i>'Kup 2 mleka, 5 bułek i sok'</i>\n"
            "• <i>'Dopisz składniki na pizzę dla 4 osób'</i>\n"
            "• <i>'Kupiłem masło i chleb, skreśl je'</i>\n\n"
            "📅 <b>Kalendarz & Śmieci:</b>\n"
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

    # 4. Szybkie skróty podglądu (komendy natywne)
    if text.lower() in ("/lista", "lista", "/zakupy", "zakupy"):
        url = f"{SUPABASE_URL}/rest/v1/home_shopping?is_bought=eq.false&order=id.asc"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=supabase_headers())
            items = resp.json() if resp.status_code == 200 else []

        if not items:
            await reply_telegram(chat_id, "🎉 <b>Lista zakupów jest pusta!</b> Wszystko kupione.")
        else:
            lines = ["🛒 <b>Aktualna lista zakupów:</b>"]
            for idx, item in enumerate(items, 1):
                qty_str = f" <i>({item.get('quantity')})</i>" if item.get("quantity") and item.get("quantity") != "1" else ""
                lines.append(f"{idx}. {item.get('item')}{qty_str}")
            await reply_telegram(chat_id, "\n".join(lines))
        return {"ok": True}

    if text.lower() in ("/kalendarz", "kalendarz", "/terminy", "terminy", "/wydarzenia", "wydarzenia"):
        today_str = str(datetime.date.today())
        url = f"{SUPABASE_URL}/rest/v1/home_calendar?event_date=gte.{today_str}&order=event_date.asc,event_time.asc&limit=15"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=supabase_headers())
            events = resp.json() if resp.status_code == 200 else []

        if not events:
            await reply_telegram(chat_id, "📅 <b>Kalendarz jest pusty!</b> Brak nadchodzących wydarzeń.")
        else:
            cat_icons = {"Bartosz": "🧔", "Daria": "👩", "Wspólne": "🏡", "Śmieci": "🗑️"}
            lines = ["📅 <b>Nadchodzące wydarzenia i terminy:</b>"]
            for ev in events:
                icon = cat_icons.get(ev.get("category", "Wspólne"), "📌")
                time_str = f" ({ev.get('event_time')})" if ev.get("event_time") else ""
                lines.append(f"• {icon} <b>{ev.get('event_date')}{time_str}</b>: {ev.get('title')} <i>[{ev.get('category')}]</i>")
            await reply_telegram(chat_id, "\n".join(lines))
        return {"ok": True}

    # 5. GŁÓWNY SILNIK: AGENT AI GEMINI 3.5 FLASH
    reply, success = await run_gemini_agent(text, sender)
    if success and reply:
        await reply_telegram(chat_id, reply)
        return {"ok": True}

    # 6. AWARYJNY FALLBACK (gdyby Gemini nie odpowiedział)
    is_calendar_add = any(text.lower().startswith(p) for p in [
        "/kalendarz ", "kalendarz ", "/termin ", "termin ", "/smieci ", "smieci ",
        "/śmieci ", "śmieci ", "/wydarzenie ", "wydarzenie "
    ])
    if is_calendar_add:
        title, ev_date, ev_time, cat = parse_calendar_input(text, sender)
        url = f"{SUPABASE_URL}/rest/v1/home_calendar"
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, headers=supabase_headers(), json={
                "title": title,
                "event_date": ev_date,
                "event_time": ev_time,
                "category": cat,
                "created_by": f"TG: {sender}"
            })
        await ws_manager.broadcast_refresh("calendar")
        cat_icons = {"Bartosz": "🧔", "Daria": "👩", "Wspólne": "🏡", "Śmieci": "🗑️"}
        icon = cat_icons.get(cat, "📌")
        time_txt = f" o godz. {ev_time}" if ev_time else ""
        await reply_telegram(
            chat_id,
            f"📅 <b>Dodano do kalendarza ({sender}):</b>\n• {icon} <b>{title}</b>\n• Data: <b>{ev_date}{time_txt}</b>\n• Kategoria: <b>{cat}</b>"
        )
        return {"ok": True}

    parsed_items = split_and_parse_shopping(text)
    if parsed_items:
        url = f"{SUPABASE_URL}/rest/v1/home_shopping"
        added_summary = []
        async with httpx.AsyncClient(timeout=10.0) as client:
            for item_name, qty in parsed_items:
                if not item_name:
                    continue
                added_summary.append(f"{item_name} ({qty})" if qty != "1" else item_name)
                await client.post(
                    url,
                    headers=supabase_headers(),
                    json={"item": item_name, "quantity": qty, "category": "Wszystko", "added_by": f"TG: {sender}"}
                )
        if added_summary:
            await ws_manager.broadcast_refresh("shopping")
            await reply_telegram(chat_id, f"✅ Dodano do listy ({sender}):\n• <b>" + "\n• ".join(added_summary) + "</b>")

    return {"ok": True}


# --- WHATSAPP WEBHOOK (TWILIO) ---
@app.post("/api/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
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

