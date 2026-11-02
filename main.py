# main.py
import os
import re
import datetime
import logging
from typing import List, Optional

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
LATITUDE = os.environ.get("LATITUDE", "52.53")
LONGITUDE = os.environ.get("LONGITUDE", "17.60")

app = FastAPI(title="Nexus Cloud Hub")

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


async def send_telegram_alert(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    targets = set()
    if TELEGRAM_CHAT_ID:
        targets.add(TELEGRAM_CHAT_ID)
    if WIFE_TELEGRAM_ID:
        targets.add(WIFE_TELEGRAM_ID)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=6.0) as client:
        for tid in targets:
            try:
                await client.post(url, json={"chat_id": tid, "text": message, "parse_mode": "HTML"})
            except Exception as exc:
                logger.error("Błąd wysyłki Telegram do %s: %s", tid, exc)


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
            f"🚨 <b>Pilne zadanie firmowe:</b>\nKlient: {payload.client_name}\nTyp: {payload.task_type}\nTermin: {payload.due_date}"
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


# --- TELEGRAM WEBHOOK ---
@app.post("/api/webhook/telegram")
async def telegram_webhook(update: dict):
    message = update.get("message", {})
    text = message.get("text", "").strip()
    chat_id = message.get("chat", {}).get("id")

    if not text or not chat_id:
        return {"ok": True}

    raw_sender = message.get("from", {})
    user_id_str = str(raw_sender.get("id", ""))
    chat_id_str = str(chat_id)

    # Rozpoznanie: Daria / Bartosz
    if user_id_str == WIFE_TELEGRAM_ID or chat_id_str == WIFE_TELEGRAM_ID:
        sender = "Daria"
    else:
        sender = "Bartosz"

    logger.info("Odebrano Telegram od %s (user %s, chat %s): %s", sender, user_id_str, chat_id, text)

    # /start lub pomoc
    if text.lower() in ("/start", "/help", "/pomoc"):
        help_msg = (
            f"👋 Cześć <b>{sender}</b>! Witaj w <b>Nexus Home Hub</b> 🏠\n\n"
            "🛒 <b>Zakupy:</b>\n"
            "Napisz po prostu co kupić, np.:\n"
            "• <code>2 mleka, 5 jajek, chleb</code>\n"
            "• <code>masło, woda 6 butelek</code>\n\n"
            "📅 <b>Kalendarz & Śmieci:</b>\n"
            "• <code>/kalendarz jutro wywóz plastiku</code>\n"
            "• <code>/kalendarz 15.11 dentysta 16:30 Daria</code>\n"
            "• <code>/smieci 04.11 odpady bio</code>\n\n"
            "📋 <b>Podgląd:</b>\n"
            "• <code>/lista</code> - aktualne zakupy\n"
            "• <code>/kalendarz</code> - nadchodzące wydarzenia\n\n"
            "Aplikacja: https://home-hub-sewt.onrender.com/"
        )
        await reply_telegram(chat_id, help_msg)
        return {"ok": True}

    # /lista - sprawdzenie zakupów
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

    # /kalendarz (bez parametrów) - podgląd nadchodzących wydarzeń
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

    # Dodawanie do kalendarza (/kalendarz ..., /termin ..., /smieci ...)
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

    # Obsługa dodawania zakupów (z komendą /kup, kup lub wprost, np. "2 jajka 3 mleka 5 bułek i sok")
    parsed_items = split_and_parse_shopping(text)
    if not parsed_items:
        return {"ok": True}

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
        await reply_telegram(chat_id, f"✅ Dodano do wspólnej listy ({sender}):\n• <b>" + "\n• ".join(added_summary) + "</b>")
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

