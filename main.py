# main.py
import os
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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"})
    except Exception as exc:
        logger.error("Błąd wysyłki Telegram: %s", exc)


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
    return FileResponse("static/index.html")


@app.get("/manifest.json")
async def serve_manifest():
    return FileResponse("static/manifest.json")


@app.get("/sw.js")
async def serve_sw():
    return FileResponse("static/sw.js", media_type="application/javascript")


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
    sender = message.get("from", {}).get("first_name", "Telegram")
    chat_id = message.get("chat", {}).get("id")

    if not text or not chat_id:
        return {"ok": True}

    logger.info("Odebrano Telegram od %s (chat %s): %s", sender, chat_id, text)

    # /start lub pomoc
    if text.lower() in ("/start", "/help", "/pomoc"):
        help_msg = (
            f"👋 Cześć <b>{sender}</b>! Jestem botem <b>Home Hub</b> 🏠\n\n"
            "🛒 <b>Dodawanie zakupów:</b>\n"
            "Napisz np.:\n"
            "• <code>/kup mleko - 2 opakowania, kawa, chleb</code>\n"
            "• albo bez ukośnika: <code>kup masło, jajka - 10 szt</code>\n\n"
            "📋 <b>Podgląd listy:</b>\n"
            "• Wyślij <code>/lista</code>, aby zobaczyć aktualne zakupy.\n\n"
            "Aplikacja webowa: https://home-hub-sewt.onrender.com/"
        )
        await reply_telegram(chat_id, help_msg)
        return {"ok": True}

    # /lista - sprawdzenie aktualnych zakupów
    if text.lower() in ("/lista", "lista", "/zakupy"):
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

    # /kup lub kup
    clean_text = text
    if clean_text.lower().startswith("/kup "):
        clean_text = clean_text[5:].strip()
    elif clean_text.lower().startswith("kup "):
        clean_text = clean_text[4:].strip()
    else:
        # Jeśli nie jest to komenda /kup, nie spamujemy użytkownika
        return {"ok": True}

    items_raw = [i.strip() for i in clean_text.split(",") if i.strip()]
    if not items_raw:
        return {"ok": True}

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
                json={"item": item_name, "quantity": qty, "category": "Wszystko", "added_by": f"TG: {sender}"}
            )

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

