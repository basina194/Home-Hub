# Nexus Cloud Home Hub 🏠💼

Nowoczesna aplikacja webowa typu Mobile-First PWA łącząca zarządzanie domem i sprawami firmowymi w jednej chmurze.

## 🚀 Technologie
- **Backend:** FastAPI (Python)
- **Baza danych w chmurze:** Supabase (REST API)
- **Komunikacja w czasie rzeczywistym:** WebSockets (natychmiastowa synchronizacja bez przeładowania)
- **Frontend:** Mobile-First PWA (HTML5, Tailwind CSS, Service Workers)
- **Integracje:** Open-Meteo (pogoda), Home Assistant (proxy), Telegram Bot (webhook `/kup`)

## 📋 Moduły
- **🏠 DOM:**
  - Warunki atmosferyczne z Open-Meteo (temperatura, wiatr, opady)
  - Wspólna lista zakupów w Supabase z oznaczaniem kupionych i czyszczeniem
  - Podgląd stanów urządzeń Home Assistant
- **💼 FIRMA:**
  - Baza zadań biznesowych i wznowień polis
  - Statystyki spraw (Wszystkie, Do zrobienia, Pilne)
  - Priorytetyzacja zadań (z powiadomieniami Telegram dla statusu Krytyczny)

## 🛠️ Uruchomienie lokalne
```bash
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload --port 8000
```
Domyślny PIN dostępu: `1234`.
