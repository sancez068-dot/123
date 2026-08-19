from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import string
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse


app = FastAPI(title="Watch Together")

ROOM_ID_RE = re.compile(r"^[A-Z0-9]{6}$")
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
ROOM_ALPHABET = string.ascii_uppercase + string.digits
MAX_NICKNAME_LENGTH = 24
MAX_MESSAGE_LENGTH = 500
MAX_CHAT_MESSAGES = 100


def now() -> float:
    return time.monotonic()


def clamp_position(value: Any) -> float:
    try:
        position = float(value)
    except (TypeError, ValueError):
        return 0.0
    if position != position or position < 0:
        return 0.0
    return min(position, 24 * 60 * 60)


def valid_video_id(value: str) -> str | None:
    value = value.strip()
    return value if VIDEO_ID_RE.fullmatch(value) else None


def extract_youtube_video_id(value: str) -> str | None:
    """Accept the common YouTube URL formats and a plain 11-character ID."""
    value = value.strip()
    direct_id = valid_video_id(value)
    if direct_id:
        return direct_id

    candidate = value
    if "://" not in candidate:
        candidate = f"https://{candidate}"

    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return None

    hostname = (parsed.hostname or "").lower().rstrip(".")
    if hostname.startswith("www."):
        hostname = hostname[4:]

    path_parts = [part for part in parsed.path.split("/") if part]
    video_id: str | None = None

    if hostname in {"youtu.be", "youtube-nocookie.com"}:
        if path_parts:
            video_id = path_parts[0]
    elif hostname in {"youtube.com", "m.youtube.com", "music.youtube.com"}:
        if path_parts and path_parts[0] == "watch":
            video_id = parse_qs(parsed.query).get("v", [None])[0]
        elif len(path_parts) >= 2 and path_parts[0] in {
            "embed",
            "shorts",
            "live",
        }:
            video_id = path_parts[1]

    return valid_video_id(video_id or "")


def make_room_id() -> str:
    return "".join(secrets.choice(ROOM_ALPHABET) for _ in range(6))


def clean_nickname(value: Any, fallback: str) -> str:
    nickname = " ".join(str(value or "").split()).strip()
    nickname = nickname[:MAX_NICKNAME_LENGTH]
    return nickname or fallback


@dataclass
class Room:
    room_id: str
    video_id: str | None = None
    playing: bool = False
    position: float = 0.0
    changed_at: float = field(default_factory=now)
    clients: dict[str, WebSocket] = field(default_factory=dict)
    nicknames: dict[str, str] = field(default_factory=dict)
    chat: list[dict[str, Any]] = field(default_factory=list)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def current_position(self) -> float:
        if not self.playing:
            return self.position
        return max(0.0, self.position + (now() - self.changed_at))

    def save_timing(
        self,
        *,
        position: Any | None = None,
        playing: bool | None = None,
    ) -> None:
        self.position = (
            clamp_position(position)
            if position is not None
            else clamp_position(self.current_position())
        )
        if playing is not None:
            self.playing = bool(playing)
        self.changed_at = now()

    def participants(self) -> list[dict[str, str]]:
        return [
            {
                "client_id": client_id,
                "nickname": self.nicknames.get(client_id, "Guest"),
            }
            for client_id in self.clients
        ]

    def state_payload(self, message_type: str = "state") -> dict[str, Any]:
        return {
            "type": message_type,
            "video_id": self.video_id,
            "playing": self.playing,
            "position": round(self.current_position(), 3),
            "participants": self.participants(),
            "chat": self.chat,
        }


rooms: dict[str, Room] = {}
rooms_lock = asyncio.Lock()


async def get_or_create_room(room_id: str | None = None) -> Room:
    async with rooms_lock:
        selected_id = (room_id or "").upper()
        if not ROOM_ID_RE.fullmatch(selected_id):
            selected_id = make_room_id()
            while selected_id in rooms:
                selected_id = make_room_id()
        room = rooms.get(selected_id)
        if room is None:
            room = Room(room_id=selected_id)
            rooms[selected_id] = room
        return room


async def send_json(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await websocket.send_json(payload)
        return True
    except Exception:
        return False


async def broadcast(
    room: Room,
    payload: dict[str, Any],
    *,
    exclude_client_id: str | None = None,
) -> None:
    disconnected: list[str] = []
    async with room.send_lock:
        for client_id, websocket in list(room.clients.items()):
            if client_id == exclude_client_id:
                continue
            if not await send_json(websocket, payload):
                disconnected.append(client_id)

    for client_id in disconnected:
        room.clients.pop(client_id, None)
        room.nicknames.pop(client_id, None)


async def broadcast_participants(room: Room) -> None:
    await broadcast(
        room,
        {
            "type": "participants",
            "participants": room.participants(),
        },
    )


@app.get("/", response_class=HTMLResponse)
async def create_room() -> RedirectResponse:
    room = await get_or_create_room()
    return RedirectResponse(url=f"/r/{room.room_id}", status_code=307)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/r/{room_id}", response_class=HTMLResponse)
async def room_page(room_id: str) -> HTMLResponse:
    room_id = room_id.upper()
    if not ROOM_ID_RE.fullmatch(room_id):
        raise HTTPException(status_code=404, detail="Room not found")
    await get_or_create_room(room_id)
    return HTMLResponse(PAGE_TEMPLATE.replace("__ROOM_ID__", room_id))


@app.websocket("/ws/{room_id}")
async def room_websocket(websocket: WebSocket, room_id: str) -> None:
    room_id = room_id.upper()
    if not ROOM_ID_RE.fullmatch(room_id):
        await websocket.close(code=1008, reason="Invalid room")
        return

    room = await get_or_create_room(room_id)
    await websocket.accept()

    client_id = secrets.token_urlsafe(9)
    fallback_nickname = f"Guest {client_id[-4:]}"
    room.clients[client_id] = websocket
    room.nicknames[client_id] = fallback_nickname

    initial_state = room.state_payload()
    initial_state["client_id"] = client_id
    await send_json(websocket, initial_state)
    await broadcast_participants(room)

    try:
        while True:
            raw_message = await websocket.receive_text()
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                await send_json(
                    websocket,
                    {"type": "error", "message": "Некорректный JSON."},
                )
                continue

            if not isinstance(message, dict):
                await send_json(
                    websocket,
                    {"type": "error", "message": "Сообщение должно быть объектом."},
                )
                continue

            message_type = str(message.get("type", "")).strip().lower()

            if message_type == "join":
                room.nicknames[client_id] = clean_nickname(
                    message.get("nickname", message.get("name")),
                    fallback_nickname,
                )
                await send_json(
                    websocket,
                    {
                        "type": "state",
                        "client_id": client_id,
                        **room.state_payload(),
                    },
                )
                await broadcast_participants(room)

            elif message_type == "set_video":
                submitted_video = str(
                    message.get("video", message.get("video_id", ""))
                )
                video_id = extract_youtube_video_id(submitted_video)
                if not video_id:
                    await send_json(
                        websocket,
                        {
                            "type": "error",
                            "message": (
                                "Не удалось распознать YouTube-ссылку или video ID."
                            ),
                        },
                    )
                    continue

                room.video_id = video_id
                room.save_timing(position=0.0, playing=False)
                await broadcast(
                    room,
                    {
                        "type": "set_video",
                        "video_id": video_id,
                        "video": video_id,
                        "position": 0.0,
                        "playing": False,
                    },
                )

            elif message_type in {"play", "pause"}:
                if not room.video_id:
                    await send_json(
                        websocket,
                        {
                            "type": "error",
                            "message": "Сначала загрузите видео.",
                        },
                    )
                    continue

                is_playing = message_type == "play"
                room.save_timing(
                    position=(
                        message["position"]
                        if "position" in message
                        else room.current_position()
                    ),
                    playing=is_playing,
                )
                await broadcast(
                    room,
                    {
                        "type": message_type,
                        "video_id": room.video_id,
                        "position": round(room.position, 3),
                        "playing": room.playing,
                    },
                )

            elif message_type == "seek":
                if not room.video_id:
                    continue
                room.save_timing(
                    position=message.get("position", room.current_position())
                )
                await broadcast(
                    room,
                    {
                        "type": "seek",
                        "video_id": room.video_id,
                        "position": round(room.position, 3),
                        "playing": room.playing,
                    },
                )

            elif message_type == "sync":
                if "position" not in message:
                    await send_json(websocket, room.state_payload("sync"))
                    continue
                if room.video_id:
                    room.save_timing(
                        position=message.get("position", room.current_position()),
                        playing=(
                            bool(message["playing"])
                            if "playing" in message
                            else room.playing
                        ),
                    )
                await broadcast(
                    room,
                    {
                        "type": "sync",
                        "video_id": room.video_id,
                        "position": round(room.current_position(), 3),
                        "playing": room.playing,
                    },
                    exclude_client_id=client_id,
                )

            elif message_type == "chat":
                text = " ".join(str(message.get("text", "")).split()).strip()
                text = text[:MAX_MESSAGE_LENGTH]
                if not text:
                    continue
                chat_message = {
                    "id": secrets.token_urlsafe(8),
                    "client_id": client_id,
                    "nickname": room.nicknames.get(client_id, fallback_nickname),
                    "text": text,
                    "created_at": int(time.time()),
                }
                room.chat.append(chat_message)
                del room.chat[:-MAX_CHAT_MESSAGES]
                await broadcast(room, {"type": "chat", "message": chat_message})

            else:
                await send_json(
                    websocket,
                    {"type": "error", "message": "Неизвестный тип сообщения."},
                )

    except WebSocketDisconnect:
        pass
    except Exception:
        # A broken socket can raise different low-level exceptions depending
        # on the ASGI server. The connection is still cleaned up below.
        pass
    finally:
        room.clients.pop(client_id, None)
        room.nicknames.pop(client_id, None)
        if room.clients:
            await broadcast_participants(room)
        else:
            async with rooms_lock:
                if rooms.get(room.room_id) is room:
                    rooms.pop(room.room_id, None)


PAGE_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#0b0c0e">
  <title>Watch Together · Room __ROOM_ID__</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b0c0e;
      --surface: #121418;
      --surface-raised: #191c21;
      --surface-soft: #22262d;
      --line: #2c3038;
      --line-strong: #3a404b;
      --text: #f0f1f3;
      --muted: #959ca8;
      --accent: #dce1e8;
      --danger: #e98f8f;
      --shadow: 0 18px 60px rgba(0, 0, 0, .38);
    }

    * { box-sizing: border-box; }
    html, body { min-height: 100%; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
      -webkit-font-smoothing: antialiased;
    }
    button, input { font: inherit; }
    button { cursor: pointer; }
    button:focus-visible, input:focus-visible {
      outline: 2px solid #aab4c3;
      outline-offset: 2px;
    }
    .app {
      min-height: 100dvh;
      display: flex;
      flex-direction: column;
    }
    .topbar {
      min-height: 64px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      padding: 12px clamp(16px, 3vw, 40px);
      border-bottom: 1px solid var(--line);
      background: #0e1013;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 11px;
      min-width: 0;
    }
    .brand-mark {
      width: 28px;
      height: 28px;
      display: grid;
      place-items: center;
      flex: 0 0 auto;
      border: 1px solid #68707c;
      border-radius: 8px;
      color: var(--text);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: -.04em;
    }
    .brand-name {
      font-size: 14px;
      font-weight: 750;
      letter-spacing: -.01em;
      white-space: nowrap;
    }
    .room-meta {
      display: flex;
      align-items: center;
      gap: 9px;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .room-code {
      padding: 6px 9px;
      border: 1px solid var(--line);
      border-radius: 6px;
      color: var(--text);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .09em;
    }
    .app-main {
      width: min(1500px, 100%);
      margin: 0 auto;
      padding: clamp(16px, 3vw, 40px);
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .video-form {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
      flex: 1;
    }
    .input {
      width: 100%;
      min-width: 0;
      height: 40px;
      padding: 0 13px;
      color: var(--text);
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 7px;
      transition: border-color .18s ease, background .18s ease;
    }
    .input::placeholder { color: #6f7681; }
    .input:hover { border-color: var(--line-strong); }
    .input:focus { border-color: #697381; background: var(--surface-raised); outline: none; }
    .button {
      height: 40px;
      padding: 0 13px;
      border: 1px solid var(--line-strong);
      border-radius: 7px;
      color: var(--text);
      background: var(--surface-soft);
      font-size: 13px;
      font-weight: 650;
      white-space: nowrap;
      transition: background .18s ease, border-color .18s ease, transform .18s ease;
    }
    .button:hover { background: #2b3038; border-color: #5b6471; }
    .button:active { transform: translateY(1px); }
    .button.subtle {
      color: var(--muted);
      background: transparent;
      border-color: var(--line);
    }
    .button.subtle:hover { color: var(--text); background: var(--surface); }
    .toolbar-actions { display: flex; gap: 8px; }
    .room-layout {
      position: relative;
      display: block;
      align-items: stretch;
      min-height: min(680px, calc(100dvh - 172px));
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #08090b;
      box-shadow: var(--shadow);
    }
    .video-stage {
      position: relative;
      min-width: 0;
      min-height: 420px;
      width: 100%;
      background: #050608;
      overflow: hidden;
      transform-origin: left center;
      transition: width .28s ease, transform .28s ease;
    }
    .room-layout.chat-open .video-stage {
      width: calc(100% - min(380px, 34vw) + 34px);
      transform: translateX(-10px);
    }
    #youtube-player,
    #youtube-player iframe {
      width: 100%;
      height: 100%;
      display: block;
    }
    .video-placeholder {
      position: absolute;
      inset: 0;
      z-index: 1;
      display: grid;
      place-items: center;
      padding: 30px;
      text-align: center;
      background: #08090b;
      pointer-events: none;
    }
    .video-placeholder.hidden { display: none; }
    .placeholder-inner { max-width: 360px; }
    .placeholder-title {
      margin: 0 0 8px;
      font-size: clamp(18px, 2.3vw, 27px);
      letter-spacing: -.03em;
    }
    .placeholder-copy {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.55;
    }
    .stage-controls {
      position: absolute;
      z-index: 3;
      right: 14px;
      bottom: 14px;
      display: flex;
      gap: 7px;
      opacity: 0;
      transform: translateY(4px);
      transition: opacity .2s ease, transform .2s ease;
    }
    .video-stage:hover .stage-controls,
    .video-stage:focus-within .stage-controls,
    .room-layout.is-fullscreen .stage-controls { opacity: 1; transform: translateY(0); }
    .stage-button {
      height: 34px;
      padding: 0 10px;
      border: 1px solid rgba(255,255,255,.2);
      border-radius: 6px;
      color: #f2f4f7;
      background: rgba(10, 12, 15, .82);
      backdrop-filter: blur(8px);
      font-size: 12px;
    }
    .stage-button:hover { background: rgba(37, 41, 47, .92); }
    .chat-panel {
      position: absolute;
      top: 0;
      right: 0;
      bottom: 0;
      width: min(380px, 34vw);
      min-width: 0;
      min-height: 0;
      display: flex;
      flex-direction: column;
      border-left: 1px solid var(--line);
      background: rgba(18, 20, 24, .96);
      box-shadow: -14px 0 40px rgba(0, 0, 0, .25);
      z-index: 5;
      transform: translateX(100%);
      pointer-events: none;
      transition: transform .28s ease;
    }
    .chat-panel.is-open {
      transform: translateX(0);
      pointer-events: auto;
    }
    .chat-header {
      min-height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 0 15px;
      border-bottom: 1px solid var(--line);
      flex: 0 0 auto;
    }
    .chat-header-actions {
      display: flex;
      align-items: center;
      gap: 9px;
    }
    .chat-close {
      height: 30px;
      padding: 0 9px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      color: var(--muted);
      background: rgba(255,255,255,.04);
      font-size: 11px;
      font-weight: 650;
    }
    .chat-close:hover {
      color: var(--text);
      background: rgba(255,255,255,.1);
    }
    .chat-title {
      margin: 0;
      font-size: 13px;
      font-weight: 750;
      letter-spacing: .01em;
    }
    .online-count {
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
    }
    .chat-messages {
      min-height: 0;
      flex: 1;
      overflow-y: auto;
      overscroll-behavior: contain;
      -webkit-overflow-scrolling: touch;
      touch-action: pan-y;
      padding: 14px 13px 18px;
      scrollbar-color: #414751 transparent;
      scrollbar-width: thin;
    }
    .chat-empty {
      padding: 20px 6px;
      color: #727a87;
      font-size: 12px;
      line-height: 1.5;
      text-align: center;
    }
    .message {
      margin: 0 0 13px;
      line-height: 1.4;
      overflow-wrap: anywhere;
    }
    .message:last-child { margin-bottom: 0; }
    .message-name {
      margin-right: 6px;
      color: #bbc2cd;
      font-size: 12px;
      font-weight: 700;
    }
    .message-text {
      color: #e2e5e9;
      font-size: 13px;
    }
    .chat-composer {
      display: flex;
      gap: 7px;
      padding: 11px;
      border-top: 1px solid var(--line);
      background: #15171b;
      flex: 0 0 auto;
    }
    .chat-composer .input { height: 38px; }
    .chat-composer .button { height: 38px; padding: 0 11px; }
    .connection {
      display: flex;
      align-items: center;
      gap: 7px;
      color: var(--muted);
      font-size: 11px;
    }
    .connection-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: #707783;
    }
    .connection.connected .connection-dot { background: #a9b7a7; }
    .connection.error .connection-dot { background: var(--danger); }
    .toast {
      position: fixed;
      left: 50%;
      bottom: 22px;
      z-index: 30;
      max-width: calc(100% - 32px);
      padding: 10px 13px;
      border: 1px solid var(--line-strong);
      border-radius: 7px;
      color: var(--text);
      background: #1b1e23;
      box-shadow: var(--shadow);
      font-size: 12px;
      opacity: 0;
      pointer-events: none;
      transform: translate(-50%, 10px);
      transition: opacity .2s ease, transform .2s ease;
    }
    .toast.visible { opacity: 1; transform: translate(-50%, 0); }
    .nickname-backdrop {
      position: fixed;
      inset: 0;
      z-index: 20;
      display: grid;
      place-items: center;
      padding: 20px;
      background: rgba(5, 6, 8, .82);
      backdrop-filter: blur(7px);
    }
    .nickname-backdrop.hidden { display: none; }
    .nickname-card {
      width: min(390px, 100%);
      padding: 25px;
      border: 1px solid var(--line-strong);
      border-radius: 10px;
      background: var(--surface);
      box-shadow: var(--shadow);
    }
    .nickname-card h1 { margin: 0 0 8px; font-size: 22px; letter-spacing: -.035em; }
    .nickname-card p { margin: 0 0 20px; color: var(--muted); font-size: 13px; line-height: 1.5; }
    .nickname-form { display: flex; gap: 8px; }
    .nickname-form .button { flex: 0 0 auto; }
    .room-layout:fullscreen {
      display: block;
      width: 100vw;
      height: 100dvh;
      min-height: 0;
      border: 0;
      border-radius: 0;
      box-shadow: none;
      background: #000;
    }
    .room-layout:fullscreen .video-stage {
      height: 100%;
      min-height: 0;
      width: 100%;
    }
    .room-layout:fullscreen.chat-open .video-stage {
      width: calc(100% - min(420px, 34vw) + 42px);
    }
    .room-layout:fullscreen .chat-panel {
      top: 0;
      right: 0;
      bottom: 0;
      width: min(420px, 34vw);
      border-left: 1px solid rgba(255,255,255,.14);
      box-shadow: -12px 0 40px rgba(0,0,0,.3);
    }
    @media (max-width: 940px) {
      .room-layout.chat-open .video-stage {
        width: calc(100% - min(340px, 36vw) + 28px);
      }
    }
    @media (max-width: 720px) {
      .topbar { min-height: 56px; padding: 10px 14px; }
      .room-meta { gap: 6px; }
      .room-meta span:first-child { display: none; }
      .app-main { padding: 12px 10px 18px; gap: 11px; }
      .toolbar { align-items: stretch; flex-direction: column; }
      .toolbar-actions { justify-content: space-between; }
      .toolbar-actions .button { flex: 1; }
      .room-layout {
        display: block;
        min-height: 0;
        overflow: hidden;
        border-radius: 8px;
      }
      .video-stage {
        height: min(66vw, 58dvh);
        min-height: 230px;
        aspect-ratio: 16 / 9;
        width: 100% !important;
        transform: none !important;
      }
      .chat-panel {
        right: 0;
        bottom: 0;
        left: 0;
        top: auto;
        width: auto;
        height: min(43%, 360px);
        min-height: 190px;
        border-top: 1px solid rgba(255,255,255,.18);
        border-left: 0;
        background: rgba(18, 20, 24, .95);
        box-shadow: 0 -12px 35px rgba(0,0,0,.28);
        transform: translateY(100%);
        transition: transform .22s ease;
      }
      .chat-panel.is-open { transform: translateY(0); }
      .chat-header { min-height: 48px; padding: 0 12px; }
      .stage-controls { opacity: 1; transform: none; right: 10px; bottom: 10px; }
      .room-layout:fullscreen .chat-panel {
        top: auto;
        right: 0;
        bottom: 0;
        left: 0;
        width: auto;
        height: min(43%, 360px);
        border-top: 1px solid rgba(255,255,255,.18);
        border-left: 0;
        box-shadow: 0 -12px 35px rgba(0,0,0,.28);
      }
      .room-layout:fullscreen .chat-panel.is-open { transform: translateY(0); }
      .chat-close { height: 28px; padding: 0 8px; }
      .nickname-form { flex-direction: column; }
      .nickname-form .button { width: 100%; }
    }
    @media (max-width: 420px) {
      .brand-name { font-size: 13px; }
      .room-code { font-size: 10px; }
      .video-stage { min-height: 210px; }
      .video-form .button { padding: 0 10px; }
    }
  </style>
</head>
<body>
  <div class="app">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark" aria-hidden="true">WT</div>
        <div class="brand-name">Watch Together</div>
      </div>
      <div class="room-meta">
        <span>Room</span>
        <span class="room-code" id="room-code">__ROOM_ID__</span>
        <button class="button subtle" id="copy-room" type="button">Copy link</button>
      </div>
    </header>

    <main class="app-main">
      <div class="toolbar">
        <form class="video-form" id="video-form">
          <input class="input" id="video-input" type="text"
                 placeholder="Paste a YouTube link or video ID" autocomplete="off">
          <button class="button" type="submit">Load video</button>
        </form>
        <div class="toolbar-actions">
          <button class="button subtle" id="chat-toggle" type="button"
                  aria-expanded="true">Chat</button>
          <button class="button subtle" id="fullscreen-toggle" type="button">Fullscreen</button>
        </div>
      </div>

      <section class="room-layout chat-open" id="room-layout" aria-label="Watch Together room">
        <div class="video-stage" id="video-stage">
          <div id="youtube-player"></div>
          <div class="video-placeholder" id="video-placeholder">
            <div class="placeholder-inner">
              <h1 class="placeholder-title">Drop in a video to start</h1>
              <p class="placeholder-copy">Everyone in this room will see the same video and playback position.</p>
            </div>
          </div>
          <div class="stage-controls">
            <button class="stage-button" id="stage-chat" type="button">Chat</button>
            <button class="stage-button" id="stage-fullscreen" type="button">Fullscreen</button>
          </div>
        </div>

        <aside class="chat-panel is-open" id="chat-panel" aria-label="Room chat">
          <div class="chat-header">
            <h2 class="chat-title">Chat</h2>
            <div class="chat-header-actions">
              <div class="online-count" id="online-count">0 online</div>
              <button class="chat-close" id="chat-close" type="button"
                      aria-label="Close chat">Close</button>
            </div>
          </div>
          <div class="chat-messages" id="chat-messages" aria-live="polite">
            <div class="chat-empty" id="chat-empty">No messages yet. Say hello when everyone is ready.</div>
          </div>
          <form class="chat-composer" id="chat-form">
            <input class="input" id="chat-input" type="text" maxlength="500"
                   placeholder="Write a message..." autocomplete="off">
            <button class="button" type="submit">Send</button>
          </form>
        </aside>
      </section>

      <div class="connection" id="connection">
        <span class="connection-dot"></span>
        <span id="connection-label">Enter a nickname to join</span>
      </div>
    </main>
  </div>

  <div class="nickname-backdrop" id="nickname-backdrop">
    <div class="nickname-card">
      <h1>Join the room</h1>
      <p>Choose a nickname so everyone knows who is watching.</p>
      <form class="nickname-form" id="nickname-form">
        <input class="input" id="nickname-input" type="text" maxlength="24"
               placeholder="Your nickname" autocomplete="nickname" required>
        <button class="button" type="submit">Join room</button>
      </form>
    </div>
  </div>

  <div class="toast" id="toast" role="status"></div>

  <script>
    (() => {
      "use strict";

      const roomId = "__ROOM_ID__";
      const nicknameBackdrop = document.getElementById("nickname-backdrop");
      const nicknameForm = document.getElementById("nickname-form");
      const nicknameInput = document.getElementById("nickname-input");
      const connection = document.getElementById("connection");
      const connectionLabel = document.getElementById("connection-label");
      const videoForm = document.getElementById("video-form");
      const videoInput = document.getElementById("video-input");
      const videoPlaceholder = document.getElementById("video-placeholder");
      const chatPanel = document.getElementById("chat-panel");
      const chatToggle = document.getElementById("chat-toggle");
      const stageChat = document.getElementById("stage-chat");
      const chatClose = document.getElementById("chat-close");
      const fullscreenToggle = document.getElementById("fullscreen-toggle");
      const stageFullscreen = document.getElementById("stage-fullscreen");
      const roomLayout = document.getElementById("room-layout");
      const chatMessages = document.getElementById("chat-messages");
      const chatEmpty = document.getElementById("chat-empty");
      const chatForm = document.getElementById("chat-form");
      const chatInput = document.getElementById("chat-input");
      const onlineCount = document.getElementById("online-count");
      const copyRoom = document.getElementById("copy-room");
      const toast = document.getElementById("toast");

      let nickname = "";
      let socket = null;
      let reconnectTimer = null;
      let reconnectDelay = 700;
      let player = null;
      let playerReady = false;
      let currentVideoId = null;
      let pendingPlayerState = null;
      let remotePlayback = null;
      let remoteSeekUntil = 0;
      let toastTimer = null;
      let unreadMessages = 0;

      function setConnection(status, text) {
        connection.classList.toggle("connected", status === "connected");
        connection.classList.toggle("error", status === "error");
        connectionLabel.textContent = text;
      }

      function showToast(text) {
        toast.textContent = text;
        toast.classList.add("visible");
        window.clearTimeout(toastTimer);
        toastTimer = window.setTimeout(() => toast.classList.remove("visible"), 2600);
      }

      function setChatOpen(open) {
        chatPanel.classList.toggle("is-open", open);
        roomLayout.classList.toggle("chat-open", open);
        chatToggle.setAttribute("aria-expanded", String(open));
        chatToggle.textContent = open ? "Close chat" : "Chat";
        stageChat.textContent = open ? "Close chat" : "Chat";
        if (open) {
          unreadMessages = 0;
          window.requestAnimationFrame(() => {
            chatMessages.scrollTop = chatMessages.scrollHeight;
          });
        }
      }

      function socketIsOpen() {
        return socket && socket.readyState === WebSocket.OPEN;
      }

      function send(type, payload = {}) {
        if (!socketIsOpen()) {
          showToast("Reconnecting — try again in a moment.");
          return false;
        }
        socket.send(JSON.stringify({ type, ...payload }));
        return true;
      }

      function connect() {
        if (!nickname) return;
        if (socket && (socket.readyState === WebSocket.OPEN ||
            socket.readyState === WebSocket.CONNECTING)) return;

        const protocol = window.location.protocol === "https:" ? "wss" : "ws";
        socket = new WebSocket(`${protocol}://${window.location.host}/ws/${roomId}`);
        setConnection("connecting", "Connecting…");

        socket.addEventListener("open", () => {
          reconnectDelay = 700;
          setConnection("connected", `Connected as ${nickname}`);
          send("join", { nickname });
        });

        socket.addEventListener("message", (event) => {
          let message;
          try {
            message = JSON.parse(event.data);
          } catch {
            return;
          }
          handleServerMessage(message);
        });

        socket.addEventListener("close", () => {
          setConnection("error", "Connection lost — reconnecting…");
          if (nickname && !reconnectTimer) {
            reconnectTimer = window.setTimeout(() => {
              reconnectTimer = null;
              connect();
            }, reconnectDelay);
            reconnectDelay = Math.min(reconnectDelay * 1.7, 6000);
          }
        });

        socket.addEventListener("error", () => {
          setConnection("error", "Connection problem");
        });
      }

      function updateParticipants(participants) {
        const list = Array.isArray(participants) ? participants : [];
        onlineCount.textContent = `${list.length} online`;
      }

      function clearChat() {
        chatMessages.replaceChildren();
        chatMessages.appendChild(chatEmpty);
        chatEmpty.style.display = "block";
      }

      function appendChatMessage(message, initial = false) {
        if (!message || typeof message.text !== "string") return;
        const wasAtBottom =
          chatMessages.scrollHeight - chatMessages.scrollTop - chatMessages.clientHeight < 42;
        chatEmpty.style.display = "none";

        const row = document.createElement("div");
        row.className = "message";
        const name = document.createElement("span");
        name.className = "message-name";
        name.textContent = `${message.nickname || "Guest"}:`;
        const text = document.createElement("span");
        text.className = "message-text";
        text.textContent = message.text;
        row.append(name, text);
        chatMessages.appendChild(row);

        if (initial || wasAtBottom) {
          window.requestAnimationFrame(() => {
            chatMessages.scrollTop = chatMessages.scrollHeight;
          });
        } else if (!chatPanel.classList.contains("is-open")) {
          unreadMessages += 1;
          chatToggle.textContent = `Chat (${unreadMessages})`;
          stageChat.textContent = `Chat (${unreadMessages})`;
        }
      }

      function renderChat(messages) {
        clearChat();
        const list = Array.isArray(messages) ? messages.slice(-100) : [];
        list.forEach((message) => appendChatMessage(message, true));
        window.requestAnimationFrame(() => {
          chatMessages.scrollTop = chatMessages.scrollHeight;
        });
      }

      function setVideoLabel(videoId) {
        videoPlaceholder.classList.toggle("hidden", Boolean(videoId));
      }

      function applyPlayerState(videoId, position, playing) {
        if (!videoId) {
          currentVideoId = null;
          pendingPlayerState = null;
          setVideoLabel(null);
          return;
        }

        const nextState = {
          videoId,
          position: Math.max(0, Number(position) || 0),
          playing: Boolean(playing)
        };
        setVideoLabel(videoId);

        if (!playerReady || !player) {
          pendingPlayerState = nextState;
          return;
        }

        if (currentVideoId !== videoId) {
          currentVideoId = videoId;
          remotePlayback = {
            playing: nextState.playing,
            expires: Date.now() + 2500
          };
          if (nextState.playing) {
            player.loadVideoById({
              videoId,
              startSeconds: nextState.position
            });
          } else {
            player.cueVideoById({
              videoId,
              startSeconds: nextState.position
            });
          }
          return;
        }

        const state = player.getPlayerState();
        const currentTime = Number(player.getCurrentTime()) || 0;
        if (Math.abs(currentTime - nextState.position) > 1.5) {
          remoteSeekUntil = Date.now() + 1200;
          player.seekTo(nextState.position, true);
        }
        if (nextState.playing && state !== YT.PlayerState.PLAYING) {
          remotePlayback = { playing: true, expires: Date.now() + 2500 };
          player.playVideo();
        } else if (!nextState.playing && state === YT.PlayerState.PLAYING) {
          remotePlayback = { playing: false, expires: Date.now() + 2500 };
          player.pauseVideo();
        }
      }

      function applyRemoteSeek(message) {
        if (!playerReady || !player || !message.video_id ||
            message.video_id !== currentVideoId) return;
        remoteSeekUntil = Date.now() + 1200;
        player.seekTo(Math.max(0, Number(message.position) || 0), true);
      }

      function applyRemotePlayback(message, playing) {
        applyPlayerState(
          message.video_id || currentVideoId,
          message.position,
          playing
        );
      }

      function handleServerMessage(message) {
        switch (message.type) {
          case "state":
            updateParticipants(message.participants);
            renderChat(message.chat);
            if (message.video_id) {
              applyPlayerState(message.video_id, message.position, message.playing);
            }
            break;
          case "participants":
            updateParticipants(message.participants);
            break;
          case "set_video":
            applyPlayerState(
              message.video_id || message.video,
              message.position || 0,
              Boolean(message.playing)
            );
            break;
          case "play":
            applyRemotePlayback(message, true);
            break;
          case "pause":
            applyRemotePlayback(message, false);
            break;
          case "seek":
            applyRemoteSeek(message);
            break;
          case "sync":
            if (message.video_id && message.video_id !== currentVideoId) {
              applyPlayerState(message.video_id, message.position, message.playing);
            } else if (playerReady && player && currentVideoId) {
              const localTime = Number(player.getCurrentTime()) || 0;
              const target = Number(message.position) || 0;
              if (Math.abs(localTime - target) > 1.7) {
                remoteSeekUntil = Date.now() + 1200;
                player.seekTo(target, true);
              }
              if (Boolean(message.playing) !==
                  (player.getPlayerState() === YT.PlayerState.PLAYING)) {
                applyPlayerState(currentVideoId, target, Boolean(message.playing));
              }
            }
            break;
          case "chat":
            appendChatMessage(message.message);
            break;
          case "error":
            showToast(message.message || "Something went wrong.");
            break;
          default:
            break;
        }
      }

      function currentTime() {
        return playerReady && player ? Math.max(0, Number(player.getCurrentTime()) || 0) : 0;
      }

      function setupPlayer() {
        if (player) return;
        player = new YT.Player("youtube-player", {
          width: "100%",
          height: "100%",
          playerVars: {
            autoplay: 0,
            controls: 1,
            enablejsapi: 1,
            fs: 1,
            modestbranding: 1,
            playsinline: 1,
            rel: 0
          },
          events: {
            onReady: () => {
              playerReady = true;
              if (pendingPlayerState) {
                const next = pendingPlayerState;
                pendingPlayerState = null;
                applyPlayerState(next.videoId, next.position, next.playing);
              }
            },
            onStateChange: (event) => {
              const state = event.data;
              if (state === YT.PlayerState.PLAYING) {
                if (remotePlayback && remotePlayback.expires >= Date.now() &&
                    remotePlayback.playing) {
                  remotePlayback = null;
                } else {
                  send("play", { position: currentTime() });
                }
              } else if (state === YT.PlayerState.PAUSED) {
                if (remotePlayback && remotePlayback.expires >= Date.now() &&
                    !remotePlayback.playing) {
                  remotePlayback = null;
                } else if (Date.now() >= remoteSeekUntil) {
                  send("pause", { position: currentTime() });
                }
              } else if (state === YT.PlayerState.ENDED) {
                send("pause", { position: Number(player.getDuration()) || currentTime() });
              }
            },
            onError: () => showToast("YouTube could not load this video.")
          }
        });
      }

      window.onYouTubeIframeAPIReady = setupPlayer;

      window.setInterval(() => {
        if (socketIsOpen() && playerReady && player && currentVideoId) {
          const state = player.getPlayerState();
          send("sync", {
            position: currentTime(),
            playing: state === YT.PlayerState.PLAYING
          });
        }
      }, 2200);

      nicknameForm.addEventListener("submit", (event) => {
        event.preventDefault();
        nickname = nicknameInput.value.trim().slice(0, 24);
        if (!nickname) return;
        nicknameBackdrop.classList.add("hidden");
        setConnection("connecting", "Connecting…");
        connect();
      });

      videoForm.addEventListener("submit", (event) => {
        event.preventDefault();
        const value = videoInput.value.trim();
        if (value) {
          send("set_video", { video: value });
          videoInput.select();
        }
      });

      chatForm.addEventListener("submit", (event) => {
        event.preventDefault();
        const text = chatInput.value.trim();
        if (!text) return;
        if (send("chat", { text: text.slice(0, 500) })) chatInput.value = "";
      });

      function toggleChat() {
        setChatOpen(!chatPanel.classList.contains("is-open"));
      }
      chatToggle.addEventListener("click", toggleChat);
      stageChat.addEventListener("click", toggleChat);
      chatClose.addEventListener("click", () => setChatOpen(false));

      async function toggleFullscreen() {
        try {
          if (document.fullscreenElement === roomLayout) {
            await document.exitFullscreen();
          } else if (roomLayout.requestFullscreen) {
            await roomLayout.requestFullscreen();
          } else {
            showToast("Fullscreen is not available in this browser.");
          }
        } catch {
          showToast("Fullscreen was blocked by the browser.");
        }
      }
      fullscreenToggle.addEventListener("click", toggleFullscreen);
      stageFullscreen.addEventListener("click", toggleFullscreen);
      document.addEventListener("fullscreenchange", () => {
        const fullscreen = document.fullscreenElement === roomLayout;
        roomLayout.classList.toggle("is-fullscreen", fullscreen);
        fullscreenToggle.textContent = fullscreen ? "Exit fullscreen" : "Fullscreen";
        stageFullscreen.textContent = fullscreen ? "Exit fullscreen" : "Fullscreen";
      });

      copyRoom.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(window.location.href);
          copyRoom.textContent = "Copied";
          window.setTimeout(() => copyRoom.textContent = "Copy link", 1600);
        } catch {
          showToast("Copy the room URL from your browser.");
        }
      });

      window.addEventListener("beforeunload", () => {
        if (socket) socket.close();
      });

      nicknameInput.focus();
    })();
  </script>
  <script src="https://www.youtube.com/iframe_api"></script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ["PORT"]) if "PORT" in os.environ else 8000
    uvicorn.run("bot:app", host="0.0.0.0", port=port)