# -*- coding: utf-8 -*-
"""
WATCH TOGETHER
Python 3.12.11
FastAPI + WebSocket
All-in-one server

Запуск:
    uvicorn r:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import asyncio
import html
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware


# ============================================================
# APP
# ============================================================

app = FastAPI(title="Watch Together")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# DATA
# ============================================================

@dataclass
class Client:
    websocket: WebSocket
    client_id: str
    nickname: str


@dataclass
class Room:
    room_id: str
    video_id: str = ""
    playing: bool = False
    position: float = 0.0
    updated_at: float = field(default_factory=time.monotonic)
    clients: dict[str, Client] = field(default_factory=dict)
    chat: list[dict[str, Any]] = field(default_factory=list)

    def current_position(self) -> float:
        if self.playing:
            return max(
                0.0,
                self.position + (time.monotonic() - self.updated_at),
            )
        return max(0.0, self.position)


rooms: dict[str, Room] = {}
rooms_lock = asyncio.Lock()


# ============================================================
# HELPERS
# ============================================================

ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def create_room_id(length: int = 6) -> str:
    return "".join(secrets.choice(ROOM_ALPHABET) for _ in range(length))


def create_client_id() -> str:
    return secrets.token_urlsafe(12)


def clean_nickname(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"\s+", " ", value)

    if not value:
        value = "Guest"

    return value[:24]


def extract_youtube_id(value: str) -> str | None:
    """
    Поддерживает:
      https://www.youtube.com/watch?v=XXXXXXXXXXX
      https://youtu.be/XXXXXXXXXXX
      https://www.youtube.com/embed/XXXXXXXXXXX
      https://www.youtube.com/shorts/XXXXXXXXXXX
      обычный video ID
    """

    value = str(value or "").strip()

    if not value:
        return None

    # Уже ID
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return value

    patterns = [
        r"(?:v=)([A-Za-z0-9_-]{11})",
        r"(?:youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:youtube\.com/embed/)([A-Za-z0-9_-]{11})",
        r"(?:youtube\.com/shorts/)([A-Za-z0-9_-]{11})",
        r"(?:youtube\.com/live/)([A-Za-z0-9_-]{11})",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(1)

    return None


def room_state(room: Room) -> dict[str, Any]:
    return {
        "type": "state",
        "room": room.room_id,
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(room.current_position(), 3),
        "participants": [
            {
                "id": client.client_id,
                "nickname": client.nickname,
            }
            for client in room.clients.values()
        ],
        "chat": room.chat[-100:],
    }


async def broadcast(
    room: Room,
    message: dict[str, Any],
    exclude: str | None = None,
) -> None:
    dead: list[str] = []

    for client_id, client in list(room.clients.items()):
        if client_id == exclude:
            continue

        try:
            await client.websocket.send_json(message)
        except Exception:
            dead.append(client_id)

    for client_id in dead:
        room.clients.pop(client_id, None)


async def broadcast_state(room: Room) -> None:
    await broadcast(room, room_state(room))


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0, viewport-fit=cover"
>
<title>Watch Together</title>

<style>
* {
    box-sizing: border-box;
}

:root {
    --bg: #09090b;
    --panel: #111114;
    --panel2: #18181c;
    --border: rgba(255,255,255,.08);
    --text: #f4f4f5;
    --muted: #a1a1aa;
    --accent: #ffffff;
    --danger: #ef4444;
}

html,
body {
    width: 100%;
    height: 100%;
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family:
        Inter,
        ui-sans-serif,
        system-ui,
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        sans-serif;
    overflow: hidden;
}

button,
input {
    font: inherit;
}

button {
    border: 0;
    cursor: pointer;
}

#app {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
}

.topbar {
    height: 64px;
    min-height: 64px;
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 18px;
    background: rgba(9,9,11,.94);
    border-bottom: 1px solid var(--border);
    z-index: 20;
}

.logo {
    font-weight: 800;
    letter-spacing: -.5px;
    white-space: nowrap;
}

.logo span {
    color: var(--muted);
    font-weight: 500;
}

.room-info {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: auto;
}

.room-code {
    color: var(--muted);
    font-size: 13px;
}

.room-code strong {
    color: var(--text);
}

.icon-btn {
    width: 40px;
    height: 40px;
    border-radius: 10px;
    background: var(--panel2);
    color: var(--text);
    border: 1px solid var(--border);
}

.icon-btn:hover {
    background: #222227;
}

.main {
    min-height: 0;
    flex: 1;
    display: flex;
    position: relative;
}

.video-area {
    min-width: 0;
    flex: 1;
    display: flex;
    flex-direction: column;
}

.player-wrap {
    position: relative;
    width: 100%;
    aspect-ratio: 16 / 9;
    max-height: calc(100vh - 170px);
    background: #000;
}

#player {
    width: 100%;
    height: 100%;
}

.empty-player {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 25px;
    background:
        radial-gradient(circle at center, #17171b 0%, #08080a 70%);
    color: var(--muted);
    text-align: center;
    pointer-events: none;
}

.empty-player.hidden {
    display: none;
}

.controls {
    padding: 14px 16px;
    display: flex;
    gap: 8px;
    border-bottom: 1px solid var(--border);
    background: var(--panel);
}

.url-input {
    min-width: 0;
    flex: 1;
    height: 42px;
    border: 1px solid var(--border);
    outline: none;
    border-radius: 10px;
    padding: 0 13px;
    color: var(--text);
    background: #0d0d10;
}

.url-input:focus {
    border-color: rgba(255,255,255,.2);
}

.primary {
    height: 42px;
    padding: 0 18px;
    border-radius: 10px;
    color: #09090b;
    background: #fff;
    font-weight: 700;
}

.primary:hover {
    opacity: .9;
}

.side {
    width: 350px;
    min-width: 350px;
    height: 100%;
    display: flex;
    flex-direction: column;
    background: var(--panel);
    border-left: 1px solid var(--border);
    transition:
        transform .25s ease,
        width .25s ease,
        min-width .25s ease;
}

.side.hidden {
    width: 0;
    min-width: 0;
    overflow: hidden;
    transform: translateX(100%);
}

.side-head {
    height: 56px;
    min-height: 56px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 12px 0 16px;
    border-bottom: 1px solid var(--border);
}

.side-title {
    font-weight: 700;
}

.online {
    color: var(--muted);
    font-size: 12px;
}

.chat {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 14px;
}

.message {
    margin-bottom: 12px;
}

.message-author {
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 3px;
}

.message-text {
    word-break: break-word;
    font-size: 14px;
    line-height: 1.4;
}

.chat-form {
    display: flex;
    gap: 8px;
    padding: 12px;
    border-top: 1px solid var(--border);
}

.chat-input {
    min-width: 0;
    flex: 1;
    height: 40px;
    padding: 0 12px;
    border-radius: 9px;
    outline: none;
    border: 1px solid var(--border);
    background: #0d0d10;
    color: var(--text);
}

.send-btn {
    width: 42px;
    border-radius: 9px;
    background: #fff;
    color: #000;
    font-weight: 800;
}

.bottom {
    height: 52px;
    min-height: 52px;
    display: flex;
    align-items: center;
    padding: 0 16px;
    gap: 12px;
    background: var(--panel);
    border-top: 1px solid var(--border);
}

.status {
    display: flex;
    align-items: center;
    gap: 7px;
    color: var(--muted);
    font-size: 12px;
}

.status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #22c55e;
}

.status-dot.offline {
    background: var(--danger);
}

.participants {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--muted);
    font-size: 12px;
}

.modal {
    position: fixed;
    inset: 0;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
    background: rgba(0,0,0,.72);
    backdrop-filter: blur(8px);
}

.modal.hidden {
    display: none;
}

.modal-card {
    width: min(430px, 100%);
    padding: 24px;
    border-radius: 16px;
    background: #121216;
    border: 1px solid var(--border);
    box-shadow: 0 20px 80px rgba(0,0,0,.5);
}

.modal-title {
    margin: 0 0 7px;
    font-size: 22px;
}

.modal-subtitle {
    margin: 0 0 20px;
    color: var(--muted);
    font-size: 14px;
}

.modal-input {
    width: 100%;
    height: 44px;
    border-radius: 10px;
    border: 1px solid var(--border);
    outline: none;
    background: #0c0c0f;
    color: var(--text);
    padding: 0 13px;
}

.modal-button {
    width: 100%;
    height: 44px;
    margin-top: 10px;
    border-radius: 10px;
    background: #fff;
    color: #000;
    font-weight: 700;
}

.toast {
    position: fixed;
    left: 50%;
    bottom: 75px;
    transform: translateX(-50%) translateY(15px);
    padding: 10px 14px;
    border-radius: 9px;
    background: #fff;
    color: #000;
    font-size: 13px;
    opacity: 0;
    pointer-events: none;
    transition: .2s ease;
    z-index: 200;
}

.toast.show {
    opacity: 1;
    transform: translateX(-50%) translateY(0);
}

@media (max-width: 800px) {
    .topbar {
        padding: 0 10px;
    }

    .room-info {
        margin-left: auto;
    }

    .room-code {
        display: none;
    }

    .main {
        overflow: hidden;
    }

    .video-area {
        width: 100%;
    }

    .player-wrap {
        aspect-ratio: 16 / 9;
        max-height: none;
    }

    .side {
        position: absolute;
        right: 0;
        top: 0;
        bottom: 0;
        width: min(360px, 92vw);
        min-width: min(360px, 92vw);
        z-index: 50;
        box-shadow: -20px 0 60px rgba(0,0,0,.45);
    }

    .side.hidden {
        width: min(360px, 92vw);
        min-width: min(360px, 92vw);
        transform: translateX(105%);
    }

    .controls {
        flex-wrap: wrap;
    }

    .url-input {
        width: 100%;
        flex-basis: 100%;
    }

    .primary {
        flex: 1;
    }
}
</style>
</head>

<body>

<div id="app">

    <header class="topbar">
        <div class="logo">
            Watch<span>Together</span>
        </div>

        <div class="room-info">
            <div class="room-code">
                ROOM <strong id="roomCode">------</strong>
            </div>

            <button
                class="icon-btn"
                id="copyRoom"
                title="Copy room link"
            >🔗</button>

            <button
                class="icon-btn"
                id="chatToggle"
                title="Toggle chat"
            >💬</button>
        </div>
    </header>

    <main class="main">

        <section class="video-area">

            <div class="player-wrap">
                <div id="player"></div>

                <div id="emptyPlayer" class="empty-player">
                    <div>
                        <strong>No video loaded</strong><br>
                        Paste a YouTube link below
                    </div>
                </div>
            </div>

            <div class="controls">
                <input
                    id="videoInput"
                    class="url-input"
                    type="text"
                    placeholder="Paste YouTube URL..."
                    autocomplete="off"
                >

                <button
                    id="loadVideo"
                    class="primary"
                >
                    Load video
                </button>
            </div>

        </section>

        <aside id="side" class="side">

            <div class="side-head">
                <div>
                    <div class="side-title">Chat</div>
                    <div class="online">
                        <span id="onlineCount">0</span> online
                    </div>
                </div>

                <button
                    id="closeChat"
                    class="icon-btn"
                >→</button>
            </div>

            <div id="chat" class="chat"></div>

            <form id="chatForm" class="chat-form">
                <input
                    id="chatInput"
                    class="chat-input"
                    maxlength="500"
                    placeholder="Message..."
                    autocomplete="off"
                >

                <button
                    class="send-btn"
                    type="submit"
                >↑</button>
            </form>

        </aside>

    </main>

    <footer class="bottom">

        <div class="status">
            <span id="statusDot" class="status-dot"></span>
            <span id="statusText">Connecting...</span>
        </div>

        <div class="participants">
            👥 <span id="participantCount">0</span>
        </div>

    </footer>

</div>


<div id="nicknameModal" class="modal">
    <div class="modal-card">
        <h2 class="modal-title">Join room</h2>
        <p class="modal-subtitle">
            Choose a nickname to enter the watch room.
        </p>

        <input
            id="nicknameInput"
            class="modal-input"
            maxlength="24"
            placeholder="Your nickname"
            autocomplete="off"
        >

        <button
            id="joinButton"
            class="modal-button"
        >
            Join room
        </button>
    </div>
</div>


<div id="toast" class="toast"></div>


<script src="https://www.youtube.com/iframe_api"></script>

<script>
"use strict";

const roomId = location.pathname.startsWith("/r/")
    ? location.pathname.split("/")[2]
    : null;

let ws = null;
let player = null;
let playerReady = false;
let applyingRemoteState = false;
let currentVideoId = "";
let nickname = "";
let reconnectTimer = null;
let suppressPlayerEventsUntil = 0;

const $ = (id) => document.getElementById(id);

const side = $("side");
const chat = $("chat");
const chatInput = $("chatInput");
const videoInput = $("videoInput");
const emptyPlayer = $("emptyPlayer");

$("roomCode").textContent = roomId || "------";


function showToast(text) {
    const toast = $("toast");

    toast.textContent = text;
    toast.classList.add("show");

    setTimeout(() => {
        toast.classList.remove("show");
    }, 2200);
}


function setConnection(connected) {
    $("statusText").textContent =
        connected ? "Connected" : "Disconnected";

    $("statusDot").classList.toggle(
        "offline",
        !connected
    );
}


function appendMessage(message) {
    const wrapper = document.createElement("div");
    wrapper.className = "message";

    const author = document.createElement("div");
    author.className = "message-author";
    author.textContent = message.nickname || "Guest";

    const text = document.createElement("div");
    text.className = "message-text";
    text.textContent = message.text || "";

    wrapper.appendChild(author);
    wrapper.appendChild(text);

    chat.appendChild(wrapper);
    chat.scrollTop = chat.scrollHeight;
}


function clearChat() {
    chat.innerHTML = "";
}


function updateParticipants(participants) {
    const count = participants.length;

    $("onlineCount").textContent = count;
    $("participantCount").textContent = count;
}


function send(data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        showToast("Not connected");
        return false;
    }

    ws.send(JSON.stringify(data));
    return true;
}


function applyState(state) {
    updateParticipants(state.participants || []);

    if (state.chat) {
        clearChat();

        for (const message of state.chat) {
            appendMessage(message);
        }
    }

    const videoId = state.video_id || "";

    if (!videoId) {
        currentVideoId = "";
        emptyPlayer.classList.remove("hidden");
        return;
    }

    emptyPlayer.classList.add("hidden");

    if (!playerReady || !player) {
        return;
    }

    applyingRemoteState = true;
    suppressPlayerEventsUntil = Date.now() + 700;

    if (currentVideoId !== videoId) {
        currentVideoId = videoId;

        player.loadVideoById({
            videoId: videoId,
            startSeconds: Number(state.position || 0)
        });

        if (!state.playing) {
            setTimeout(() => {
                if (player) {
                    player.pauseVideo();
                }
            }, 300);
        }

    } else {
        const target = Number(state.position || 0);
        const local = player.getCurrentTime() || 0;

        if (Math.abs(local - target) > 1.5) {
            player.seekTo(target, true);
        }

        if (state.playing) {
            player.playVideo();
        } else {
            player.pauseVideo();
        }
    }

    setTimeout(() => {
        applyingRemoteState = false;
    }, 800);
}


function handleMessage(data) {

    if (data.type === "state") {
        applyState(data);
        return;
    }

    if (data.type === "chat") {
        appendMessage(data.message);
        return;
    }

    if (data.type === "participants") {
        updateParticipants(data.participants || []);
        return;
    }

    if (data.type === "error") {
        showToast(data.message || "Error");
        return;
    }

    if (data.type === "room_created") {
        return;
    }
}


function connect() {
    if (!roomId || !nickname) {
        return;
    }

    if (ws) {
        try {
            ws.close();
        } catch (_) {}
    }

    const protocol =
        location.protocol === "https:"
            ? "wss:"
            : "ws:";

    ws = new WebSocket(
        protocol +
        "//" +
        location.host +
        "/ws/" +
        encodeURIComponent(roomId)
    );

    ws.onopen = () => {
        setConnection(true);

        send({
            type: "join",
            nickname: nickname
        });
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleMessage(data);
        } catch (_) {}
    };

    ws.onclose = () => {
        setConnection(false);

        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
        }

        reconnectTimer = setTimeout(() => {
            connect();
        }, 2000);
    };

    ws.onerror = () => {
        setConnection(false);
    };
}


function createPlayer() {
    player = new YT.Player("player", {
        width: "100%",
        height: "100%",
        videoId: "",
        playerVars: {
            autoplay: 0,
            controls: 1,
            rel: 0,
            modestbranding: 1,
            playsinline: 1
        },

        events: {
            onReady: () => {
                playerReady = true;

                send({
                    type: "request_state"
                });
            },

            onStateChange: (event) => {

                if (
                    applyingRemoteState ||
                    Date.now() < suppressPlayerEventsUntil
                ) {
                    return;
                }

                if (!playerReady) {
                    return;
                }

                const position =
                    player.getCurrentTime() || 0;

                if (event.data === YT.PlayerState.PLAYING) {

                    send({
                        type: "play",
                        position: position
                    });

                } else if (
                    event.data === YT.PlayerState.PAUSED
                ) {

                    send({
                        type: "pause",
                        position: position
                    });
                }
            }
        }
    });
}


function loadVideo() {
    const value = videoInput.value.trim();

    if (!value) {
        showToast("Paste a YouTube link");
        return;
    }

    send({
        type: "set_video",
        video: value
    });
}


$("loadVideo").addEventListener("click", loadVideo);

videoInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        loadVideo();
    }
});


$("chatForm").addEventListener("submit", (event) => {
    event.preventDefault();

    const text = chatInput.value.trim();

    if (!text) {
        return;
    }

    send({
        type: "chat",
        text: text
    });

    chatInput.value = "";
    chatInput.focus();
});


$("chatToggle").addEventListener("click", () => {
    side.classList.toggle("hidden");
});


$("closeChat").addEventListener("click", () => {
    side.classList.add("hidden");
});


$("copyRoom").addEventListener("click", async () => {
    try {
        await navigator.clipboard.writeText(location.href);
        showToast("Room link copied");
    } catch (_) {
        showToast(location.href);
    }
});


$("nicknameInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        $("joinButton").click();
    }
});


$("joinButton").addEventListener("click", () => {
    nickname = $("nicknameInput").value.trim();

    if (!nickname) {
        nickname = "Guest";
    }

    nickname = nickname.slice(0, 24);

    localStorage.setItem(
        "watch_together_nickname",
        nickname
    );

    $("nicknameModal").classList.add("hidden");

    connect();
});


function start() {

    if (!roomId) {
        const newRoom =
            Math.random()
                .toString(36)
                .slice(2, 8)
                .toUpperCase();

        history.replaceState(
            {},
            "",
            "/r/" + newRoom
        );

        location.reload();
        return;
    }

    const saved =
        localStorage.getItem(
            "watch_together_nickname"
        );

    if (saved) {
        $("nicknameInput").value = saved;
    }

    $("nicknameModal").classList.remove("hidden");
}


window.onYouTubeIframeAPIReady = createPlayer;

start();
</script>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(HTML)


@app.get("/r/{room_id}", response_class=HTMLResponse)
async def room_page(room_id: str) -> HTMLResponse:
    room_id = room_id.upper()

    if not re.fullmatch(r"[A-Z0-9]{4,20}", room_id):
        return HTMLResponse(
            "<h1>Invalid room</h1>",
            status_code=400,
        )

    async with rooms_lock:
        if room_id not in rooms:
            rooms[room_id] = Room(room_id=room_id)

    return HTMLResponse(HTML)


# ============================================================
# WEBSOCKET
# ============================================================

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    room_id: str,
):
    await websocket.accept()

    room_id = room_id.upper()

    async with rooms_lock:
        room = rooms.get(room_id)

        if room is None:
            room = Room(room_id=room_id)
            rooms[room_id] = room

    client_id = create_client_id()
    client: Client | None = None

    try:

        first = await websocket.receive_json()

        if first.get("type") != "join":
            await websocket.send_json({
                "type": "error",
                "message": "Join required",
            })
            await websocket.close()
            return

        nickname = clean_nickname(
            first.get("nickname", "Guest")
        )

        client = Client(
            websocket=websocket,
            client_id=client_id,
            nickname=nickname,
        )

        room.clients[client_id] = client

        await websocket.send_json(
            room_state(room)
        )

        await broadcast(
            room,
            {
                "type": "participants",
                "participants": [
                    {
                        "id": c.client_id,
                        "nickname": c.nickname,
                    }
                    for c in room.clients.values()
                ],
            },
        )

        while True:

            data = await websocket.receive_json()

            message_type = data.get("type")

            # ------------------------------------------------
            # REQUEST STATE
            # ------------------------------------------------

            if message_type == "request_state":

                await websocket.send_json(
                    room_state(room)
                )

            # ------------------------------------------------
            # SET VIDEO
            # ------------------------------------------------

            elif message_type == "set_video":

                video_id = extract_youtube_id(
                    data.get("video", "")
                )

                if not video_id:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Invalid YouTube URL",
                    })
                    continue

                room.video_id = video_id
                room.position = 0.0
                room.playing = False
                room.updated_at = time.monotonic()

                await broadcast_state(room)

            # ------------------------------------------------
            # PLAY
            # ------------------------------------------------

            elif message_type == "play":

                if not room.video_id:
                    continue

                try:
                    position = max(
                        0.0,
                        float(data.get("position", 0)),
                    )
                except (TypeError, ValueError):
                    position = room.current_position()

                room.position = position
                room.playing = True
                room.updated_at = time.monotonic()

                await broadcast(
                    room,
                    {
                        "type": "state",
                        "room": room.room_id,
                        "video_id": room.video_id,
                        "playing": True,
                        "position": round(position, 3),
                        "participants": [
                            {
                                "id": c.client_id,
                                "nickname": c.nickname,
                            }
                            for c in room.clients.values()
                        ],
                        "chat": room.chat[-100:],
                    },
                    exclude=client_id,
                )

            # ------------------------------------------------
            # PAUSE
            # ------------------------------------------------

            elif message_type == "pause":

                try:
                    position = max(
                        0.0,
                        float(data.get("position", 0)),
                    )
                except (TypeError, ValueError):
                    position = room.current_position()

                room.position = position
                room.playing = False
                room.updated_at = time.monotonic()

                await broadcast(
                    room,
                    {
                        "type": "state",
                        "room": room.room_id,
                        "video_id": room.video_id,
                        "playing": False,
                        "position": round(position, 3),
                        "participants": [
                            {
                                "id": c.client_id,
                                "nickname": c.nickname,
                            }
                            for c in room.clients.values()
                        ],
                        "chat": room.chat[-100:],
                    },
                    exclude=client_id,
                )

            # ------------------------------------------------
            # CHAT
            # ------------------------------------------------

            elif message_type == "chat":

                text = str(data.get("text", "")).strip()

                if not text:
                    continue

                text = text[:500]

                chat_message = {
                    "id": secrets.token_hex(8),
                    "nickname": nickname,
                    "text": text,
                    "time": int(time.time()),
                }

                room.chat.append(chat_message)

                if len(room.chat) > 100:
                    room.chat = room.chat[-100:]

                await broadcast(
                    room,
                    {
                        "type": "chat",
                        "message": chat_message,
                    },
                )

    except WebSocketDisconnect:
        pass

    except Exception:
        pass

    finally:

        if client_id in room.clients:
            room.clients.pop(client_id, None)

        try:
            await broadcast(
                room,
                {
                    "type": "participants",
                    "participants": [
                        {
                            "id": c.client_id,
                            "nickname": c.nickname,
                        }
                        for c in room.clients.values()
                    ],
                },
            )
        except Exception:
            pass

        # Удаляем пустые комнаты.
        if not room.clients:
            async with rooms_lock:
                if room_id in rooms and not rooms[room_id].clients:
                    rooms.pop(room_id, None)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "rooms": len(rooms),
    }