# -*- coding: utf-8 -*-

import asyncio
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse


# ============================================================
# APP
# ============================================================

app = FastAPI(
    title="Watch Together",
    version="1.0.0",
)

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
        if not self.playing:
            return max(0.0, self.position)

        elapsed = time.monotonic() - self.updated_at

        return max(
            0.0,
            self.position + elapsed,
        )


rooms: dict[str, Room] = {}

rooms_lock = asyncio.Lock()


# ============================================================
# HELPERS
# ============================================================

ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def generate_room_id(length: int = 6) -> str:
    return "".join(
        secrets.choice(ROOM_ALPHABET)
        for _ in range(length)
    )


def generate_client_id() -> str:
    return secrets.token_urlsafe(16)


def clean_nickname(value: Any) -> str:
    nickname = str(value or "").strip()

    nickname = re.sub(
        r"\s+",
        " ",
        nickname,
    )

    if not nickname:
        nickname = "Guest"

    return nickname[:24]


def extract_youtube_id(value: Any) -> str | None:
    """
    Accepts:

    https://www.youtube.com/watch?v=XXXXXXXXXXX
    https://youtu.be/XXXXXXXXXXX
    https://www.youtube.com/embed/XXXXXXXXXXX
    https://www.youtube.com/shorts/XXXXXXXXXXX
    https://www.youtube.com/live/XXXXXXXXXXX

    Also accepts a direct 11-character YouTube ID.
    """

    value = str(value or "").strip()

    if not value:
        return None

    if re.fullmatch(
        r"[A-Za-z0-9_-]{11}",
        value,
    ):
        return value

    patterns = [
        r"[?&]v=([A-Za-z0-9_-]{11})",
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"youtube\.com/embed/([A-Za-z0-9_-]{11})",
        r"youtube\.com/shorts/([A-Za-z0-9_-]{11})",
        r"youtube\.com/live/([A-Za-z0-9_-]{11})",
    ]

    for pattern in patterns:
        match = re.search(
            pattern,
            value,
            re.IGNORECASE,
        )

        if match:
            return match.group(1)

    return None


def get_participants(room: Room) -> list[dict[str, str]]:
    return [
        {
            "id": client.client_id,
            "nickname": client.nickname,
        }
        for client in room.clients.values()
    ]


def get_full_state(room: Room) -> dict[str, Any]:
    return {
        "type": "full_state",
        "room": room.room_id,
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(
            room.current_position(),
            3,
        ),
        "participants": get_participants(room),
        "chat": room.chat[-100:],
    }


async def send_json(
    websocket: WebSocket,
    data: dict[str, Any],
) -> bool:
    try:
        await websocket.send_json(data)
        return True
    except Exception:
        return False


async def broadcast(
    room: Room,
    data: dict[str, Any],
    exclude: str | None = None,
) -> None:
    dead_clients: list[str] = []

    for client_id, client in list(room.clients.items()):
        if client_id == exclude:
            continue

        success = await send_json(
            client.websocket,
            data,
        )

        if not success:
            dead_clients.append(client_id)

    for client_id in dead_clients:
        room.clients.pop(
            client_id,
            None,
        )


async def broadcast_participants(room: Room) -> None:
    await broadcast(
        room,
        {
            "type": "participants",
            "participants": get_participants(room),
        },
    )


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
    content="width=device-width, initial-scale=1.0"
>

<title>Watch Together</title>

<style>

* {
    box-sizing: border-box;
}

html,
body {
    width: 100%;
    height: 100%;
    margin: 0;
}

body {
    background: #080808;
    color: #ffffff;
    font-family:
        Inter,
        Arial,
        sans-serif;
    overflow: hidden;
}

button,
input {
    font: inherit;
}

button {
    cursor: pointer;
}

#app {
    width: 100%;
    height: 100%;

    display: flex;
    flex-direction: column;
}


/* HEADER */

.header {
    height: 60px;
    min-height: 60px;

    display: flex;
    align-items: center;

    padding: 0 14px;

    background: #101010;

    border-bottom: 1px solid #252525;
}

.logo {
    font-size: 17px;
    font-weight: 800;
}

.logo span {
    color: #777777;
    font-weight: 500;
}

.header-right {
    margin-left: auto;

    display: flex;
    align-items: center;
    gap: 8px;
}

.room {
    color: #888888;
    font-size: 12px;
}

.room strong {
    color: #ffffff;
}

.icon-button {
    width: 40px;
    height: 40px;

    border: 1px solid #292929;
    border-radius: 9px;

    background: #191919;
    color: #ffffff;
}

.icon-button:hover {
    background: #242424;
}


/* MAIN */

.main {
    flex: 1;
    min-height: 0;

    display: flex;

    position: relative;
}

.video-section {
    flex: 1;
    min-width: 0;

    display: flex;
    flex-direction: column;
}


/* PLAYER */

.player-container {
    width: 100%;

    aspect-ratio: 16 / 9;

    background: #000000;

    position: relative;
}

#player {
    width: 100%;
    height: 100%;
}

.placeholder {
    position: absolute;

    inset: 0;

    z-index: 2;

    display: flex;
    align-items: center;
    justify-content: center;

    text-align: center;

    background: #0b0b0b;
    color: #777777;

    pointer-events: none;
}

.placeholder.hidden {
    display: none;
}


/* VIDEO INPUT */

.video-bar {
    display: flex;

    gap: 8px;

    padding: 10px;

    background: #111111;

    border-bottom: 1px solid #252525;
}

.video-input {
    flex: 1;
    min-width: 0;

    height: 42px;

    padding: 0 12px;

    border: 1px solid #292929;
    border-radius: 9px;

    background: #090909;
    color: #ffffff;

    outline: none;
}

.video-input:focus {
    border-color: #444444;
}

.load-button {
    height: 42px;

    padding: 0 18px;

    border: 0;
    border-radius: 9px;

    background: #ffffff;
    color: #000000;

    font-weight: 700;
}


/* CHAT */

.chat {
    width: 340px;
    min-width: 340px;

    display: flex;
    flex-direction: column;

    background: #111111;

    border-left: 1px solid #252525;

    transition:
        width 0.25s ease,
        min-width 0.25s ease;
}

.chat.closed {
    width: 0;
    min-width: 0;

    overflow: hidden;
}

.chat-header {
    height: 55px;
    min-height: 55px;

    display: flex;
    align-items: center;

    padding: 0 10px 0 14px;

    border-bottom: 1px solid #252525;
}

.chat-title {
    font-weight: 700;
}

.chat-online {
    margin-top: 2px;

    color: #777777;

    font-size: 11px;
}

.chat-close {
    margin-left: auto;
}

.messages {
    flex: 1;
    min-height: 0;

    overflow-y: auto;

    padding: 13px;
}

.message {
    margin-bottom: 13px;
}

.message-author {
    margin-bottom: 3px;

    color: #888888;

    font-size: 12px;
}

.message-text {
    font-size: 14px;
    line-height: 1.4;

    word-break: break-word;
}

.chat-form {
    display: flex;

    gap: 7px;

    padding: 10px;

    border-top: 1px solid #252525;
}

.chat-input {
    flex: 1;
    min-width: 0;

    height: 40px;

    padding: 0 11px;

    border: 1px solid #292929;
    border-radius: 8px;

    background: #090909;
    color: #ffffff;

    outline: none;
}

.send {
    width: 42px;

    border: 0;
    border-radius: 8px;

    background: #ffffff;
    color: #000000;

    font-weight: 800;
}


/* FOOTER */

.footer {
    height: 48px;
    min-height: 48px;

    display: flex;
    align-items: center;

    padding: 0 13px;

    background: #101010;

    border-top: 1px solid #252525;
}

.status {
    display: flex;
    align-items: center;

    gap: 7px;

    color: #888888;

    font-size: 12px;
}

.status-dot {
    width: 7px;
    height: 7px;

    border-radius: 50%;

    background: #22c55e;
}

.status-dot.offline {
    background: #ef4444;
}

.users {
    margin-left: auto;

    color: #888888;

    font-size: 12px;
}


/* MODAL */

.modal {
    position: fixed;

    inset: 0;

    z-index: 100;

    display: flex;
    align-items: center;
    justify-content: center;

    padding: 20px;

    background: rgba(0, 0, 0, 0.82);
}

.modal.hidden {
    display: none;
}

.modal-card {
    width: min(400px, 100%);

    padding: 23px;

    border: 1px solid #292929;
    border-radius: 14px;

    background: #121212;
}

.modal-title {
    margin: 0 0 7px;
}

.modal-description {
    margin: 0 0 18px;

    color: #777777;

    font-size: 14px;
}

.nickname {
    width: 100%;

    height: 44px;

    padding: 0 12px;

    border: 1px solid #292929;
    border-radius: 9px;

    background: #090909;
    color: #ffffff;

    outline: none;
}

.join {
    width: 100%;

    height: 44px;

    margin-top: 9px;

    border: 0;
    border-radius: 9px;

    background: #ffffff;
    color: #000000;

    font-weight: 700;
}


/* TOAST */

.toast {
    position: fixed;

    left: 50%;
    bottom: 65px;

    z-index: 200;

    padding: 9px 13px;

    border-radius: 8px;

    background: #ffffff;
    color: #000000;

    font-size: 13px;

    opacity: 0;

    transform:
        translate(-50%, 10px);

    pointer-events: none;

    transition: 0.2s ease;
}

.toast.show {
    opacity: 1;

    transform:
        translate(-50%, 0);
}


/* MOBILE */

@media (max-width: 800px) {

    .room {
        display: none;
    }

    .chat {
        position: absolute;

        right: 0;
        top: 0;
        bottom: 0;

        width: min(350px, 92vw);
        min-width: min(350px, 92vw);

        z-index: 50;

        box-shadow:
            -20px 0 50px rgba(0,0,0,.5);

        transition:
            transform .25s ease;
    }

    .chat.closed {
        width: min(350px, 92vw);
        min-width: min(350px, 92vw);

        transform: translateX(105%);
    }

    .video-bar {
        flex-wrap: wrap;
    }

    .video-input {
        width: 100%;
        flex-basis: 100%;
    }

    .load-button {
        flex: 1;
    }
}

</style>

</head>

<body>

<div id="app">

    <header class="header">

        <div class="logo">
            Watch<span>Together</span>
        </div>

        <div class="header-right">

            <div class="room">
                ROOM:
                <strong id="roomCode">------</strong>
            </div>

            <button
                id="copyButton"
                class="icon-button"
                type="button"
            >
                🔗
            </button>

            <button
                id="chatButton"
                class="icon-button"
                type="button"
            >
                💬
            </button>

        </div>

    </header>


    <main class="main">

        <section class="video-section">

            <div class="player-container">

                <div id="player"></div>

                <div
                    id="placeholder"
                    class="placeholder"
                >
                    <div>
                        <strong>
                            No video loaded
                        </strong>
                        <br>
                        Paste a YouTube link below
                    </div>
                </div>

            </div>


            <div class="video-bar">

                <input
                    id="videoInput"
                    class="video-input"
                    type="text"
                    placeholder="Paste YouTube URL..."
                    autocomplete="off"
                >

                <button
                    id="loadButton"
                    class="load-button"
                    type="button"
                >
                    Load video
                </button>

            </div>

        </section>


        <aside
            id="chat"
            class="chat"
        >

            <div class="chat-header">

                <div>

                    <div class="chat-title">
                        Chat
                    </div>

                    <div class="chat-online">
                        <span id="onlineCount">0</span>
                        online
                    </div>

                </div>

                <button
                    id="closeChat"
                    class="icon-button chat-close"
                    type="button"
                >
                    →
                </button>

            </div>


            <div
                id="messages"
                class="messages"
            ></div>


            <form
                id="chatForm"
                class="chat-form"
            >

                <input
                    id="chatInput"
                    class="chat-input"
                    maxlength="500"
                    placeholder="Message..."
                    autocomplete="off"
                >

                <button
                    class="send"
                    type="submit"
                >
                    ↑
                </button>

            </form>

        </aside>

    </main>


    <footer class="footer">

        <div class="status">

            <span
                id="statusDot"
                class="status-dot offline"
            ></span>

            <span id="statusText">
                Connecting...
            </span>

        </div>

        <div class="users">
            👥
            <span id="usersCount">0</span>
        </div>

    </footer>

</div>


<div
    id="nicknameModal"
    class="modal"
>

    <div class="modal-card">

        <h2 class="modal-title">
            Join room
        </h2>

        <p class="modal-description">
            Choose your nickname.
        </p>

        <input
            id="nicknameInput"
            class="nickname"
            maxlength="24"
            placeholder="Nickname"
            autocomplete="off"
        >

        <button
            id="joinButton"
            class="join"
            type="button"
        >
            Join
        </button>

    </div>

</div>


<div
    id="toast"
    class="toast"
></div>


<script
    src="https://www.youtube.com/iframe_api"
></script>


<script>

"use strict";


/* ============================================================
   STATE
   ============================================================ */

const roomId =
    location.pathname
        .split("/")
        .filter(Boolean)[1]
        ?.toUpperCase() || null;

let socket = null;

let player = null;

let playerReady = false;

let nickname = "";

let currentVideoId = "";

let reconnectTimer = null;

let suppressUntil = 0;

let lastPosition = 0;

let lastSentSeek = -1;


/* ============================================================
   DOM
   ============================================================ */

const chat =
    document.getElementById("chat");

const messages =
    document.getElementById("messages");

const chatInput =
    document.getElementById("chatInput");

const videoInput =
    document.getElementById("videoInput");

const placeholder =
    document.getElementById("placeholder");

const modal =
    document.getElementById("nicknameModal");

const nicknameInput =
    document.getElementById("nicknameInput");

const toast =
    document.getElementById("toast");


document.getElementById(
    "roomCode"
).textContent =
    roomId || "------";


/* ============================================================
   TOAST
   ============================================================ */

function showToast(text) {

    toast.textContent = text;

    toast.classList.add("show");

    setTimeout(
        function () {
            toast.classList.remove("show");
        },
        2200
    );
}


/* ============================================================
   CONNECTION
   ============================================================ */

function setConnection(connected) {

    const dot =
        document.getElementById(
            "statusDot"
        );

    const text =
        document.getElementById(
            "statusText"
        );

    dot.classList.toggle(
        "offline",
        !connected
    );

    text.textContent =
        connected
            ? "Connected"
            : "Disconnected";
}


/* ============================================================
   SEND
   ============================================================ */

function send(data) {

    if (
        !socket ||
        socket.readyState !==
            WebSocket.OPEN
    ) {
        return false;
    }

    try {

        socket.send(
            JSON.stringify(data)
        );

        return true;

    } catch (error) {

        return false;
    }
}


/* ============================================================
   PARTICIPANTS
   ============================================================ */

function updateParticipants(list) {

    const count =
        Array.isArray(list)
            ? list.length
            : 0;

    document.getElementById(
        "onlineCount"
    ).textContent = count;

    document.getElementById(
        "usersCount"
    ).textContent = count;
}


/* ============================================================
   CHAT
   ============================================================ */

function clearMessages() {
    messages.innerHTML = "";
}


function addMessage(message) {

    const wrapper =
        document.createElement("div");

    wrapper.className =
        "message";


    const author =
        document.createElement("div");

    author.className =
        "message-author";

    author.textContent =
        message.nickname || "Guest";


    const text =
        document.createElement("div");

    text.className =
        "message-text";

    text.textContent =
        message.text || "";


    wrapper.appendChild(author);

    wrapper.appendChild(text);

    messages.appendChild(wrapper);

    messages.scrollTop =
        messages.scrollHeight;
}


/* ============================================================
   YOUTUBE
   ============================================================ */

window.onYouTubeIframeAPIReady =
    function () {
        createPlayer();
    };


function createPlayer() {

    if (player) {
        return;
    }

    if (
        !window.YT ||
        !window.YT.Player
    ) {
        return;
    }

    player =
        new YT.Player(
            "player",
            {
                width: "100%",
                height: "100%",

                playerVars: {
                    autoplay: 0,
                    controls: 1,
                    rel: 0,
                    playsinline: 1,
                    modestbranding: 1
                },

                events: {
                    onReady:
                        onPlayerReady,

                    onStateChange:
                        onPlayerStateChange
                }
            }
        );
}


function onPlayerReady() {

    playerReady = true;

    send({
        type: "request_state"
    });
}


/* ============================================================
   YOUTUBE STATE
   ============================================================ */

function onPlayerStateChange(event) {

    if (!playerReady) {
        return;
    }

    if (
        Date.now() <
        suppressUntil
    ) {
        return;
    }

    if (!player) {
        return;
    }

    const position =
        player.getCurrentTime() || 0;


    if (
        event.data ===
        YT.PlayerState.PLAYING
    ) {

        send({
            type: "play",
            position: position
        });

    } else if (
        event.data ===
        YT.PlayerState.PAUSED
    ) {

        send({
            type: "pause",
            position: position
        });
    }


    lastPosition =
        position;
}


/* ============================================================
   APPLY FULL STATE
   ============================================================ */

function applyFullState(data) {

    updateParticipants(
        data.participants || []
    );


    clearMessages();

    for (
        const message
        of (data.chat || [])
    ) {
        addMessage(message);
    }


    if (!data.video_id) {

        currentVideoId = "";

        placeholder.classList.remove(
            "hidden"
        );

        return;
    }


    placeholder.classList.add(
        "hidden"
    );


    if (
        !playerReady ||
        !player
    ) {
        return;
    }


    const videoId =
        data.video_id;

    const position =
        Number(
            data.position || 0
        );


    suppressUntil =
        Date.now() + 1500;


    if (
        currentVideoId !==
        videoId
    ) {

        currentVideoId =
            videoId;


        player.loadVideoById({
            videoId: videoId,
            startSeconds: position
        });


        setTimeout(
            function () {

                if (!player) {
                    return;
                }

                suppressUntil =
                    Date.now() + 1000;

                if (data.playing) {
                    player.playVideo();
                } else {
                    player.pauseVideo();
                }
            },
            600
        );

    } else {

        const localPosition =
            player.getCurrentTime() || 0;


        if (
            Math.abs(
                localPosition - position
            ) > 1.25
        ) {

            player.seekTo(
                position,
                true
            );
        }


        if (data.playing) {
            player.playVideo();
        } else {
            player.pauseVideo();
        }
    }


    lastPosition =
        position;
}


/* ============================================================
   REMOTE PLAY
   ============================================================ */

function remotePlay(position) {

    if (
        !playerReady ||
        !player
    ) {
        return;
    }

    suppressUntil =
        Date.now() + 1000;


    const local =
        player.getCurrentTime() || 0;


    if (
        Math.abs(
            local - position
        ) > 1.25
    ) {

        player.seekTo(
            position,
            true
        );
    }


    player.playVideo();

    lastPosition =
        position;
}


/* ============================================================
   REMOTE PAUSE
   ============================================================ */

function remotePause(position) {

    if (
        !playerReady ||
        !player
    ) {
        return;
    }

    suppressUntil =
        Date.now() + 1000;


    const local =
        player.getCurrentTime() || 0;


    if (
        Math.abs(
            local - position
        ) > 1.25
    ) {

        player.seekTo(
            position,
            true
        );
    }


    player.pauseVideo();

    lastPosition =
        position;
}


/* ============================================================
   REMOTE SEEK
   ============================================================ */

function remoteSeek(position) {

    if (
        !playerReady ||
        !player
    ) {
        return;
    }

    suppressUntil =
        Date.now() + 800;


    player.seekTo(
        position,
        true
    );


    lastPosition =
        position;
}


/* ============================================================
   REMOTE VIDEO
   ============================================================ */

function remoteVideo(videoId) {

    if (
        !playerReady ||
        !player
    ) {
        return;
    }

    currentVideoId =
        videoId;

    placeholder.classList.add(
        "hidden"
    );


    suppressUntil =
        Date.now() + 1500;


    player.loadVideoById({
        videoId: videoId,
        startSeconds: 0
    });


    lastPosition = 0;
}


/* ============================================================
   SOCKET MESSAGE
   ============================================================ */

function handleMessage(data) {

    if (!data || !data.type) {
        return;
    }


    switch (data.type) {

        case "full_state":

            applyFullState(data);

            break;


        case "video":

            remoteVideo(
                data.video_id
            );

            break;


        case "play":

            remotePlay(
                Number(
                    data.position || 0
                )
            );

            break;


        case "pause":

            remotePause(
                Number(
                    data.position || 0
                )
            );

            break;


        case "seek":

            remoteSeek(
                Number(
                    data.position || 0
                )
            );

            break;


        case "chat":

            addMessage(
                data.message
            );

            break;


        case "participants":

            updateParticipants(
                data.participants || []
            );

            break;


        case "error":

            showToast(
                data.message ||
                "Server error"
            );

            break;
    }
}


/* ============================================================
   CONNECT
   ============================================================ */

function connect() {

    if (
        !roomId ||
        !nickname
    ) {
        return;
    }


    if (
        socket &&
        socket.readyState ===
            WebSocket.OPEN
    ) {
        return;
    }


    const protocol =
        location.protocol === "https:"
            ? "wss:"
            : "ws:";


    const url =
        protocol +
        "//" +
        location.host +
        "/ws/" +
        encodeURIComponent(
            roomId
        );


    socket =
        new WebSocket(url);


    socket.onopen =
        function () {

            setConnection(true);

            send({
                type: "join",
                nickname: nickname
            });
        };


    socket.onmessage =
        function (event) {

            try {

                const data =
                    JSON.parse(
                        event.data
                    );

                handleMessage(data);

            } catch (error) {

                console.error(
                    error
                );
            }
        };


    socket.onerror =
        function () {

            setConnection(false);
        };


    socket.onclose =
        function () {

            setConnection(false);

            clearTimeout(
                reconnectTimer
            );


            reconnectTimer =
                setTimeout(
                    function () {
                        connect();
                    },
                    2000
                );
        };
}


/* ============================================================
   LOAD VIDEO
   ============================================================ */

function loadVideo() {

    const value =
        videoInput.value.trim();


    if (!value) {

        showToast(
            "Paste a YouTube URL"
        );

        return;
    }


    if (
        !socket ||
        socket.readyState !==
            WebSocket.OPEN
    ) {

        showToast(
            "Not connected"
        );

        return;
    }


    send({
        type: "set_video",
        video: value
    });
}


document.getElementById(
    "loadButton"
).addEventListener(
    "click",
    loadVideo
);


videoInput.addEventListener(
    "keydown",
    function (event) {

        if (
            event.key ===
            "Enter"
        ) {

            event.preventDefault();

            loadVideo();
        }
    }
);


/* ============================================================
   CHAT
   ============================================================ */

document.getElementById(
    "chatForm"
).addEventListener(
    "submit",
    function (event) {

        event.preventDefault();


        const text =
            chatInput.value.trim();


        if (!text) {
            return;
        }


        send({
            type: "chat",
            text: text
        });


        chatInput.value = "";

        chatInput.focus();
    }
);


/* ============================================================
   CHAT BUTTONS
   ============================================================ */

document.getElementById(
    "chatButton"
).addEventListener(
    "click",
    function () {

        chat.classList.toggle(
            "closed"
        );
    }
);


document.getElementById(
    "closeChat"
).addEventListener(
    "click",
    function () {

        chat.classList.add(
            "closed"
        );
    }
);


/* ============================================================
   COPY ROOM
   ============================================================ */

document.getElementById(
    "copyButton"
).addEventListener(
    "click",
    async function () {

        try {

            await navigator.clipboard.writeText(
                location.href
            );

            showToast(
                "Room link copied"
            );

        } catch (error) {

            showToast(
                location.href
            );
        }
    }
);


/* ============================================================
   SEEK DETECTION
   ============================================================ */

setInterval(
    function () {

        if (
            !playerReady ||
            !player
        ) {
            return;
        }


        if (
            !socket ||
            socket.readyState !==
                WebSocket.OPEN
        ) {
            return;
        }


        if (
            Date.now() <
            suppressUntil
        ) {
            return;
        }


        let position;

        let state;


        try {

            position =
                player.getCurrentTime() || 0;

            state =
                player.getPlayerState();

        } catch (error) {

            return;
        }


        const difference =
            Math.abs(
                position -
                lastPosition
            );


        if (
            difference > 1.5 &&
            (
                state ===
                    YT.PlayerState.PLAYING ||
                state ===
                    YT.PlayerState.PAUSED
            )
        ) {

            if (
                Math.abs(
                    position -
                    lastSentSeek
                ) > 1
            ) {

                send({
                    type: "seek",
                    position: position
                });

                lastSentSeek =
                    position;
            }
        }


        lastPosition =
            position;

    },
    500
);


/* ============================================================
   JOIN
   ============================================================ */

function joinRoom() {

    let value =
        nicknameInput.value.trim();


    if (!value) {
        value = "Guest";
    }


    nickname =
        value.slice(
            0,
            24
        );


    localStorage.setItem(
        "watch_together_nickname",
        nickname
    );


    modal.classList.add(
        "hidden"
    );


    connect();
}


document.getElementById(
    "joinButton"
).addEventListener(
    "click",
    joinRoom
);


nicknameInput.addEventListener(
    "keydown",
    function (event) {

        if (
            event.key ===
            "Enter"
        ) {

            event.preventDefault();

            joinRoom();
        }
    }
);


/* ============================================================
   START
   ============================================================ */

function start() {

    if (!roomId) {

        location.href =
            "/";

        return;
    }


    const saved =
        localStorage.getItem(
            "watch_together_nickname"
        );


    if (saved) {
        nicknameInput.value =
            saved;
    }


    modal.classList.remove(
        "hidden"
    );


    if (
        window.YT &&
        window.YT.Player
    ) {

        createPlayer();
    }
}


start();

</script>

</body>

</html>
"""


# ============================================================
# HTTP ROUTES
# ============================================================

@app.get("/")
async def root() -> RedirectResponse:
    room_id = generate_room_id()

    async with rooms_lock:
        rooms[room_id] = Room(
            room_id=room_id
        )

    return RedirectResponse(
        url=f"/r/{room_id}",
        status_code=302,
    )


@app.get(
    "/r/{room_id}",
    response_class=HTMLResponse,
)
async def room_page(room_id: str) -> HTMLResponse:

    room_id = room_id.upper()

    if not re.fullmatch(
        r"[A-Z0-9]{4,20}",
        room_id,
    ):
        return HTMLResponse(
            "<h1>Invalid room ID</h1>",
            status_code=400,
        )


    async with rooms_lock:

        if room_id not in rooms:
            rooms[room_id] = Room(
                room_id=room_id
            )


    return HTMLResponse(HTML)


# ============================================================
# WEBSOCKET
# ============================================================

@app.websocket(
    "/ws/{room_id}"
)
async def websocket_endpoint(
    websocket: WebSocket,
    room_id: str,
) -> None:

    await websocket.accept()

    room_id = room_id.upper()


    async with rooms_lock:

        room = rooms.get(room_id)

        if room is None:

            room = Room(
                room_id=room_id
            )

            rooms[room_id] = room


    client_id = generate_client_id()

    client: Client | None = None


    try:

        # ----------------------------------------------------
        # JOIN MESSAGE
        # ----------------------------------------------------

        first_message = (
            await websocket.receive_json()
        )


        if (
            first_message.get("type")
            != "join"
        ):

            await send_json(
                websocket,
                {
                    "type": "error",
                    "message": "Join required",
                },
            )

            await websocket.close()

            return


        nickname = clean_nickname(
            first_message.get(
                "nickname",
                "Guest",
            )
        )


        client = Client(
            websocket=websocket,
            client_id=client_id,
            nickname=nickname,
        )


        room.clients[client_id] = client


        # ----------------------------------------------------
        # SEND CURRENT STATE
        # ----------------------------------------------------

        await send_json(
            websocket,
            get_full_state(room),
        )


        await broadcast_participants(
            room
        )


        # ----------------------------------------------------
        # MESSAGE LOOP
        # ----------------------------------------------------

        while True:

            data = (
                await websocket.receive_json()
            )


            message_type = data.get(
                "type"
            )


            # ================================================
            # REQUEST STATE
            # ================================================

            if (
                message_type ==
                "request_state"
            ):

                await send_json(
                    websocket,
                    get_full_state(room),
                )


            # ================================================
            # SET VIDEO
            # ================================================

            elif (
                message_type ==
                "set_video"
            ):

                video_id = extract_youtube_id(
                    data.get(
                        "video",
                        "",
                    )
                )


                if not video_id:

                    await send_json(
                        websocket,
                        {
                            "type": "error",
                            "message":
                                "Invalid YouTube URL",
                        },
                    )

                    continue


                room.video_id = video_id

                room.position = 0.0

                room.playing = False

                room.updated_at = (
                    time.monotonic()
                )


                await broadcast(
                    room,
                    {
                        "type": "video",
                        "video_id": video_id,
                    },
                )


            # ================================================
            # PLAY
            # ================================================

            elif (
                message_type ==
                "play"
            ):

                if not room.video_id:
                    continue


                try:

                    position = float(
                        data.get(
                            "position",
                            0,
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    position = (
                        room.current_position()
                    )


                position = max(
                    0.0,
                    position,
                )


                room.position = position

                room.playing = True

                room.updated_at = (
                    time.monotonic()
                )


                await broadcast(
                    room,
                    {
                        "type": "play",
                        "position": round(
                            position,
                            3,
                        ),
                    },
                    exclude=client_id,
                )


            # ================================================
            # PAUSE
            # ================================================

            elif (
                message_type ==
                "pause"
            ):

                if not room.video_id:
                    continue


                try:

                    position = float(
                        data.get(
                            "position",
                            0,
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    position = (
                        room.current_position()
                    )


                position = max(
                    0.0,
                    position,
                )


                room.position = position

                room.playing = False

                room.updated_at = (
                    time.monotonic()
                )


                await broadcast(
                    room,
                    {
                        "type": "pause",
                        "position": round(
                            position,
                            3,
                        ),
                    },
                    exclude=client_id,
                )


            # ================================================
            # SEEK
            # ================================================

            elif (
                message_type ==
                "seek"
            ):

                if not room.video_id:
                    continue


                try:

                    position = float(
                        data.get(
                            "position",
                            0,
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue


                position = max(
                    0.0,
                    position,
                )


                room.position = position

                room.updated_at = (
                    time.monotonic()
                )


                await broadcast(
                    room,
                    {
                        "type": "seek",
                        "position": round(
                            position,
                            3,
                        ),
                    },
                    exclude=client_id,
                )


            # ================================================
            # CHAT
            # ================================================

            elif (
                message_type ==
                "chat"
            ):

                text = str(
                    data.get(
                        "text",
                        "",
                    )
                ).strip()


                if not text:
                    continue


                text = text[:500]


                message = {
                    "id": secrets.token_hex(8),
                    "nickname": nickname,
                    "text": text,
                    "time": int(time.time()),
                }


                room.chat.append(
                    message
                )


                if len(room.chat) > 100:

                    room.chat = (
                        room.chat[-100:]
                    )


                await broadcast(
                    room,
                    {
                        "type": "chat",
                        "message": message,
                    },
                )


    except WebSocketDisconnect:

        pass


    except Exception as error:

        print(
            "WebSocket error:",
            repr(error),
        )


    finally:

        room.clients.pop(
            client_id,
            None,
        )


        try:

            await broadcast_participants(
                room
            )

        except Exception:

            pass


        if not room.clients:

            async with rooms_lock:

                current_room = rooms.get(
                    room_id
                )

                if (
                    current_room is room
                    and not room.clients
                ):

                    rooms.pop(
                        room_id,
                        None,
                    )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "rooms": len(rooms),
        "service": "watch-together",
    }