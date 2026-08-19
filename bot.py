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
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# MODELS
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


def participants(room: Room) -> list[dict[str, str]]:
    return [
        {
            "id": client.client_id,
            "nickname": client.nickname,
        }
        for client in room.clients.values()
    ]


def full_state(room: Room) -> dict[str, Any]:
    return {
        "type": "full_state",
        "room": room.room_id,
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(
            room.current_position(),
            3,
        ),
        "participants": participants(room),
        "chat": room.chat[-100:],
    }


async def safe_send(
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

    dead: list[str] = []

    for client_id, client in list(
        room.clients.items()
    ):

        if client_id == exclude:
            continue

        ok = await safe_send(
            client.websocket,
            data,
        )

        if not ok:
            dead.append(client_id)

    for client_id in dead:
        room.clients.pop(
            client_id,
            None,
        )


async def broadcast_participants(
    room: Room,
) -> None:

    await broadcast(
        room,
        {
            "type": "participants",
            "participants": participants(room),
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
    content="width=device-width, initial-scale=1.0, viewport-fit=cover"
>

<meta
    name="theme-color"
    content="#080808"
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
    padding: 0;

    background: #080808;
    color: white;

    font-family:
        Inter,
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;
}

body {
    overflow: hidden;
}

button,
input {
    font: inherit;
}

button {
    cursor: pointer;
}


/* ============================================================
   APP
   ============================================================ */

#app {
    width: 100%;
    height: 100dvh;

    display: flex;
    flex-direction: column;

    background: #080808;
}


/* ============================================================
   HEADER
   ============================================================ */

.header {
    height: 58px;
    min-height: 58px;

    display: flex;
    align-items: center;

    padding: 0 14px;

    background: #101010;

    border-bottom: 1px solid #242424;

    z-index: 20;
}

.logo {
    font-size: 17px;
    font-weight: 800;
}

.logo span {
    color: #777;
    font-weight: 500;
}

.header-right {
    margin-left: auto;

    display: flex;
    align-items: center;

    gap: 7px;
}

.room {
    color: #777;

    font-size: 11px;
}

.room strong {
    color: #fff;
}

.icon-button {
    width: 39px;
    height: 39px;

    display: flex;
    align-items: center;
    justify-content: center;

    padding: 0;

    border: 1px solid #292929;
    border-radius: 9px;

    background: #191919;
    color: white;
}

.icon-button:hover {
    background: #242424;
}


/* ============================================================
   MAIN
   ============================================================ */

.main {
    position: relative;

    flex: 1;
    min-height: 0;

    display: flex;

    background: #000;
}

.video-section {
    position: relative;

    flex: 1;
    min-width: 0;
    min-height: 0;

    display: flex;
    flex-direction: column;

    background: #000;
}


/* ============================================================
   PLAYER
   ============================================================ */

.player-container {
    position: relative;

    width: 100%;

    aspect-ratio: 16 / 9;

    background: #000;

    overflow: hidden;
}

#player {
    position: absolute;

    inset: 0;

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

    color: #777;

    background: #090909;

    pointer-events: none;
}

.placeholder.hidden {
    display: none;
}


/* ============================================================
   VIDEO BAR
   ============================================================ */

.video-bar {
    display: flex;

    gap: 8px;

    padding: 10px;

    background: #111;

    border-bottom: 1px solid #242424;
}

.video-input {
    flex: 1;

    min-width: 0;

    height: 42px;

    padding: 0 12px;

    border: 1px solid #292929;
    border-radius: 9px;

    outline: none;

    background: #080808;
    color: white;
}

.video-input:focus {
    border-color: #444;
}

.load-button {
    height: 42px;

    padding: 0 17px;

    border: 0;
    border-radius: 9px;

    background: white;
    color: black;

    font-weight: 700;
}


/* ============================================================
   DESKTOP CHAT
   ============================================================ */

.desktop-chat {
    width: 340px;
    min-width: 340px;

    display: flex;
    flex-direction: column;

    background: #111;

    border-left: 1px solid #242424;
}

.desktop-chat.hidden {
    display: none;
}

.chat-header {
    height: 55px;
    min-height: 55px;

    display: flex;
    align-items: center;

    padding: 0 12px;

    border-bottom: 1px solid #242424;
}

.chat-title {
    font-weight: 700;
}

.chat-online {
    margin-top: 2px;

    color: #777;

    font-size: 11px;
}

.chat-close {
    margin-left: auto;
}

.messages {
    flex: 1;
    min-height: 0;

    overflow-y: auto;

    padding: 12px;
}

.message {
    margin-bottom: 13px;

    animation: messageIn .12s ease;
}

@keyframes messageIn {

    from {
        opacity: 0;
        transform: translateY(3px);
    }

    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.message-author {
    margin-bottom: 2px;

    color: #aaa;

    font-size: 12px;
    font-weight: 600;
}

.message-text {
    color: #eee;

    font-size: 14px;
    line-height: 1.4;

    word-break: break-word;
}

.chat-form {
    display: flex;

    gap: 7px;

    padding: 10px;

    border-top: 1px solid #242424;
}

.chat-input {
    flex: 1;

    min-width: 0;

    height: 40px;

    padding: 0 11px;

    border: 1px solid #292929;
    border-radius: 8px;

    outline: none;

    background: #080808;
    color: white;
}

.send {
    width: 42px;

    border: 0;
    border-radius: 8px;

    background: white;
    color: black;

    font-weight: 800;
}


/* ============================================================
   MOBILE TWITCH CHAT
   ============================================================ */

.mobile-chat {
    position: absolute;

    left: 0;
    right: 0;
    bottom: 0;

    z-index: 15;

    display: none;

    flex-direction: column;

    pointer-events: none;
}

.mobile-chat.visible {
    display: flex;
}

.mobile-messages {
    max-height: 45vh;

    padding: 12px 12px 70px;

    overflow: hidden;

    mask-image:
        linear-gradient(
            to bottom,
            transparent 0%,
            black 15%,
            black 100%
        );

    -webkit-mask-image:
        linear-gradient(
            to bottom,
            transparent 0%,
            black 15%,
            black 100%
        );
}

.mobile-message {
    width: fit-content;
    max-width: 90%;

    margin-top: 5px;
    margin-bottom: 5px;

    color: white;

    font-size: 14px;
    line-height: 1.35;

    text-shadow:
        0 1px 2px rgba(0,0,0,.95),
        0 0 4px rgba(0,0,0,.8);

    animation: messageIn .12s ease;
}

.mobile-message-author {
    color: #ddd;

    font-weight: 700;
}

.mobile-message-text {
    color: white;
}


/* ============================================================
   MOBILE CHAT INPUT
   ============================================================ */

.mobile-chat-input {
    position: absolute;

    left: 10px;
    right: 10px;
    bottom: 10px;

    z-index: 30;

    display: none;

    gap: 7px;

    pointer-events: auto;
}

.mobile-chat-input.visible {
    display: flex;
}

.mobile-chat-input input {
    flex: 1;

    min-width: 0;

    height: 42px;

    padding: 0 12px;

    border: 1px solid rgba(255,255,255,.15);
    border-radius: 21px;

    outline: none;

    background: rgba(15,15,15,.9);
    color: white;

    backdrop-filter: blur(10px);
}

.mobile-send {
    width: 42px;
    height: 42px;

    border: 0;
    border-radius: 50%;

    background: white;
    color: black;

    font-weight: 800;
}


/* ============================================================
   MOBILE CHAT TOP BUTTON
   ============================================================ */

.mobile-chat-close {
    position: absolute;

    right: 10px;
    top: 10px;

    z-index: 30;

    width: 38px;
    height: 38px;

    display: none;

    border: 1px solid rgba(255,255,255,.12);
    border-radius: 50%;

    background: rgba(0,0,0,.6);
    color: white;

    backdrop-filter: blur(8px);
}


/* ============================================================
   FOOTER
   ============================================================ */

.footer {
    height: 46px;
    min-height: 46px;

    display: flex;
    align-items: center;

    padding: 0 13px;

    background: #101010;

    border-top: 1px solid #242424;
}

.status {
    display: flex;
    align-items: center;

    gap: 7px;

    color: #777;

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

    color: #777;

    font-size: 12px;
}


/* ============================================================
   MODAL
   ============================================================ */

.modal {
    position: fixed;

    inset: 0;

    z-index: 100;

    display: flex;
    align-items: center;
    justify-content: center;

    padding: 20px;

    background: rgba(0,0,0,.86);
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

    color: #777;

    font-size: 14px;
}

.nickname {
    width: 100%;

    height: 44px;

    padding: 0 12px;

    border: 1px solid #292929;
    border-radius: 9px;

    outline: none;

    background: #080808;
    color: white;
}

.join {
    width: 100%;

    height: 44px;

    margin-top: 9px;

    border: 0;
    border-radius: 9px;

    background: white;
    color: black;

    font-weight: 700;
}


/* ============================================================
   TOAST
   ============================================================ */

.toast {
    position: fixed;

    left: 50%;
    bottom: 65px;

    z-index: 200;

    padding: 9px 13px;

    border-radius: 8px;

    background: white;
    color: black;

    font-size: 13px;

    opacity: 0;

    pointer-events: none;

    transform:
        translate(-50%, 10px);

    transition: .2s ease;
}

.toast.show {
    opacity: 1;

    transform:
        translate(-50%, 0);
}


/* ============================================================
   MOBILE
   ============================================================ */

@media (max-width: 800px) {

    .header {
        height: 52px;
        min-height: 52px;

        padding: 0 10px;
    }

    .logo {
        font-size: 15px;
    }

    .room {
        display: none;
    }

    .main {
        display: block;
    }

    .video-section {
        width: 100%;
        height: 100%;
    }

    .player-container {
        aspect-ratio: auto;

        width: 100%;
        height: auto;

        flex: 0 0 auto;
    }

    .video-bar {
        padding: 8px;
    }

    .video-input {
        height: 40px;
    }

    .load-button {
        height: 40px;
    }

    .desktop-chat {
        display: none !important;
    }

    .mobile-chat {
        display: none;
    }

    .mobile-chat.visible {
        display: flex;
    }

    .mobile-chat-close {
        display: block;
    }

    .footer {
        display: none;
    }
}


/* ============================================================
   LANDSCAPE PHONE
   ============================================================ */

@media (
    max-width: 900px
) and (
    orientation: landscape
) {

    #app {
        height: 100dvh;
    }

    .header {
        display: none;
    }

    .video-section {
        width: 100%;
        height: 100%;
    }

    .player-container {
        width: 100%;
        height: 100%;

        aspect-ratio: auto;
    }

    .video-bar {
        position: absolute;

        left: 0;
        right: 0;
        bottom: 0;

        z-index: 12;

        opacity: 0;

        transition: opacity .2s;

        background:
            linear-gradient(
                transparent,
                rgba(0,0,0,.9)
            );

        border: 0;
    }

    .video-section:hover .video-bar,
    .video-bar:focus-within {
        opacity: 1;
    }

    .footer {
        display: none;
    }

    .mobile-messages {
        max-height: 55vh;
    }
}


/* ============================================================
   FULLSCREEN
   ============================================================ */

.player-container:fullscreen {
    width: 100vw;
    height: 100vh;

    background: black;
}

.player-container:-webkit-full-screen {
    width: 100vw;
    height: 100vh;

    background: black;
}

.player-container:fullscreen #player,
.player-container:-webkit-full-screen #player {
    width: 100%;
    height: 100%;
}

.player-container:fullscreen .mobile-chat,
.player-container:-webkit-full-screen .mobile-chat {
    display: flex;
}


/* ============================================================
   SAFE AREA
   ============================================================ */

@supports (
    padding: env(safe-area-inset-bottom)
) {

    .mobile-chat-input {
        bottom:
            calc(
                10px +
                env(safe-area-inset-bottom)
            );
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
                <strong id="roomCode">
                    ------
                </strong>
            </div>


            <button
                id="copyButton"
                class="icon-button"
                type="button"
                aria-label="Copy room"
            >
                🔗
            </button>


            <button
                id="chatButton"
                class="icon-button"
                type="button"
                aria-label="Chat"
            >
                💬
            </button>

        </div>

    </header>


    <main class="main">


        <section class="video-section">


            <div
                id="playerContainer"
                class="player-container"
            >

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


                <!-- TWITCH STYLE CHAT -->

                <div
                    id="mobileChat"
                    class="mobile-chat"
                >

                    <button
                        id="mobileChatClose"
                        class="mobile-chat-close"
                        type="button"
                    >
                        ×
                    </button>


                    <div
                        id="mobileMessages"
                        class="mobile-messages"
                    ></div>


                    <form
                        id="mobileChatForm"
                        class="mobile-chat-input"
                    >

                        <input
                            id="mobileChatInput"
                            maxlength="500"
                            placeholder="Send a message..."
                            autocomplete="off"
                        >


                        <button
                            class="mobile-send"
                            type="submit"
                        >
                            ↑
                        </button>

                    </form>

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


        <!-- DESKTOP CHAT -->

        <aside
            id="desktopChat"
            class="desktop-chat hidden"
        >

            <div class="chat-header">

                <div>

                    <div class="chat-title">
                        Chat
                    </div>

                    <div class="chat-online">

                        <span id="onlineCount">
                            0
                        </span>

                        online

                    </div>

                </div>


                <button
                    id="desktopChatClose"
                    class="icon-button chat-close"
                    type="button"
                >
                    →
                </button>

            </div>


            <div
                id="desktopMessages"
                class="messages"
            ></div>


            <form
                id="desktopChatForm"
                class="chat-form"
            >

                <input
                    id="desktopChatInput"
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

            <span id="usersCount">
                0
            </span>

        </div>

    </footer>


</div>


<!-- NICKNAME -->

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

const pathParts =
    location.pathname
        .split("/")
        .filter(Boolean);


const roomId =
    pathParts[0] === "r"
        ? pathParts[1]?.toUpperCase()
        : null;


let socket = null;

let player = null;

let playerReady = false;

let nickname = "";

let currentVideoId = "";

let reconnectTimer = null;

let suppressUntil = 0;

let lastPosition = 0;

let lastServerPosition = 0;

let lastSentSeek = -1;

let lastServerSync = 0;

let mobileChatOpen = false;


/* ============================================================
   DOM
   ============================================================ */

const desktopChat =
    document.getElementById(
        "desktopChat"
    );

const mobileChat =
    document.getElementById(
        "mobileChat"
    );

const mobileMessages =
    document.getElementById(
        "mobileMessages"
    );

const desktopMessages =
    document.getElementById(
        "desktopMessages"
    );

const placeholder =
    document.getElementById(
        "placeholder"
    );

const nicknameModal =
    document.getElementById(
        "nicknameModal"
    );

const nicknameInput =
    document.getElementById(
        "nicknameInput"
    );

const toast =
    document.getElementById(
        "toast"
    );

const videoInput =
    document.getElementById(
        "videoInput"
    );


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

    clearTimeout(
        showToast.timer
    );

    showToast.timer =
        setTimeout(
            function () {
                toast.classList.remove(
                    "show"
                );
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
    ).textContent =
        count;


    document.getElementById(
        "usersCount"
    ).textContent =
        count;
}


/* ============================================================
   CHAT
   ============================================================ */

function addDesktopMessage(message) {

    const wrapper =
        document.createElement(
            "div"
        );

    wrapper.className =
        "message";


    const author =
        document.createElement(
            "div"
        );

    author.className =
        "message-author";

    author.textContent =
        message.nickname ||
        "Guest";


    const text =
        document.createElement(
            "div"
        );

    text.className =
        "message-text";

    text.textContent =
        message.text ||
        "";


    wrapper.appendChild(
        author
    );

    wrapper.appendChild(
        text
    );


    desktopMessages.appendChild(
        wrapper
    );


    desktopMessages.scrollTop =
        desktopMessages.scrollHeight;
}


function addMobileMessage(message) {

    const wrapper =
        document.createElement(
            "div"
        );

    wrapper.className =
        "mobile-message";


    const author =
        document.createElement(
            "span"
        );

    author.className =
        "mobile-message-author";

    author.textContent =
        (
            message.nickname ||
            "Guest"
        ) + ": ";


    const text =
        document.createElement(
            "span"
        );

    text.className =
        "mobile-message-text";

    text.textContent =
        message.text ||
        "";


    wrapper.appendChild(
        author
    );

    wrapper.appendChild(
        text
    );


    mobileMessages.appendChild(
        wrapper
    );


    while (
        mobileMessages.children.length >
        30
    ) {

        mobileMessages.removeChild(
            mobileMessages.firstChild
        );
    }
}


function addMessage(message) {

    addDesktopMessage(
        message
    );

    addMobileMessage(
        message
    );
}


function clearMessages() {

    desktopMessages.innerHTML =
        "";

    mobileMessages.innerHTML =
        "";
}


/* ============================================================
   YOUTUBE API
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
                    modestbranding: 1,
                    enablejsapi: 1
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
   PLAYER STATE
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


    let position = 0;

    try {

        position =
            player.getCurrentTime() ||
            0;

    } catch (error) {

        return;
    }


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
   APPLY SERVER STATE
   ============================================================ */

function applyFullState(data) {

    updateParticipants(
        data.participants || []
    );


    clearMessages();


    for (
        const message
        of (
            data.chat ||
            []
        )
    ) {

        addMessage(
            message
        );
    }


    if (!data.video_id) {

        currentVideoId =
            "";

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


    const playing =
        Boolean(
            data.playing
        );


    currentVideoId =
        videoId;


    lastServerPosition =
        position;


    lastServerSync =
        Date.now();


    suppressUntil =
        Date.now() + 1800;


    try {

        player.loadVideoById({
            videoId: videoId,
            startSeconds: position
        });

    } catch (error) {

        return;
    }


    setTimeout(
        function () {

            if (!player) {
                return;
            }


            suppressUntil =
                Date.now() + 1500;


            try {

                player.seekTo(
                    position,
                    true
                );


                if (playing) {

                    player.playVideo();

                } else {

                    player.pauseVideo();
                }

            } catch (error) {
            }


            lastPosition =
                position;

        },
        700
    );
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
        Date.now() + 1800;


    try {

        player.loadVideoById({
            videoId: videoId,
            startSeconds: 0
        });

    } catch (error) {
    }


    lastPosition = 0;

    lastServerPosition = 0;
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
        Date.now() + 1500;


    try {

        const local =
            player.getCurrentTime() ||
            0;


        if (
            Math.abs(
                local - position
            ) > 0.8
        ) {

            player.seekTo(
                position,
                true
            );
        }


        player.playVideo();

    } catch (error) {
    }


    lastPosition =
        position;

    lastServerPosition =
        position;

    lastServerSync =
        Date.now();
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
        Date.now() + 1500;


    try {

        const local =
            player.getCurrentTime() ||
            0;


        if (
            Math.abs(
                local - position
            ) > 0.8
        ) {

            player.seekTo(
                position,
                true
            );
        }


        player.pauseVideo();

    } catch (error) {
    }


    lastPosition =
        position;

    lastServerPosition =
        position;

    lastServerSync =
        Date.now();
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
        Date.now() + 1200;


    try {

        player.seekTo(
            position,
            true
        );

    } catch (error) {
    }


    lastPosition =
        position;

    lastServerPosition =
        position;

    lastServerSync =
        Date.now();
}


/* ============================================================
   SOCKET MESSAGE
   ============================================================ */

function handleMessage(data) {

    if (
        !data ||
        !data.type
    ) {
        return;
    }


    switch (data.type) {

        case "full_state":

            applyFullState(
                data
            );

            break;


        case "video":

            remoteVideo(
                data.video_id
            );

            break;


        case "play":

            remotePlay(
                Number(
                    data.position ||
                    0
                )
            );

            break;


        case "pause":

            remotePause(
                Number(
                    data.position ||
                    0
                )
            );

            break;


        case "seek":

            remoteSeek(
                Number(
                    data.position ||
                    0
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
                data.participants ||
                []
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
        location.protocol ===
            "https:"
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


                handleMessage(
                    data
                );

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
   CHAT SEND
   ============================================================ */

function sendChat(
    input
) {

    const text =
        input.value.trim();


    if (!text) {
        return;
    }


    send({
        type: "chat",
        text: text
    });


    input.value = "";

    input.focus();
}


/* DESKTOP */

document.getElementById(
    "desktopChatForm"
).addEventListener(
    "submit",
    function (event) {

        event.preventDefault();

        sendChat(
            document.getElementById(
                "desktopChatInput"
            )
        );
    }
);


/* MOBILE */

document.getElementById(
    "mobileChatForm"
).addEventListener(
    "submit",
    function (event) {

        event.preventDefault();

        sendChat(
            document.getElementById(
                "mobileChatInput"
            )
        );
    }
);


/* ============================================================
   DESKTOP CHAT
   ============================================================ */

document.getElementById(
    "chatButton"
).addEventListener(
    "click",
    function () {

        if (
            window.innerWidth <= 800
        ) {

            toggleMobileChat();

        } else {

            desktopChat.classList.toggle(
                "hidden"
            );
        }
    }
);


document.getElementById(
    "desktopChatClose"
).addEventListener(
    "click",
    function () {

        desktopChat.classList.add(
            "hidden"
        );
    }
);


/* ============================================================
   MOBILE CHAT
   ============================================================ */

function toggleMobileChat() {

    mobileChatOpen =
        !mobileChatOpen;


    mobileChat.classList.toggle(
        "visible",
        mobileChatOpen
    );


    const input =
        document.querySelector(
            ".mobile-chat-input"
        );


    input.classList.toggle(
        "visible",
        mobileChatOpen
    );


    if (mobileChatOpen) {

        setTimeout(
            function () {

                document.getElementById(
                    "mobileChatInput"
                ).focus();

            },
            200
        );
    }
}


document.getElementById(
    "mobileChatClose"
).addEventListener(
    "click",
    function () {

        mobileChatOpen =
            false;


        mobileChat.classList.remove(
            "visible"
        );


        document.querySelector(
            ".mobile-chat-input"
        ).classList.remove(
            "visible"
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
   SEEK SYNC
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


        let position = 0;

        let state;


        try {

            position =
                player.getCurrentTime() ||
                0;

            state =
                player.getPlayerState();

        } catch (error) {

            return;
        }


        /*
         * Detect manual seek.
         *
         * During normal playback the position
         * changes by roughly 0.5 sec between
         * checks.
         *
         * A larger jump means the user
         * dragged the YouTube timeline.
         */

        const delta =
            Math.abs(
                position -
                lastPosition
            );


        if (
            delta > 1.7 &&
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
   PERIODIC RESYNC
   ============================================================ */

/*
 * Every few seconds ask the server for
 * the authoritative room state.
 *
 * This fixes small differences between
 * phones/computers caused by network latency.
 */

setInterval(
    function () {

        if (
            !socket ||
            socket.readyState !==
                WebSocket.OPEN
        ) {
            return;
        }


        if (
            !playerReady ||
            !player
        ) {
            return;
        }


        send({
            type: "sync"
        });

    },
    4000
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


    nicknameModal.classList.add(
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

        location.href = "/";

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


    nicknameModal.classList.remove(
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
# HTTP
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
async def room_page(
    room_id: str,
) -> HTMLResponse:

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


    return HTMLResponse(
        HTML
    )


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


    room_id =
        room_id.upper()


    async with rooms_lock:

        room = rooms.get(
            room_id
        )


        if room is None:

            room = Room(
                room_id=room_id
            )

            rooms[room_id] = room


    client_id =
        generate_client_id()


    try:

        # ----------------------------------------------------
        # JOIN
        # ----------------------------------------------------

        first_message =
            await websocket.receive_json()


        if (
            first_message.get("type")
            != "join"
        ):

            await safe_send(
                websocket,
                {
                    "type": "error",
                    "message":
                        "Join required",
                },
            )

            await websocket.close()

            return


        nickname =
            clean_nickname(
                first_message.get(
                    "nickname",
                    "Guest",
                )
            )


        client =
            Client(
                websocket=websocket,
                client_id=client_id,
                nickname=nickname,
            )


        room.clients[
            client_id
        ] = client


        # ----------------------------------------------------
        # CURRENT STATE
        # ----------------------------------------------------

        await safe_send(
            websocket,
            full_state(room),
        )


        await broadcast_participants(
            room
        )


        # ----------------------------------------------------
        # LOOP
        # ----------------------------------------------------

        while True:

            data =
                await websocket.receive_json()


            message_type =
                data.get("type")


            # ================================================
            # REQUEST STATE
            # ================================================

            if message_type in (
                "request_state",
                "sync",
            ):

                await safe_send(
                    websocket,
                    full_state(room),
                )


            # ================================================
            # SET VIDEO
            # ================================================

            elif (
                message_type ==
                "set_video"
            ):

                video_id =
                    extract_youtube_id(
                        data.get(
                            "video",
                            "",
                        )
                    )


                if not video_id:

                    await safe_send(
                        websocket,
                        {
                            "type": "error",
                            "message":
                                "Invalid YouTube URL",
                        },
                    )

                    continue


                room.video_id =
                    video_id

                room.position =
                    0.0

                room.playing =
                    False

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type": "video",
                        "video_id":
                            video_id,
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

                    position =
                        float(
                            data.get(
                                "position",
                                0,
                            )
                        )

                except (
                    TypeError,
                    ValueError,
                ):

                    position =
                        room.current_position()


                position =
                    max(
                        0.0,
                        position,
                    )


                room.position =
                    position

                room.playing =
                    True

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type": "play",
                        "position":
                            round(
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

                    position =
                        float(
                            data.get(
                                "position",
                                0,
                            )
                        )

                except (
                    TypeError,
                    ValueError,
                ):

                    position =
                        room.current_position()


                position =
                    max(
                        0.0,
                        position,
                    )


                room.position =
                    position

                room.playing =
                    False

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type": "pause",
                        "position":
                            round(
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

                    position =
                        float(
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


                position =
                    max(
                        0.0,
                        position,
                    )


                room.position =
                    position

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type": "seek",
                        "position":
                            round(
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

                text =
                    str(
                        data.get(
                            "text",
                            "",
                        )
                    ).strip()


                if not text:
                    continue


                text =
                    text[:500]


                message = {
                    "id":
                        secrets.token_hex(8),

                    "nickname":
                        nickname,

                    "text":
                        text,

                    "time":
                        int(time.time()),
                }


                room.chat.append(
                    message
                )


                if len(room.chat) > 100:

                    room.chat =
                        room.chat[-100:]


                await broadcast(
                    room,
                    {
                        "type": "chat",
                        "message":
                            message,
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

                current =
                    rooms.get(
                        room_id
                    )


                if (
                    current is room
                    and not room.clients
                ):

                    rooms.pop(
                        room_id,
                        None,
                    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health() -> dict[str, Any]:

    return {
        "status": "ok",
        "service": "watch-together",
        "rooms": len(rooms),
    }