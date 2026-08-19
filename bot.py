# -*- coding: utf-8 -*-

"""
WATCH TOGETHER
Python 3.12.11
FastAPI + WebSocket
One-file application

Render Start Command:

uvicorn r:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

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
# ROOM DATA
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

    # monotonic timestamp corresponding to position
    updated_at: float = field(
        default_factory=time.monotonic
    )

    clients: dict[str, Client] = field(
        default_factory=dict
    )

    chat: list[dict[str, Any]] = field(
        default_factory=list
    )

    def get_position(self) -> float:
        """
        Calculate current video position.

        If the room is playing, position advances
        with time.
        """

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

ROOM_ALPHABET = (
    "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
)


def generate_room_id(length: int = 6) -> str:
    return "".join(
        secrets.choice(ROOM_ALPHABET)
        for _ in range(length)
    )


def generate_client_id() -> str:
    return secrets.token_urlsafe(16)


def clean_nickname(value: Any) -> str:
    value = str(value or "").strip()

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    if not value:
        value = "Guest"

    return value[:24]


def extract_youtube_id(value: Any) -> str | None:
    """
    Extract YouTube video ID.

    Supported:

    https://www.youtube.com/watch?v=XXXXXXXXXXX

    https://youtu.be/XXXXXXXXXXX

    https://www.youtube.com/embed/XXXXXXXXXXX

    https://www.youtube.com/shorts/XXXXXXXXXXX

    https://www.youtube.com/live/XXXXXXXXXXX

    Direct 11-character YouTube ID.
    """

    value = str(value or "").strip()

    if not value:
        return None

    # Direct ID
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
            flags=re.IGNORECASE,
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


def make_full_state(room: Room) -> dict[str, Any]:
    return {
        "type": "full_state",
        "room": room.room_id,
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(
            room.get_position(),
            3,
        ),
        "participants": participants(room),
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

    dead: list[str] = []

    for client_id, client in list(
        room.clients.items()
    ):

        if client_id == exclude:
            continue

        success = await send_json(
            client.websocket,
            data,
        )

        if not success:
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

<title>Watch Together</title>

<style>

* {
    box-sizing: border-box;
}

:root {
    --bg: #08080a;
    --panel: #111114;
    --panel2: #18181c;
    --border: rgba(255,255,255,.08);
    --text: #f5f5f5;
    --muted: #9b9ba3;
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
    cursor: pointer;
}

#app {
    width: 100%;
    height: 100%;

    display: flex;
    flex-direction: column;
}


/* ============================================================
   HEADER
   ============================================================ */

.header {
    height: 62px;
    min-height: 62px;

    display: flex;
    align-items: center;

    padding: 0 16px;

    background: rgba(8,8,10,.96);

    border-bottom:
        1px solid var(--border);

    z-index: 20;
}

.logo {
    font-size: 17px;
    font-weight: 800;
}

.logo span {
    color: var(--muted);
    font-weight: 500;
}

.header-right {
    margin-left: auto;

    display: flex;
    align-items: center;
    gap: 8px;
}

.room-label {
    color: var(--muted);
    font-size: 12px;
}

.room-label strong {
    color: var(--text);
}

.icon-button {
    width: 40px;
    height: 40px;

    border-radius: 10px;

    border:
        1px solid var(--border);

    background: var(--panel2);
    color: var(--text);
}

.icon-button:hover {
    background: #222228;
}


/* ============================================================
   MAIN
   ============================================================ */

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


/* ============================================================
   PLAYER
   ============================================================ */

.player-container {
    width: 100%;

    aspect-ratio: 16 / 9;

    background: #000;

    position: relative;
}

#player {
    width: 100%;
    height: 100%;
}

.player-placeholder {
    position: absolute;

    inset: 0;

    display: flex;
    align-items: center;
    justify-content: center;

    text-align: center;

    color: var(--muted);

    background:
        radial-gradient(
            circle at center,
            #17171c,
            #050507 70%
        );

    pointer-events: none;

    z-index: 2;
}

.player-placeholder.hidden {
    display: none;
}


/* ============================================================
   VIDEO BAR
   ============================================================ */

.video-bar {
    display: flex;

    gap: 8px;

    padding: 12px;

    background: var(--panel);

    border-bottom:
        1px solid var(--border);
}

.video-input {
    flex: 1;
    min-width: 0;

    height: 42px;

    padding: 0 13px;

    color: var(--text);

    background: #0c0c0f;

    border:
        1px solid var(--border);

    border-radius: 10px;

    outline: none;
}

.video-input:focus {
    border-color:
        rgba(255,255,255,.22);
}

.load-button {
    height: 42px;

    padding: 0 18px;

    border: 0;

    border-radius: 10px;

    background: #fff;

    color: #000;

    font-weight: 700;
}

.load-button:hover {
    opacity: .9;
}


/* ============================================================
   CHAT
   ============================================================ */

.chat-panel {
    width: 350px;
    min-width: 350px;

    display: flex;
    flex-direction: column;

    background: var(--panel);

    border-left:
        1px solid var(--border);

    transition:
        width .25s ease,
        min-width .25s ease,
        transform .25s ease;
}

.chat-panel.closed {
    width: 0;
    min-width: 0;

    overflow: hidden;
}

.chat-header {
    height: 56px;
    min-height: 56px;

    display: flex;
    align-items: center;

    padding: 0 10px 0 15px;

    border-bottom:
        1px solid var(--border);
}

.chat-title {
    font-weight: 700;
}

.chat-online {
    color: var(--muted);
    font-size: 11px;
}

.chat-close {
    margin-left: auto;
}

.chat-messages {
    flex: 1;
    min-height: 0;

    overflow-y: auto;

    padding: 14px;
}

.chat-message {
    margin-bottom: 13px;
}

.chat-author {
    margin-bottom: 3px;

    color: var(--muted);

    font-size: 12px;
}

.chat-text {
    font-size: 14px;

    line-height: 1.4;

    word-break: break-word;
}

.chat-form {
    display: flex;

    gap: 8px;

    padding: 12px;

    border-top:
        1px solid var(--border);
}

.chat-input {
    flex: 1;
    min-width: 0;

    height: 40px;

    padding: 0 12px;

    border:
        1px solid var(--border);

    border-radius: 9px;

    background: #0c0c0f;
    color: var(--text);

    outline: none;
}

.send-button {
    width: 42px;

    border: 0;

    border-radius: 9px;

    background: #fff;

    color: #000;

    font-weight: 800;
}


/* ============================================================
   FOOTER
   ============================================================ */

.footer {
    height: 50px;
    min-height: 50px;

    display: flex;
    align-items: center;

    padding: 0 14px;

    background: var(--panel);

    border-top:
        1px solid var(--border);
}

.connection {
    display: flex;
    align-items: center;

    gap: 7px;

    color: var(--muted);

    font-size: 12px;
}

.connection-dot {
    width: 7px;
    height: 7px;

    border-radius: 50%;

    background: #22c55e;
}

.connection-dot.offline {
    background: #ef4444;
}

.users {
    margin-left: auto;

    color: var(--muted);

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

    background:
        rgba(0,0,0,.78);

    backdrop-filter: blur(8px);
}

.modal.hidden {
    display: none;
}

.modal-card {
    width: min(420px, 100%);

    padding: 24px;

    border:
        1px solid var(--border);

    border-radius: 16px;

    background: #121216;
}

.modal-title {
    margin: 0 0 7px;

    font-size: 22px;
}

.modal-text {
    margin: 0 0 20px;

    color: var(--muted);

    font-size: 14px;
}

.nickname-input {
    width: 100%;

    height: 44px;

    padding: 0 13px;

    border:
        1px solid var(--border);

    border-radius: 10px;

    background: #0b0b0e;

    color: var(--text);

    outline: none;
}

.join-button {
    width: 100%;

    height: 44px;

    margin-top: 10px;

    border: 0;

    border-radius: 10px;

    background: #fff;

    color: #000;

    font-weight: 700;
}


/* ============================================================
   TOAST
   ============================================================ */

.toast {
    position: fixed;

    left: 50%;
    bottom: 70px;

    transform:
        translate(-50%, 15px);

    opacity: 0;

    pointer-events: none;

    z-index: 300;

    padding: 10px 14px;

    border-radius: 9px;

    background: #fff;
    color: #000;

    font-size: 13px;

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
        padding: 0 10px;
    }

    .room-label {
        display: none;
    }

    .player-container {
        aspect-ratio: 16 / 9;
    }

    .video-bar {
        flex-wrap: wrap;
    }

    .video-input {
        flex-basis: 100%;
        width: 100%;
    }

    .load-button {
        flex: 1;
    }

    .chat-panel {
        position: absolute;

        right: 0;
        top: 0;
        bottom: 0;

        width: min(360px, 92vw);
        min-width: min(360px, 92vw);

        z-index: 50;

        box-shadow:
            -20px 0 60px
            rgba(0,0,0,.5);
    }

    .chat-panel.closed {
        width: min(360px, 92vw);
        min-width: min(360px, 92vw);

        transform:
            translateX(105%);
    }
}

</style>

</head>


<body>


<div id="app">


    <!-- HEADER -->

    <header class="header">

        <div class="logo">
            Watch<span>Together</span>
        </div>

        <div class="header-right">

            <div class="room-label">
                ROOM
                <strong id="roomCode">------</strong>
            </div>

            <button
                id="copyButton"
                class="icon-button"
                title="Copy room link"
            >
                🔗
            </button>

            <button
                id="chatButton"
                class="icon-button"
                title="Chat"
            >
                💬
            </button>

        </div>

    </header>


    <!-- MAIN -->

    <main class="main">


        <!-- VIDEO -->

        <section class="video-section">

            <div class="player-container">

                <div
                    id="player"
                ></div>

                <div
                    id="placeholder"
                    class="player-placeholder"
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
                >
                    Load video
                </button>

            </div>

        </section>


        <!-- CHAT -->

        <aside
            id="chatPanel"
            class="chat-panel"
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
                    id="closeChat"
                    class="icon-button chat-close"
                >
                    →
                </button>

            </div>


            <div
                id="chatMessages"
                class="chat-messages"
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
                    class="send-button"
                    type="submit"
                >
                    ↑
                </button>

            </form>

        </aside>


    </main>


    <!-- FOOTER -->

    <footer class="footer">

        <div class="connection">

            <span
                id="connectionDot"
                class="connection-dot offline"
            ></span>

            <span id="connectionText">
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


<!-- NICKNAME MODAL -->

<div
    id="nicknameModal"
    class="modal"
>

    <div class="modal-card">

        <h2 class="modal-title">
            Join room
        </h2>

        <p class="modal-text">
            Choose a nickname.
        </p>

        <input
            id="nicknameInput"
            class="nickname-input"
            maxlength="24"
            placeholder="Your nickname"
            autocomplete="off"
        >

        <button
            id="joinButton"
            class="join-button"
        >
            Join room
        </button>

    </div>

</div>


<div
    id="toast"
    class="toast"
></div>


<!-- ============================================================
     YOUTUBE API
     ============================================================ -->

<script src="https://www.youtube.com/iframe_api"></script>


<script>

"use strict";


/* ============================================================
   VARIABLES
   ============================================================ */

const roomId =
    location.pathname.startsWith("/r/")
        ? location.pathname
            .split("/")[2]
            .toUpperCase()
        : null;


let websocket = null;

let player = null;

let playerReady = false;

let nickname = "";

let currentVideoId = "";

let reconnectTimer = null;

let intentionallyClosed = false;


/*
    Prevent local YouTube events generated by
    remote commands from being sent back.
*/

let suppressEventsUntil = 0;


/*
    Last position known locally.

    Used to detect manual seeking because
    YouTube IFrame API doesn't expose a simple
    "seek" event.
*/

let lastKnownPosition = 0;

let lastKnownPlayerState = -1;


/*
    Prevent the sync loop from sending repeatedly.
*/

let lastSentSeek = -1;


/* ============================================================
   DOM
   ============================================================ */

const chatPanel =
    document.getElementById("chatPanel");

const chatMessages =
    document.getElementById("chatMessages");

const chatInput =
    document.getElementById("chatInput");

const videoInput =
    document.getElementById("videoInput");

const placeholder =
    document.getElementById("placeholder");

const nicknameModal =
    document.getElementById("nicknameModal");

const nicknameInput =
    document.getElementById("nicknameInput");

const toast =
    document.getElementById("toast");


document.getElementById(
    "roomCode"
).textContent = roomId || "------";


/* ============================================================
   TOAST
   ============================================================ */

function showToast(text) {

    toast.textContent = text;

    toast.classList.add("show");

    setTimeout(() => {
        toast.classList.remove("show");
    }, 2200);
}


/* ============================================================
   CONNECTION UI
   ============================================================ */

function setConnection(connected) {

    const dot =
        document.getElementById(
            "connectionDot"
        );

    const text =
        document.getElementById(
            "connectionText"
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
   WEBSOCKET SEND
   ============================================================ */

function send(data) {

    if (
        !websocket ||
        websocket.readyState !== WebSocket.OPEN
    ) {
        return false;
    }

    try {

        websocket.send(
            JSON.stringify(data)
        );

        return true;

    } catch (error) {

        return false;
    }
}


/* ============================================================
   CHAT
   ============================================================ */

function clearChat() {
    chatMessages.innerHTML = "";
}


function addChatMessage(message) {

    const wrapper =
        document.createElement("div");

    wrapper.className =
        "chat-message";


    const author =
        document.createElement("div");

    author.className =
        "chat-author";

    author.textContent =
        message.nickname || "Guest";


    const text =
        document.createElement("div");

    text.className =
        "chat-text";

    text.textContent =
        message.text || "";


    wrapper.appendChild(author);
    wrapper.appendChild(text);

    chatMessages.appendChild(wrapper);

    chatMessages.scrollTop =
        chatMessages.scrollHeight;
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
   YOUTUBE API
   ============================================================ */

window.onYouTubeIframeAPIReady = function () {

    createPlayer();
};


function createPlayer() {

    if (player) {
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
                    modestbranding: 1,
                    playsinline: 1,
                    origin: location.origin
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

    /*
        Ask server for current room state.

        This is important if the user joined
        an already-running room.
    */

    send({
        type: "request_state"
    });
}


/* ============================================================
   PLAYER STATE CHANGE
   ============================================================ */

function onPlayerStateChange(event) {

    if (!playerReady) {
        return;
    }


    const state =
        event.data;


    /*
        Ignore events generated by remote commands.
    */

    if (
        Date.now() <
        suppressEventsUntil
    ) {
        lastKnownPlayerState = state;

        if (player) {
            lastKnownPosition =
                player.getCurrentTime() || 0;
        }

        return;
    }


    if (!player) {
        return;
    }


    const position =
        player.getCurrentTime() || 0;


    /*
        PLAYING
    */

    if (
        state ===
        YT.PlayerState.PLAYING
    ) {

        send({
            type: "play",
            position: position
        });
    }


    /*
        PAUSED
    */

    else if (
        state ===
        YT.PlayerState.PAUSED
    ) {

        send({
            type: "pause",
            position: position
        });
    }


    lastKnownPlayerState =
        state;

    lastKnownPosition =
        position;
}


/* ============================================================
   APPLY SERVER STATE
   ============================================================ */

function applyFullState(data) {

    updateParticipants(
        data.participants || []
    );


    /*
        Chat
    */

    clearChat();

    for (
        const message
        of (data.chat || [])
    ) {

        addChatMessage(message);
    }


    /*
        No video
    */

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


    if (!playerReady || !player) {
        return;
    }


    const videoId =
        data.video_id;

    const position =
        Number(data.position || 0);


    /*
        Suppress events caused by
        this remote state.
    */

    suppressEventsUntil =
        Date.now() + 1200;


    /*
        Different video.
    */

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


        /*
            loadVideoById may autoplay
            depending on browser/player state.

            Force the correct state after
            a short delay.
        */

        setTimeout(() => {

            if (!player) {
                return;
            }

            if (data.playing) {

                player.playVideo();

            } else {

                player.pauseVideo();
            }

        }, 500);


        lastKnownPosition =
            position;

        return;
    }


    /*
        Same video.
    */

    const localPosition =
        player.getCurrentTime() || 0;


    /*
        Correct large drift.
    */

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


    /*
        Correct play state.
    */

    if (data.playing) {

        player.playVideo();

    } else {

        player.pauseVideo();
    }


    lastKnownPosition =
        position;
}


/* ============================================================
   REMOTE PLAY
   ============================================================ */

function applyRemotePlay(position) {

    if (!playerReady || !player) {
        return;
    }


    suppressEventsUntil =
        Date.now() + 1000;


    const local =
        player.getCurrentTime() || 0;


    if (
        Math.abs(local - position)
        > 1.25
    ) {

        player.seekTo(
            position,
            true
        );
    }


    player.playVideo();


    lastKnownPosition =
        position;
}


/* ============================================================
   REMOTE PAUSE
   ============================================================ */

function applyRemotePause(position) {

    if (!playerReady || !player) {
        return;
    }


    suppressEventsUntil =
        Date.now() + 1000;


    const local =
        player.getCurrentTime() || 0;


    if (
        Math.abs(local - position)
        > 1.25
    ) {

        player.seekTo(
            position,
            true
        );
    }


    player.pauseVideo();


    lastKnownPosition =
        position;
}


/* ============================================================
   REMOTE SEEK
   ============================================================ */

function applyRemoteSeek(position) {

    if (!playerReady || !player) {
        return;
    }


    suppressEventsUntil =
        Date.now() + 800;


    player.seekTo(
        position,
        true
    );


    lastKnownPosition =
        position;
}


/* ============================================================
   REMOTE VIDEO
   ============================================================ */

function applyRemoteVideo(videoId) {

    if (!playerReady || !player) {
        return;
    }


    currentVideoId =
        videoId;


    placeholder.classList.add(
        "hidden"
    );


    suppressEventsUntil =
        Date.now() + 1500;


    player.loadVideoById({
        videoId: videoId,
        startSeconds: 0
    });


    /*
        New videos start paused.

        Server will send the actual play state
        separately if required.
    */

    setTimeout(() => {

        if (player) {
            player.pauseVideo();
        }

    }, 400);


    lastKnownPosition = 0;
}


/* ============================================================
   HANDLE WEBSOCKET MESSAGE
   ============================================================ */

function handleMessage(data) {

    if (!data || !data.type) {
        return;
    }


    /*
        Full state
    */

    if (
        data.type ===
        "full_state"
    ) {

        applyFullState(data);

        return;
    }


    /*
        New video
    */

    if (
        data.type ===
        "video"
    ) {

        applyRemoteVideo(
            data.video_id
        );

        return;
    }


    /*
        Play
    */

    if (
        data.type ===
        "play"
    ) {

        applyRemotePlay(
            Number(
                data.position || 0
            )
        );

        return;
    }


    /*
        Pause
    */

    if (
        data.type ===
        "pause"
    ) {

        applyRemotePause(
            Number(
                data.position || 0
            )
        );

        return;
    }


    /*
        Seek
    */

    if (
        data.type ===
        "seek"
    ) {

        applyRemoteSeek(
            Number(
                data.position || 0
            )
        );

        return;
    }


    /*
        Chat
    */

    if (
        data.type ===
        "chat"
    ) {

        addChatMessage(
            data.message
        );

        return;
    }


    /*
        Participants
    */

    if (
        data.type ===
        "participants"
    ) {

        updateParticipants(
            data.participants || []
        );

        return;
    }


    /*
        Error
    */

    if (
        data.type ===
        "error"
    ) {

        showToast(
            data.message ||
            "Server error"
        );

        return;
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
        websocket &&
        websocket.readyState ===
        WebSocket.OPEN
    ) {
        return;
    }


    if (websocket) {

        try {
            websocket.close();
        } catch (_) {}
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


    websocket =
        new WebSocket(url);


    websocket.onopen =
        function () {

            setConnection(true);


            send({
                type: "join",
                nickname: nickname
            });
        };


    websocket.onmessage =
        function (event) {

            try {

                const data =
                    JSON.parse(
                        event.data
                    );

                handleMessage(data);

            } catch (error) {

                console.error(
                    "Invalid WebSocket message",
                    error
                );
            }
        };


    websocket.onerror =
        function () {

            setConnection(false);
        };


    websocket.onclose =
        function () {

            setConnection(false);


            if (
                intentionallyClosed
            ) {
                return;
            }


            clearTimeout(
                reconnectTimer
            );


            reconnectTimer =
                setTimeout(
                    () => {
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
            "Paste a YouTube link"
        );

        return;
    }


    if (
        !websocket ||
        websocket.readyState !==
        WebSocket.OPEN
    ) {

        showToast(
            "Not connected"
        );

        return;
    }


    /*
        The server extracts and validates
        the actual YouTube ID.
    */

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
            event.key === "Enter"
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
   CHAT TOGGLE
   ============================================================ */

document.getElementById(
    "chatButton"
).addEventListener(
    "click",
    function () {

        chatPanel.classList.toggle(
            "closed"
        );
    }
);


document.getElementById(
    "closeChat"
).addEventListener(
    "click",
    function () {

        chatPanel.classList.add(
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

        } catch (_) {

            showToast(
                location.href
            );
        }
    }
);


/* ============================================================
   POSITION / SEEK DETECTOR
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
            !websocket ||
            websocket.readyState !==
            WebSocket.OPEN
        ) {
            return;
        }


        if (
            Date.now() <
            suppressEventsUntil
        ) {
            return;
        }


        let position = 0;

        let state = -1;


        try {

            position =
                player.getCurrentTime() || 0;

            state =
                player.getPlayerState();

        } catch (_) {

            return;
        }


        /*
            Detect manual seek.

            Normal playback changes by approximately
            the elapsed time.

            A large jump indicates seek.
        */

        const difference =
            Math.abs(
                position -
                lastKnownPosition
            );


        if (
            state ===
                YT.PlayerState.PLAYING &&
            difference > 1.5
        ) {

            /*
                Don't send the same seek repeatedly.
            */

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


        /*
            When paused, a position jump is
            also a seek.
        */

        else if (
            state ===
                YT.PlayerState.PAUSED &&
            difference > 1.5
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


        lastKnownPosition =
            position;

        lastKnownPlayerState =
            state;

    },
    500
);


/* ============================================================
   NICKNAME
   ============================================================ */

document.getElementById(
    "joinButton"
).addEventListener(
    "click",
    function () {

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
);


nicknameInput.addEventListener(
    "keydown",
    function (event) {

        if (
            event.key === "Enter"
        ) {

            event.preventDefault();

            document.getElementById(
                "joinButton"
            ).click();
        }
    }
);


/* ============================================================
   START
   ============================================================ */

function start() {

    /*
        If someone somehow opens the root,
        server should redirect to a room.

        This branch is only a fallback.
    */

    if (!roomId) {

        const generated =
            Math.random()
                .toString(36)
                .slice(2, 8)
                .toUpperCase();


        location.href =
            "/r/" +
            generated;

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


    /*
        If YouTube API has already loaded
        before our callback was installed,
        create the player manually.
    */

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
async def root():

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
):

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
):

    await websocket.accept()


    room_id = room_id.upper()


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


    client: Client | None = None


    try:

        # ====================================================
        # FIRST MESSAGE MUST BE JOIN
        # ====================================================

        first_message =
            await websocket.receive_json()


        if (
            first_message.get("type")
            != "join"
        ):

            await send_json(
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


        client = Client(
            websocket=websocket,
            client_id=client_id,
            nickname=nickname,
        )


        room.clients[
            client_id
        ] = client


        # ====================================================
        # SEND CURRENT ROOM STATE TO NEW CLIENT
        # ====================================================

        await send_json(
            websocket,
            make_full_state(room),
        )


        # ====================================================
        # UPDATE PARTICIPANTS FOR EVERYONE
        # ====================================================

        await broadcast_participants(
            room
        )


        # ====================================================
        # MESSAGE LOOP
        # ====================================================

        while True:

            data =
                await websocket.receive_json()


            message_type =
                data.get("type")


            # =================================================
            # REQUEST STATE
            # =================================================

            if (
                message_type
                == "request_state"
            ):

                await send_json(
                    websocket,
                    make_full_state(
                        room
                    ),
                )


            # =================================================
            # SET VIDEO
            # =================================================

            elif (
                message_type
                == "set_video"
            ):

                video_id =
                    extract_youtube_id(
                        data.get(
                            "video",
                            "",
                        )
                    )


                if not video_id:

                    await send_json(
                        websocket,
                        {
                            "type":
                                "error",

                            "message":
                                "Invalid YouTube URL",
                        },
                    )

                    continue


                # ---------------------------------------------
                # CHANGE ROOM STATE
                # ---------------------------------------------

                room.video_id =
                    video_id

                room.playing = False

                room.position = 0.0

                room.updated_at =
                    time.monotonic()


                # ---------------------------------------------
                # SEND NEW VIDEO TO EVERYONE
                # ---------------------------------------------

                await broadcast(
                    room,
                    {
                        "type":
                            "video",

                        "video_id":
                            video_id,
                    },
                )


            # =================================================
            # PLAY
            # =================================================

            elif (
                message_type
                == "play"
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
                        room.get_position()


                position =
                    max(
                        0.0,
                        position,
                    )


                room.position =
                    position

                room.playing = True

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type":
                            "play",

                        "position":
                            round(
                                position,
                                3,
                            ),
                    },
                    exclude=client_id,
                )


            # =================================================
            # PAUSE
            # =================================================

            elif (
                message_type
                == "pause"
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
                        room.get_position()


                position =
                    max(
                        0.0,
                        position,
                    )


                room.position =
                    position

                room.playing = False

                room.updated_at =
                    time.monotonic()


                await broadcast(
                    room,
                    {
                        "type":
                            "pause",

                        "position":
                            round(
                                position,
                                3,
                            ),
                    },
                    exclude=client_id,
                )


            # =================================================
            # SEEK
            # =================================================

            elif (
                message_type
                == "seek"
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
                        "type":
                            "seek",

                        "position":
                            round(
                                position,
                                3,
                            ),
                    },
                    exclude=client_id,
                )


            # =================================================
            # CHAT
            # =================================================

            elif (
                message_type
                == "chat"
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
                        int(
                            time.time()
                        ),
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
                        "type":
                            "chat",

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

        # ====================================================
        # REMOVE CLIENT
        # ====================================================

        room.clients.pop(
            client_id,
            None,
        )


        # ====================================================
        # UPDATE PARTICIPANTS
        # ====================================================

        try:

            await broadcast_participants(
                room
            )

        except Exception:

            pass


        # ====================================================
        # REMOVE EMPTY ROOM
        # ====================================================

        if not room.clients:

            async with rooms_lock:

                if (
                    room_id in rooms
                    and
                    not rooms[
                        room_id
                    ].clients
                ):

                    rooms.pop(
                        room_id,
                        None,
                    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "ok",
        "rooms": len(rooms),
        "service":
            "watch-together",
    }