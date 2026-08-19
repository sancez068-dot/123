from __future__ import annotations

import asyncio
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI(title="Watch Together")

ROOM_ID_RE = re.compile(r"^[A-Za-z0-9]{6}$")
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
ROOM_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
MAX_NICKNAME = 24
MAX_CHAT = 500
MAX_HISTORY = 100
SYNC_INTERVAL = 4.0


@dataclass
class Participant:
    id: str
    nickname: str
    websocket: WebSocket


@dataclass
class Room:
    room_id: str
    video_id: str | None = None
    playing: bool = False
    position: float = 0.0
    changed_at: float = field(default_factory=time.monotonic)
    participants: dict[str, Participant] = field(default_factory=dict)
    chat: list[dict[str, Any]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    sync_task: asyncio.Task | None = None

    def current_position(self) -> float:
        if self.playing:
            return max(0.0, self.position + (time.monotonic() - self.changed_at))
        return max(0.0, self.position)


rooms: dict[str, Room] = {}
rooms_lock = asyncio.Lock()


def make_room_id() -> str:
    # uuid is sufficient here and avoids a random dependency.
    alphabet = ROOM_CODE_ALPHABET
    value = uuid.uuid4().int
    chars = []
    for _ in range(6):
        chars.append(alphabet[value % len(alphabet)])
        value //= len(alphabet)
    return "".join(chars)


async def get_or_create_room() -> Room:
    async with rooms_lock:
        while True:
            room_id = make_room_id()
            if room_id not in rooms:
                room = Room(room_id=room_id)
                rooms[room_id] = room
                room.sync_task = asyncio.create_task(room_sync_loop(room))
                return room


def clean_nickname(value: Any) -> str:
    text = str(value or "").strip()
    text = " ".join(text.split())
    return text[:MAX_NICKNAME] or "Guest"


def clean_chat(value: Any) -> str:
    text = str(value or "").strip()
    return text[:MAX_CHAT]


def extract_video_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    if VIDEO_ID_RE.fullmatch(raw):
        return raw

    # Accept URLs with or without a protocol and ignore extra query parameters.
    candidate = raw if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw) else "https://" + raw
    try:
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(candidate)
        host = parsed.netloc.lower().split(":", 1)[0]
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
            if path == "/watch":
                value = query.get("v", [None])[0]
                return value if value and VIDEO_ID_RE.fullmatch(value) else None
            for prefix in ("/embed/", "/shorts/", "/live/"):
                if path.startswith(prefix):
                    value = path[len(prefix):].split("/", 1)[0]
                    return value if VIDEO_ID_RE.fullmatch(value) else None

        if host in {"youtu.be", "www.youtu.be"}:
            value = path.lstrip("/").split("/", 1)[0]
            return value if VIDEO_ID_RE.fullmatch(value) else None
    except Exception:
        return None
    return None


async def send_json(ws: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await ws.send_json(payload)
        return True
    except Exception:
        return False


async def broadcast(room: Room, payload: dict[str, Any]) -> None:
    async with room.lock:
        sockets = [p.websocket for p in room.participants.values()]
    if not sockets:
        return
    results = await asyncio.gather(*(send_json(ws, payload) for ws in sockets), return_exceptions=True)

    dead: list[WebSocket] = []
    for ws, result in zip(sockets, results):
        if result is not True:
            dead.append(ws)
    if dead:
        async with room.lock:
            dead_ids = [pid for pid, p in room.participants.items() if p.websocket in dead]
            for pid in dead_ids:
                room.participants.pop(pid, None)


def state_payload(room: Room) -> dict[str, Any]:
    return {
        "type": "state",
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(room.current_position(), 3),
        "participants": [p.nickname for p in room.participants.values()],
        "chat": room.chat[-MAX_HISTORY:],
    }


async def room_sync_loop(room: Room) -> None:
    try:
        while True:
            await asyncio.sleep(SYNC_INTERVAL)
            async with room.lock:
                if not room.participants:
                    return
                payload = {
                    "type": "sync",
                    "video_id": room.video_id,
                    "playing": room.playing,
                    "position": round(room.current_position(), 3),
                }
            await broadcast(room, payload)
    except asyncio.CancelledError:
        return


async def delete_empty_room(room: Room) -> None:
    async with rooms_lock:
        if not room.participants and rooms.get(room.room_id) is room:
            rooms.pop(room.room_id, None)
            if room.sync_task and not room.sync_task.done():
                room.sync_task.cancel()


async def broadcast_participants(room: Room) -> None:
    async with room.lock:
        names = [p.nickname for p in room.participants.values()]
    await broadcast(room, {"type": "participants", "participants": names})


@app.get("/", response_class=RedirectResponse)
async def index() -> RedirectResponse:
    room = await get_or_create_room()
    return RedirectResponse(url=f"/r/{room.room_id}", status_code=307)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/r/{room_id}", response_class=HTMLResponse)
async def room_page(room_id: str) -> HTMLResponse:
    if not ROOM_ID_RE.fullmatch(room_id):
        return HTMLResponse("Invalid room", status_code=404)
    async with rooms_lock:
        room = rooms.get(room_id)
        if room is None:
            room = Room(room_id=room_id)
            rooms[room_id] = room
            room.sync_task = asyncio.create_task(room_sync_loop(room))
    return HTMLResponse(HTML_PAGE.replace("__ROOM_ID__", room_id))


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str) -> None:
    if not ROOM_ID_RE.fullmatch(room_id):
        await websocket.close(code=1008, reason="Invalid room")
        return

    await websocket.accept()
    async with rooms_lock:
        room = rooms.get(room_id)
        if room is None:
            room = Room(room_id=room_id)
            rooms[room_id] = room
            room.sync_task = asyncio.create_task(room_sync_loop(room))

    participant: Participant | None = None
    try:
        first = await websocket.receive_json()
        if first.get("type") != "join":
            await send_json(websocket, {"type": "error", "message": "First message must be join."})
            await websocket.close(code=1008)
            return

        participant = Participant(uuid.uuid4().hex, clean_nickname(first.get("nickname")), websocket)
        async with room.lock:
            room.participants[participant.id] = participant
            state = state_payload(room)
        await send_json(websocket, state)
        await broadcast_participants(room)

        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "set_video":
                video_id = extract_video_id(data.get("video"))
                if not video_id:
                    await send_json(websocket, {"type": "error", "message": "Invalid YouTube video link or ID."})
                    continue
                async with room.lock:
                    room.video_id = video_id
                    room.position = 0.0
                    room.playing = False
                    room.changed_at = time.monotonic()
                await broadcast(room, {
                    "type": "state",
                    "video_id": video_id,
                    "playing": False,
                    "position": 0.0,
                    "participants": [p.nickname for p in room.participants.values()],
                    "chat": room.chat[-MAX_HISTORY:],
                })

            elif msg_type in {"play", "pause", "seek"}:
                try:
                    position = max(0.0, float(data.get("position", 0)))
                except (TypeError, ValueError):
                    await send_json(websocket, {"type": "error", "message": "Invalid playback position."})
                    continue
                async with room.lock:
                    room.position = position
                    room.changed_at = time.monotonic()
                    if msg_type == "play":
                        room.playing = True
                    elif msg_type == "pause":
                        room.playing = False
                await broadcast(room, {
                    "type": msg_type,
                    "position": round(position, 3),
                })

            elif msg_type == "chat":
                text = clean_chat(data.get("text"))
                if not text:
                    continue
                message = {
                    "nickname": participant.nickname,
                    "text": text,
                    "ts": int(time.time()),
                }
                async with room.lock:
                    room.chat.append(message)
                    room.chat = room.chat[-MAX_HISTORY:]
                await broadcast(room, {"type": "chat", **message})

            elif msg_type == "sync":
                async with room.lock:
                    payload = {
                        "type": "sync",
                        "video_id": room.video_id,
                        "playing": room.playing,
                        "position": round(room.current_position(), 3),
                    }
                await send_json(websocket, payload)

            elif msg_type == "join":
                await send_json(websocket, {"type": "error", "message": "Already joined."})
            else:
                await send_json(websocket, {"type": "error", "message": "Unknown message type."})

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if participant is not None:
            async with room.lock:
                room.participants.pop(participant.id, None)
            await broadcast_participants(room)
            await delete_empty_room(room)


HTML_PAGE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Watch Together</title>
<style>
:root{color-scheme:dark;--bg:#090a0c;--panel:#111317;--panel2:#0d0f12;--line:#24272d;--text:#f2f3f5;--muted:#8f949d;--accent:#fff}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0;width:100%;height:100%;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;overflow:hidden}
button,input{font:inherit}
button{color:inherit}
.app{height:100dvh;display:flex;flex-direction:column;background:var(--bg)}
.topbar{height:58px;min-height:58px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:10px;padding:0 14px;background:#0b0c0f;z-index:20}
.logo{font-weight:700;letter-spacing:.2px;white-space:nowrap}.room{color:var(--muted);font-size:12px;padding:5px 8px;border:1px solid var(--line);border-radius:7px}
.urlbar{display:flex;gap:8px;margin-left:auto;width:min(620px,55vw)}
.urlbar input{min-width:0;flex:1;background:#15171b;border:1px solid var(--line);outline:0;color:var(--text);border-radius:8px;padding:9px 11px}.urlbar input:focus{border-color:#454951}
.btn{border:1px solid var(--line);background:#17191e;border-radius:8px;padding:8px 12px;cursor:pointer}.btn:hover{background:#1c1f24}.btn:active{transform:translateY(1px)}
.main{min-height:0;flex:1;display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:10px;padding:10px}
.video-wrap{min-width:0;min-height:0;position:relative;background:#000;border:1px solid var(--line);border-radius:10px;overflow:hidden;display:flex;align-items:center;justify-content:center}
#player{width:100%;height:100%}.empty{position:absolute;inset:0;display:grid;place-items:center;color:#686d76;font-size:14px;pointer-events:none}
.chat{min-width:0;min-height:0;border:1px solid var(--line);border-radius:10px;background:var(--panel);display:flex;flex-direction:column;overflow:hidden}
.chat-head{height:50px;min-height:50px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 13px}.chat-title{font-weight:650}.online{color:var(--muted);font-size:12px}
.messages{min-height:0;flex:1;overflow-y:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;padding:10px 11px;scrollbar-width:thin}
.message{padding:5px 2px;line-height:1.35;overflow-wrap:anywhere}.name{font-size:12px;color:#aeb3bc;margin-right:6px}.text{font-size:13px;color:#eceef1}
.chat-input{border-top:1px solid var(--line);padding:9px;display:flex;gap:7px;background:var(--panel2)}.chat-input input{min-width:0;flex:1;background:#15171b;border:1px solid var(--line);border-radius:8px;padding:9px 10px;outline:0}.chat-input input:focus{border-color:#454951}
.mobile-chat-toggle{display:none;position:absolute;right:10px;bottom:10px;z-index:15}
.overlay-chat{display:none}
.status{position:absolute;left:10px;top:10px;z-index:12;background:rgba(0,0,0,.65);border:1px solid rgba(255,255,255,.12);border-radius:7px;padding:5px 8px;font-size:11px;color:#c9ccd2;pointer-events:none;opacity:0;transition:opacity .2s}.status.show{opacity:1}
.fullscreen .topbar{display:none}.fullscreen .main{padding:0;display:block}.fullscreen .video-wrap{border:0;border-radius:0;width:100%;height:100%}.fullscreen .chat{display:none}.fullscreen .mobile-chat-toggle{display:block}
@media (max-width:900px){
 .topbar{height:52px;min-height:52px}.logo{font-size:14px}.room{display:none}.urlbar{width:auto;flex:1}.urlbar input{padding:8px}.urlbar .btn{padding:8px 10px}
 .main{display:block;position:relative;padding:0}.video-wrap{border:0;border-radius:0;height:100%;width:100%}.chat{display:none}.mobile-chat-toggle{display:block}
 .overlay-chat{position:absolute;display:flex;left:0;right:0;bottom:0;height:min(52dvh,520px);z-index:14;background:linear-gradient(to bottom,rgba(10,11,13,.04),rgba(10,11,13,.98) 18%);pointer-events:none;padding-top:28px}
 .overlay-chat.hidden{display:none}.overlay-chat .chat{display:flex;position:relative;inset:auto;width:100%;height:100%;border:0;border-radius:0;background:rgba(12,14,17,.94);pointer-events:auto}
 .overlay-chat .chat-head{background:rgba(13,15,18,.97)}
 .overlay-chat .messages{padding-bottom:8px}
}
@media (orientation:landscape) and (max-width:900px){.overlay-chat{height:min(72dvh,420px);width:min(420px,92vw);left:auto;right:8px;bottom:8px;border:1px solid var(--line);border-radius:9px;padding-top:0;background:rgba(12,14,17,.96)}}
@media (max-width:500px){.urlbar{gap:5px}.urlbar input{font-size:13px}.urlbar .btn{font-size:13px;padding:8px}.chat-input{padding-bottom:calc(9px + env(safe-area-inset-bottom))}}
</style>
</head>
<body>
<div class="app" id="app">
<header class="topbar">
  <div class="logo">Watch Together</div><div class="room">ROOM <span id="roomCode">__ROOM_ID__</span></div>
  <form class="urlbar" id="videoForm"><input id="videoInput" autocomplete="off" placeholder="YouTube link or video ID"><button class="btn" type="submit">Load</button></form>
</header>
<main class="main">
  <section class="video-wrap" id="videoWrap">
    <div id="player"></div><div class="empty" id="empty">Enter a YouTube video to start</div><div class="status" id="status"></div>
    <button class="btn mobile-chat-toggle" id="chatToggle">Chat</button>
  </section>
  <aside class="chat" id="desktopChat"></aside>
  <div class="overlay-chat hidden" id="overlayChat"><aside class="chat" id="mobileChat"></aside></div>
</main>
</div>
<script>
const ROOM_ID="__ROOM_ID__";
const MAX_NICKNAME=24, MAX_CHAT=500;
let ws=null, reconnectTimer=null, reconnectDelay=500, joined=false, player=null, playerReady=false;
let applyingRemote=false, suppressEventsUntil=0, currentVideo=null, userWantsPlaying=false;
let nickname=sessionStorage.getItem("wt_nickname")||prompt("Nickname (max 24 characters):","")||"Guest";
nickname=nickname.trim().replace(/\s+/g," ").slice(0,MAX_NICKNAME)||"Guest"; sessionStorage.setItem("wt_nickname",nickname);

const app=document.getElementById("app"), empty=document.getElementById("empty"), statusEl=document.getElementById("status");
const videoInput=document.getElementById("videoInput"), videoForm=document.getElementById("videoForm");
const overlayChat=document.getElementById("overlayChat"), chatToggle=document.getElementById("chatToggle");

function makeChat(){
  const el=document.createElement("aside"); el.className="chat";
  el.innerHTML='<div class="chat-head"><span class="chat-title">Chat</span><span class="online" data-online>0 online</span></div><div class="messages" data-messages></div><form class="chat-input"><input maxlength="500" autocomplete="off" placeholder="Message..."><button class="btn" type="submit">Send</button></form>';
  const form=el.querySelector("form"), input=el.querySelector("input");
  form.addEventListener("submit",e=>{e.preventDefault();const text=input.value.trim().slice(0,MAX_CHAT);if(text){send({type:"chat",text});input.value="";input.focus();}});
  el.querySelector("[data-messages]").addEventListener("scroll",()=>updateScrollState(el));
  return el;
}
const desktopChat=makeChat(), mobileChat=makeChat();
document.getElementById("desktopChat").replaceWith(desktopChat); document.getElementById("mobileChat").replaceWith(mobileChat);

function getMessages(){return [desktopChat.querySelector("[data-messages]"),mobileChat.querySelector("[data-messages]")];}
function updateScrollState(chat){chat._atBottom=chat.scrollHeight-chat.scrollTop-chat.clientHeight<40;}
function addMessage(m){
  for(const box of getMessages()){
    const stick=box._atBottom!==false;
    const row=document.createElement("div"); row.className="message";
    const name=document.createElement("span"); name.className="name"; name.textContent=(m.nickname||"Guest")+":";
    const text=document.createElement("span"); text.className="text"; text.textContent=m.text||"";
    row.append(name,text); box.appendChild(row);
    if(box.children.length>100) box.removeChild(box.firstChild);
    if(stick) box.scrollTop=box.scrollHeight;
    updateScrollState(box);
  }
}
function setHistory(history){
  for(const box of getMessages()){box.replaceChildren();box._atBottom=true;}
  (history||[]).forEach(addMessage);
  for(const box of getMessages())box.scrollTop=box.scrollHeight;
}
function setOnline(list){for(const el of [desktopChat,mobileChat])el.querySelector("[data-online]").textContent=`${(list||[]).length} online`;}

function showStatus(text){statusEl.textContent=text;statusEl.classList.add("show");clearTimeout(showStatus.t);showStatus.t=setTimeout(()=>statusEl.classList.remove("show"),1800)}
function send(obj){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));}

function connect(){
  clearTimeout(reconnectTimer); joined=false;
  const proto=location.protocol==="https:"?"wss":"ws";
  ws=new WebSocket(`${proto}://${location.host}/ws/${ROOM_ID}`);
  ws.onopen=()=>{reconnectDelay=500;send({type:"join",nickname});};
  ws.onmessage=e=>{try{handleMessage(JSON.parse(e.data))}catch{}};
  ws.onclose=()=>{joined=false;clearTimeout(reconnectTimer);reconnectTimer=setTimeout(connect,reconnectDelay);reconnectDelay=Math.min(reconnectDelay*1.7,5000);showStatus("Reconnecting…")};
  ws.onerror=()=>{};
}

function handleMessage(m){
  if(m.type==="state"){
    joined=true;setOnline(m.participants||[]);setHistory(m.chat||[]);
    currentVideo=m.video_id||null; empty.style.display=currentVideo?"none":"grid";
    if(currentVideo)loadVideo(currentVideo,!!m.playing,Number(m.position)||0);
    else if(playerReady){applyingRemote=true;player.pauseVideo();player.seekTo(0,true);setTimeout(()=>applyingRemote=false,100)}
    return;
  }
  if(m.type==="participants"){setOnline(m.participants||[]);return}
  if(m.type==="chat"){addMessage(m);return}
  if(m.type==="error"){showStatus(m.message||"Error");return}
  if(m.type==="play"){remotePlayback("play",Number(m.position)||0);return}
  if(m.type==="pause"){remotePlayback("pause",Number(m.position)||0);return}
  if(m.type==="sync"){remoteSync(m);return}
}

function ensurePlayer(){
  if(window.YT&&window.YT.Player){createPlayer();return}
  if(!document.getElementById("yt-api")){const s=document.createElement("script");s.id="yt-api";s.src="https://www.youtube.com/iframe_api";document.head.appendChild(s)}
}
window.onYouTubeIframeAPIReady=()=>createPlayer();
function createPlayer(){
  if(player) return;
  player=new YT.Player("player",{width:"100%",height:"100%",videoId:currentVideo||undefined,playerVars:{autoplay:0,controls:1,rel:0,playsinline:1,modestbranding:1},events:{onReady:()=>{playerReady=true;if(currentVideo)send({type:"sync"})},onStateChange:onPlayerStateChange,onError:()=>showStatus("YouTube could not play this video")}});
}
function loadVideo(id,playing,pos){
  currentVideo=id;empty.style.display="none";ensurePlayer();
  const apply=()=>{if(!playerReady||!player)return setTimeout(apply,80);applyingRemote=true; suppressEventsUntil=Date.now()+500; player.cueVideoById({videoId:id,startSeconds:Math.max(0,pos)}); setTimeout(()=>{if(!playerReady)return; if(playing)player.playVideo();else player.pauseVideo(); setTimeout(()=>{applyingRemote=false;suppressEventsUntil=0},300)},180)};
  apply();
}
function remotePlayback(kind,pos){
  if(!playerReady||!player)return;
  applyingRemote=true;suppressEventsUntil=Date.now()+500;
  player.seekTo(pos,true); if(kind==="play")player.playVideo();else player.pauseVideo();
  setTimeout(()=>{applyingRemote=false;suppressEventsUntil=0},550);
}
function remoteSync(m){
  if(!m.video_id){return}
  if(m.video_id!==currentVideo){loadVideo(m.video_id,!!m.playing,Number(m.position)||0);return}
  if(!playerReady||!player||applyingRemote)return;
  const local=player.getCurrentTime?player.getCurrentTime():0, target=Number(m.position)||0, diff=target-local;
  if(Math.abs(diff)>0.8){applyingRemote=true;suppressEventsUntil=Date.now()+500;player.seekTo(target,true);setTimeout(()=>{applyingRemote=false;suppressEventsUntil=0},550)}
  const state=player.getPlayerState();
  if(m.playing&&state!==YT.PlayerState.PLAYING&&state!==YT.PlayerState.BUFFERING){applyingRemote=true;player.playVideo();setTimeout(()=>applyingRemote=false,500)}
  if(!m.playing&&state===YT.PlayerState.PLAYING){applyingRemote=true;player.pauseVideo();setTimeout(()=>applyingRemote=false,500)}
}
function onPlayerStateChange(e){
  if(applyingRemote||Date.now()<suppressEventsUntil||!joined||!playerReady)return;
  if(e.data===YT.PlayerState.PLAYING){send({type:"play",position:player.getCurrentTime()||0});}
  else if(e.data===YT.PlayerState.PAUSED){send({type:"pause",position:player.getCurrentTime()||0});}
}

videoForm.addEventListener("submit",e=>{e.preventDefault();const value=videoInput.value.trim();if(value)send({type:"set_video",video:value});});
chatToggle.addEventListener("click",()=>overlayChat.classList.toggle("hidden"));
let fsTimer=null;
function addFullscreenChatHook(){
  document.addEventListener("fullscreenchange",()=>{app.classList.toggle("fullscreen",!!document.fullscreenElement);});
}
addFullscreenChatHook();

// Catch direct seek actions from the YouTube player with a short polling window.
let lastTime=0;
setInterval(()=>{
  if(!playerReady||!player||applyingRemote||Date.now()<suppressEventsUntil||!joined)return;
  const state=player.getPlayerState(); if(state!==YT.PlayerState.PLAYING&&state!==YT.PlayerState.PAUSED)return;
  const now=player.getCurrentTime()||0;
  if(lastTime&&Math.abs(now-lastTime)>1.2)send({type:"seek",position:now});
  lastTime=now;
},700);

connect();
ensurePlayer();
</script>
</body>
</html>'''
