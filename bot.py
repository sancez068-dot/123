# -*- coding: utf-8 -*-
import asyncio
import json
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse


app = FastAPI(title="Watch Together", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


ROOM_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


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
    changed_at: float = field(default_factory=time.monotonic)
    clients: dict[str, Client] = field(default_factory=dict)
    chat: list[dict[str, Any]] = field(default_factory=list)

    def position_now(self) -> float:
        if not self.playing:
            return max(0.0, self.position)
        return max(0.0, self.position + (time.monotonic() - self.changed_at))


rooms: dict[str, Room] = {}
rooms_lock = asyncio.Lock()


def make_room_id(length: int = 6) -> str:
    return "".join(secrets.choice(ROOM_CHARS) for _ in range(length))


def make_client_id() -> str:
    return secrets.token_urlsafe(18)


def clean_name(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return (text or "Guest")[:24]


def youtube_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", text):
        return text
    patterns = (
        r"(?:youtube\.com/watch\?.*?[?&]v=|youtube\.com/watch\?v=)([A-Za-z0-9_-]{11})",
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"youtube\.com/embed/([A-Za-z0-9_-]{11})",
        r"youtube\.com/shorts/([A-Za-z0-9_-]{11})",
        r"youtube\.com/live/([A-Za-z0-9_-]{11})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    match = re.search(r"[?&]v=([A-Za-z0-9_-]{11})", text)
    return match.group(1) if match else None


def room_state(room: Room) -> dict[str, Any]:
    return {
        "type": "state",
        "video_id": room.video_id,
        "playing": room.playing,
        "position": round(room.position_now(), 3),
        "participants": [
            {"id": c.client_id, "nickname": c.nickname}
            for c in room.clients.values()
        ],
        "chat": room.chat[-100:],
    }


async def send_json(ws: WebSocket, data: dict[str, Any]) -> bool:
    try:
        await ws.send_json(data)
        return True
    except Exception:
        return False


async def broadcast(room: Room, data: dict[str, Any], exclude: str | None = None) -> None:
    dead = []
    for cid, client in list(room.clients.items()):
        if cid == exclude:
            continue
        if not await send_json(client.websocket, data):
            dead.append(cid)
    for cid in dead:
        room.clients.pop(cid, None)


async def broadcast_users(room: Room) -> None:
    await broadcast(
        room,
        {
            "type": "participants",
            "participants": [
                {"id": c.client_id, "nickname": c.nickname}
                for c in room.clients.values()
            ],
        },
    )


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#090909">
<title>Watch Together</title>
<style>
*{box-sizing:border-box}
html,body{margin:0;width:100%;height:100%;background:#090909;color:#fff;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif}
body{overflow:hidden}
button,input{font:inherit}
button{cursor:pointer}
#app{height:100dvh;display:flex;flex-direction:column;background:#090909}
.header{height:56px;min-height:56px;display:flex;align-items:center;padding:0 12px;background:#111;border-bottom:1px solid #252525;z-index:50}
.logo{font-size:16px;font-weight:800}.logo span{color:#777;font-weight:500}
.header-right{margin-left:auto;display:flex;gap:7px;align-items:center}
.room{font-size:11px;color:#777}.room b{color:#fff}
.icon{width:38px;height:38px;border:1px solid #292929;border-radius:9px;background:#191919;color:#fff}
.main{flex:1;min-height:0;display:flex;background:#000}
.video-section{flex:1;min-width:0;min-height:0;display:flex;flex-direction:column;background:#000}
.player-wrap{position:relative;width:100%;aspect-ratio:16/9;background:#000}
#player{position:absolute;inset:0;width:100%;height:100%}
.placeholder{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#777;background:#090909;z-index:2;pointer-events:none}
.placeholder.hide{display:none}
.video-bar{display:flex;gap:8px;padding:9px;background:#111;border-bottom:1px solid #252525}
.video-input{flex:1;min-width:0;height:41px;padding:0 11px;border:1px solid #292929;border-radius:8px;background:#080808;color:#fff;outline:0}
.load{height:41px;padding:0 15px;border:0;border-radius:8px;background:#fff;color:#000;font-weight:700}
.desktop-chat{width:340px;min-width:340px;display:flex;flex-direction:column;background:#111;border-left:1px solid #252525}
.desktop-chat.closed{display:none}
.chat-head{height:54px;min-height:54px;display:flex;align-items:center;padding:0 11px;border-bottom:1px solid #252525}
.chat-title{font-weight:750}.online{font-size:11px;color:#777;margin-top:2px}
.messages{flex:1;min-height:0;overflow:auto;padding:12px}
.msg{margin-bottom:13px;word-break:break-word}.author{font-size:12px;color:#aaa;font-weight:650;margin-bottom:2px}.text{font-size:14px;line-height:1.4;color:#eee}
.chat-form{display:flex;gap:7px;padding:9px;border-top:1px solid #252525}
.chat-input{flex:1;min-width:0;height:40px;padding:0 10px;border:1px solid #292929;border-radius:8px;background:#080808;color:#fff;outline:0}
.send{width:42px;border:0;border-radius:8px;background:#fff;color:#000;font-weight:800}
.mobile-chat{position:absolute;inset:0;z-index:20;display:none;pointer-events:none;background:linear-gradient(to top,rgba(0,0,0,.78),rgba(0,0,0,0) 65%)}
.mobile-chat.open{display:block}
.mobile-msgs{position:absolute;left:0;right:0;bottom:55px;max-height:48%;overflow:hidden;padding:10px 12px}
.mobile-msg{font-size:14px;line-height:1.35;margin:5px 0;text-shadow:0 1px 3px #000;word-break:break-word}
.mobile-author{font-weight:750;color:#ddd}.mobile-text{color:#fff}
.mobile-form{position:absolute;left:10px;right:10px;bottom:10px;display:flex;gap:7px;pointer-events:auto}
.mobile-form input{flex:1;min-width:0;height:42px;padding:0 13px;border:1px solid rgba(255,255,255,.16);border-radius:22px;background:rgba(12,12,12,.9);color:#fff;outline:0}
.mobile-send{width:42px;height:42px;border:0;border-radius:50%;background:#fff;color:#000;font-weight:800}
.mobile-close{position:absolute;right:10px;top:10px;width:38px;height:38px;border:1px solid rgba(255,255,255,.15);border-radius:50%;background:rgba(0,0,0,.65);color:#fff;z-index:3;pointer-events:auto}
.footer{height:42px;min-height:42px;display:flex;align-items:center;padding:0 12px;background:#111;border-top:1px solid #252525;color:#777;font-size:12px}
.dot{width:7px;height:7px;border-radius:50%;background:#22c55e;margin-right:7px}.dot.off{background:#ef4444}
.users{margin-left:auto}
.modal{position:fixed;inset:0;z-index:100;display:flex;align-items:center;justify-content:center;padding:20px;background:rgba(0,0,0,.88)}
.modal.hide{display:none}.card{width:min(390px,100%);padding:22px;border:1px solid #292929;border-radius:14px;background:#121212}
.card h2{margin:0 0 7px}.card p{margin:0 0 16px;color:#777;font-size:14px}
.name{width:100%;height:44px;padding:0 11px;border:1px solid #292929;border-radius:9px;background:#080808;color:#fff;outline:0}
.join{width:100%;height:44px;margin-top:8px;border:0;border-radius:9px;background:#fff;color:#000;font-weight:750}
.toast{position:fixed;left:50%;bottom:58px;z-index:200;transform:translate(-50%,10px);opacity:0;transition:.2s;padding:9px 13px;border-radius:8px;background:#fff;color:#000;font-size:13px;pointer-events:none}.toast.show{opacity:1;transform:translate(-50%,0)}
@media(max-width:800px){
 .header{height:50px;min-height:50px}.room{display:none}
 .main{position:relative;display:block}
 .video-section{width:100%;height:100%}
 .player-wrap{aspect-ratio:16/9}
 .desktop-chat{display:none!important}
 .footer{display:none}
}
@media(max-width:900px) and (orientation:landscape){
 .header{display:none}
 .video-section{height:100%}
 .player-wrap{height:100%;aspect-ratio:auto}
 .video-bar{position:absolute;left:0;right:0;bottom:0;z-index:10;opacity:0;background:linear-gradient(transparent,rgba(0,0,0,.9));border:0;transition:.2s}
 .video-section:hover .video-bar,.video-bar:focus-within{opacity:1}
}
.player-wrap:fullscreen,.player-wrap:-webkit-full-screen{width:100vw;height:100vh;background:#000}
.player-wrap:fullscreen #player,.player-wrap:-webkit-full-screen #player{width:100%;height:100%}
.player-wrap:fullscreen .mobile-chat,.player-wrap:-webkit-full-screen .mobile-chat{display:block}
@supports(padding:env(safe-area-inset-bottom)){.mobile-form{bottom:calc(10px + env(safe-area-inset-bottom))}}
</style>
</head>
<body>
<div id="app">
<header class="header">
 <div class="logo">Watch<span>Together</span></div>
 <div class="header-right">
  <div class="room">ROOM: <b id="roomCode">------</b></div>
  <button class="icon" id="copyBtn">🔗</button>
  <button class="icon" id="chatBtn">💬</button>
 </div>
</header>
<main class="main">
<section class="video-section">
 <div class="player-wrap" id="playerWrap">
  <div id="player"></div>
  <div class="placeholder" id="placeholder"><div><b>No video loaded</b><br>Paste a YouTube link below</div></div>
  <div class="mobile-chat" id="mobileChat">
   <button class="mobile-close" id="mobileClose">×</button>
   <div class="mobile-msgs" id="mobileMsgs"></div>
   <form class="mobile-form" id="mobileForm">
    <input id="mobileInput" maxlength="500" autocomplete="off" placeholder="Send a message...">
    <button class="mobile-send">↑</button>
   </form>
  </div>
 </div>
 <div class="video-bar">
  <input class="video-input" id="videoInput" placeholder="Paste YouTube URL..." autocomplete="off">
  <button class="load" id="loadBtn">Load</button>
 </div>
</section>
<aside class="desktop-chat" id="desktopChat">
 <div class="chat-head">
  <div><div class="chat-title">Chat</div><div class="online"><span id="online">0</span> online</div></div>
  <button class="icon" id="closeChat" style="margin-left:auto">→</button>
 </div>
 <div class="messages" id="messages"></div>
 <form class="chat-form" id="chatForm">
  <input class="chat-input" id="chatInput" maxlength="500" autocomplete="off" placeholder="Message...">
  <button class="send">↑</button>
 </form>
</aside>
</main>
<footer class="footer"><span class="dot off" id="dot"></span><span id="status">Connecting...</span><span class="users">👥 <span id="users">0</span></span></footer>
</div>

<div class="modal" id="modal">
 <div class="card">
  <h2>Join room</h2>
  <p>Choose your nickname.</p>
  <input class="name" id="name" maxlength="24" placeholder="Nickname" autocomplete="off">
  <button class="join" id="join">Join</button>
 </div>
</div>
<div class="toast" id="toast"></div>

<script src="https://www.youtube.com/iframe_api"></script>
<script>
"use strict";

const roomId = location.pathname.split("/").filter(Boolean)[1] || "";
document.getElementById("roomCode").textContent = roomId || "------";

let ws = null;
let player = null;
let playerReady = false;
let nickname = "";
let suppressUntil = 0;
let lastPosition = 0;
let lastSeekSent = -999;
let reconnectTimer = null;

const $ = id => document.getElementById(id);

function toast(text){
  $("toast").textContent = text;
  $("toast").classList.add("show");
  clearTimeout(toast.t);
  toast.t = setTimeout(() => $("toast").classList.remove("show"), 2200);
}

function setStatus(ok){
  $("dot").classList.toggle("off", !ok);
  $("status").textContent = ok ? "Connected" : "Disconnected";
}

function send(data){
  if(!ws || ws.readyState !== WebSocket.OPEN) return false;
  ws.send(JSON.stringify(data));
  return true;
}

function addMessage(m){
  const d = document.createElement("div");
  d.className = "msg";
  const a = document.createElement("div");
  a.className = "author";
  a.textContent = m.nickname || "Guest";
  const t = document.createElement("div");
  t.className = "text";
  t.textContent = m.text || "";
  d.append(a,t);
  $("messages").appendChild(d);
  $("messages").scrollTop = $("messages").scrollHeight;

  const md = document.createElement("div");
  md.className = "mobile-msg";
  const ma = document.createElement("span");
  ma.className = "mobile-author";
  ma.textContent = (m.nickname || "Guest") + ": ";
  const mt = document.createElement("span");
  mt.className = "mobile-text";
  mt.textContent = m.text || "";
  md.append(ma,mt);
  $("mobileMsgs").appendChild(md);
  while($("mobileMsgs").children.length > 30) $("mobileMsgs").firstChild.remove();
}

function clearChat(){
  $("messages").innerHTML = "";
  $("mobileMsgs").innerHTML = "";
}

function updateUsers(list){
  const n = Array.isArray(list) ? list.length : 0;
  $("online").textContent = n;
  $("users").textContent = n;
}

function applyState(s){
  updateUsers(s.participants || []);
  clearChat();
  (s.chat || []).forEach(addMessage);

  if(!s.video_id){
    $("placeholder").classList.remove("hide");
    return;
  }

  $("placeholder").classList.add("hide");
  if(!playerReady || !player) return;

  const pos = Number(s.position || 0);
  suppressUntil = Date.now() + 1800;

  try{
    player.cueVideoById({videoId:s.video_id,startSeconds:pos});
  }catch(e){}

  setTimeout(() => {
    if(!player) return;
    suppressUntil = Date.now() + 1500;
    try{
      player.seekTo(pos,true);
      if(s.playing) player.playVideo();
      else player.pauseVideo();
    }catch(e){}
    lastPosition = pos;
  }, 650);
}

function remoteVideo(id){
  if(!playerReady || !player) return;
  $("placeholder").classList.add("hide");
  suppressUntil = Date.now() + 1800;
  try{ player.cueVideoById({videoId:id,startSeconds:0}); }catch(e){}
  lastPosition = 0;
}

function remotePlay(pos){
  if(!playerReady || !player) return;
  suppressUntil = Date.now() + 1400;
  try{
    const local = player.getCurrentTime() || 0;
    if(Math.abs(local-pos) > .8) player.seekTo(pos,true);
    player.playVideo();
  }catch(e){}
  lastPosition = pos;
}

function remotePause(pos){
  if(!playerReady || !player) return;
  suppressUntil = Date.now() + 1400;
  try{
    const local = player.getCurrentTime() || 0;
    if(Math.abs(local-pos) > .8) player.seekTo(pos,true);
    player.pauseVideo();
  }catch(e){}
  lastPosition = pos;
}

function remoteSeek(pos){
  if(!playerReady || !player) return;
  suppressUntil = Date.now() + 1200;
  try{ player.seekTo(pos,true); }catch(e){}
  lastPosition = pos;
}

function handle(d){
  if(!d || !d.type) return;

  if(d.type === "state") applyState(d);
  else if(d.type === "video") remoteVideo(d.video_id);
  else if(d.type === "play") remotePlay(Number(d.position||0));
  else if(d.type === "pause") remotePause(Number(d.position||0));
  else if(d.type === "seek") remoteSeek(Number(d.position||0));
  else if(d.type === "chat") addMessage(d.message);
  else if(d.type === "participants") updateUsers(d.participants || []);
  else if(d.type === "error") toast(d.message || "Error");
}

function connect(){
  if(!roomId || !nickname) return;

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/${encodeURIComponent(roomId)}`);

  ws.onopen = () => {
    setStatus(true);
    send({type:"join",nickname});
  };

  ws.onmessage = e => {
    try{ handle(JSON.parse(e.data)); }catch(err){}
  };

  ws.onerror = () => setStatus(false);

  ws.onclose = () => {
    setStatus(false);
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, 2000);
  };
}

function loadVideo(){
  const value = $("videoInput").value.trim();
  if(!value) return toast("Paste a YouTube URL");
  if(!send({type:"set_video",video:value})) return toast("Not connected");
}

function sendChat(input){
  const text = input.value.trim();
  if(!text) return;
  if(send({type:"chat",text})) input.value = "";
}

function toggleChat(){
  if(window.innerWidth <= 800){
    $("mobileChat").classList.toggle("open");
    if($("mobileChat").classList.contains("open")){
      setTimeout(() => $("mobileInput").focus(), 150);
    }
  }else{
    $("desktopChat").classList.toggle("closed");
  }
}

window.onYouTubeIframeAPIReady = () => createPlayer();

function createPlayer(){
  if(player || !window.YT || !YT.Player) return;
  player = new YT.Player("player",{
    width:"100%",
    height:"100%",
    playerVars:{
      autoplay:0,
      controls:1,
      rel:0,
      playsinline:1,
      modestbranding:1,
      enablejsapi:1
    },
    events:{
      onReady:()=>{
        playerReady=true;
        send({type:"request_state"});
      },
      onStateChange:e=>{
        if(!playerReady || Date.now()<suppressUntil) return;
        let pos=0;
        try{pos=player.getCurrentTime()||0}catch(err){return}

        if(e.data === YT.PlayerState.PLAYING){
          send({type:"play",position:pos});
        }else if(e.data === YT.PlayerState.PAUSED){
          send({type:"pause",position:pos});
        }
        lastPosition=pos;
      }
    }
  });
}

$("join").onclick = () => {
  nickname = ($("name").value.trim() || "Guest").slice(0,24);
  localStorage.setItem("watch_together_name",nickname);
  $("modal").classList.add("hide");
  connect();
};

$("name").addEventListener("keydown",e=>{if(e.key==="Enter") $("join").click()});
$("loadBtn").onclick=loadVideo;
$("videoInput").addEventListener("keydown",e=>{if(e.key==="Enter") loadVideo()});
$("chatBtn").onclick=toggleChat;
$("closeChat").onclick=()=>$("desktopChat").classList.add("closed");
$("mobileClose").onclick=()=>$("mobileChat").classList.remove("open");
$("chatForm").onsubmit=e=>{e.preventDefault();sendChat($("chatInput"))};
$("mobileForm").onsubmit=e=>{e.preventDefault();sendChat($("mobileInput"))};

$("copyBtn").onclick=async()=>{
  try{
    await navigator.clipboard.writeText(location.href);
    toast("Room link copied");
  }catch(e){
    toast(location.href);
  }
};

setInterval(()=>{
  if(!playerReady || !player || !ws || ws.readyState!==WebSocket.OPEN) return;
  if(Date.now()<suppressUntil) return;

  let pos=0,state=0;
  try{
    pos=player.getCurrentTime()||0;
    state=player.getPlayerState();
  }catch(e){return}

  const delta=Math.abs(pos-lastPosition);

  if(delta>1.7 && (state===YT.PlayerState.PLAYING || state===YT.PlayerState.PAUSED)){
    if(Math.abs(pos-lastSeekSent)>1){
      send({type:"seek",position:pos});
      lastSeekSent=pos;
    }
  }

  lastPosition=pos;
},500);

setInterval(()=>{
  if(ws && ws.readyState===WebSocket.OPEN && playerReady) send({type:"sync"});
},4000);

const saved = localStorage.getItem("watch_together_name");
if(saved) $("name").value = saved;

if(!roomId){
  location.href="/";
}else{
  $("modal").classList.remove("hide");
  if(window.YT && window.YT.Player) createPlayer();
}
</script>
</body>
</html>
"""


@app.get("/")
async def index() -> RedirectResponse:
    room_id = make_room_id()
    async with rooms_lock:
        rooms[room_id] = Room(room_id=room_id)
    return RedirectResponse(f"/r/{room_id}", status_code=302)


@app.get("/r/{room_id}", response_class=HTMLResponse)
async def room_page(room_id: str) -> HTMLResponse:
    room_id = room_id.upper()
    if not re.fullmatch(r"[A-Z0-9]{4,20}", room_id):
        return HTMLResponse("<h1>Invalid room</h1>", status_code=400)
    async with rooms_lock:
        rooms.setdefault(room_id, Room(room_id=room_id))
    return HTMLResponse(HTML)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "rooms": len(rooms)}


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str) -> None:
    await websocket.accept()
    room_id = room_id.upper()

    async with rooms_lock:
        room = rooms.setdefault(room_id, Room(room_id=room_id))

    client_id = make_client_id()
    client: Client | None = None

    try:
        first = await websocket.receive_json()

        if first.get("type") != "join":
            await send_json(websocket, {"type": "error", "message": "Join required"})
            await websocket.close()
            return

        nickname = clean_name(first.get("nickname"))
        client = Client(websocket, client_id, nickname)
        room.clients[client_id] = client

        await send_json(websocket, room_state(room))
        await broadcast_users(room)

        while True:
            data = await websocket.receive_json()
            kind = data.get("type")

            if kind in ("request_state", "sync"):
                await send_json(websocket, room_state(room))

            elif kind == "set_video":
                vid = youtube_id(data.get("video"))
                if not vid:
                    await send_json(websocket, {"type":"error","message":"Invalid YouTube URL"})
                    continue

                room.video_id = vid
                room.playing = False
                room.position = 0.0
                room.changed_at = time.monotonic()

                await broadcast(room, {"type":"video","video_id":vid})

            elif kind in ("play", "pause", "seek"):
                if not room.video_id:
                    continue

                try:
                    pos = max(0.0, float(data.get("position", 0)))
                except (TypeError, ValueError):
                    pos = room.position_now()

                if kind == "play":
                    room.position = pos
                    room.playing = True
                    room.changed_at = time.monotonic()
                    payload = {"type":"play","position":round(pos,3)}
                elif kind == "pause":
                    room.position = pos
                    room.playing = False
                    room.changed_at = time.monotonic()
                    payload = {"type":"pause","position":round(pos,3)}
                else:
                    room.position = pos
                    room.changed_at = time.monotonic()
                    payload = {"type":"seek","position":round(pos,3)}

                await broadcast(room, payload, exclude=client_id)

            elif kind == "chat":
                text = str(data.get("text","")).strip()[:500]
                if not text:
                    continue

                message = {
                    "id": secrets.token_hex(8),
                    "nickname": nickname,
                    "text": text,
                    "time": int(time.time()),
                }
                room.chat.append(message)
                room.chat = room.chat[-100:]
                await broadcast(room, {"type":"chat","message":message})

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print("WebSocket error:", repr(exc))
    finally:
        room.clients.pop(client_id, None)
        try:
            await broadcast_users(room)
        except Exception:
            pass
        if not room.clients:
            async with rooms_lock:
                if rooms.get(room_id) is room:
                    rooms.pop(room_id, None)


if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
