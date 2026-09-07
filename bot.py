from __future__ import annotations  
  
import asyncio  
import base64  
import hashlib  
import hmac  
import json  
import os  
import re  
import secrets  
import string  
import time  
from dataclasses import dataclass, field  
from typing import Any  
from urllib.parse import parse_qs, urlsplit  
  
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware  
from fastapi.encoders import jsonable_encoder  
from fastapi.responses import JSONResponse  
from psycopg.rows import dict_row  
from psycopg.types.json import Json  
from psycopg_pool import AsyncConnectionPool  
  
  
app = FastAPI(title="Watch Together API", version="2.0.0")

# The frontend is a separate site / APK. Keep this API CORS-friendly.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["null"],
    allow_origin_regex=r".*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)  
  
ROOM_ID_RE = re.compile(r"^[A-Z0-9]{6}$")  
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")  
ROOM_ALPHABET = string.ascii_uppercase + string.digits  
MAX_NICKNAME_LENGTH = 24  
MAX_MESSAGE_LENGTH = 500  
MAX_CHAT_MESSAGES = 100  
SESSION_COOKIE = "watch_together_session"  
SESSION_MAX_AGE = 60 * 60 * 24 * 30  
DB_URL_ENV = "SUPABASE_DATABASE_URL"  
  
db_pool: AsyncConnectionPool | None = None  
process_session_secret = secrets.token_bytes(32)  
cleanup_task: asyncio.Task[None] | None = None  
  
  
def now() -> float:  
    return time.monotonic()  
  
  
def session_secret() -> bytes:  
    configured = os.environ.get("SESSION_SECRET")  
    return configured.encode("utf-8") if configured else process_session_secret  
  
  
def password_digest(password: str, salt: bytes | None = None) -> str:  
    salt = salt or secrets.token_bytes(16)  
    digest = hashlib.pbkdf2_hmac(  
        "sha256",  
        password.encode("utf-8"),  
        salt,  
        210_000,  
    )  
    return (  
        base64.urlsafe_b64encode(salt).decode("ascii").rstrip("=")  
        + "$"  
        + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")  
    )  
  
  
def password_matches(password: str, stored: str) -> bool:  
    try:  
        salt_text, digest_text = stored.split("$", 1)  
        salt = base64.urlsafe_b64decode(salt_text + "===")  
        expected = password_digest(password, salt).split("$", 1)[1]  
        return hmac.compare_digest(expected, digest_text)  
    except (ValueError, TypeError):  
        return False  
  
  
def make_session(user_id: int) -> str:  
    expires = int(time.time()) + SESSION_MAX_AGE  
    body = f"{user_id}:{expires}".encode("utf-8")  
    encoded = base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")  
    signature = hmac.new(session_secret(), encoded.encode("ascii"), hashlib.sha256).hexdigest()  
    return f"{encoded}.{signature}"  
  
  
def make_room_access_token(room_id: str) -> str:  
    encoded = base64.urlsafe_b64encode(room_id.encode("ascii")).decode("ascii").rstrip("=")  
    return hmac.new(  
        session_secret(), f"room:{encoded}".encode("ascii"), hashlib.sha256  
    ).hexdigest()  
  
  
def valid_room_access_token(room_id: str, token: str | None) -> bool:  
    if not token:  
        return False  
    encoded = base64.urlsafe_b64encode(room_id.encode("ascii")).decode("ascii").rstrip("=")  
    expected = hmac.new(  
        session_secret(), f"room:{encoded}".encode("ascii"), hashlib.sha256  
    ).hexdigest()  
    return hmac.compare_digest(token, expected)  
  
  
def session_user_id(token: str | None) -> int | None:  
    if not token or "." not in token:  
        return None  
    encoded, signature = token.split(".", 1)  
    expected = hmac.new(  
        session_secret(),  
        encoded.encode("ascii"),  
        hashlib.sha256,  
    ).hexdigest()  
    if not hmac.compare_digest(signature, expected):  
        return None  
    try:  
        user_text, expires_text = base64.urlsafe_b64decode(  
            encoded + "==="  
        ).decode("utf-8").split(":", 1)  
        if int(expires_text) < int(time.time()):  
            return None  
        return int(user_text)  
    except (ValueError, UnicodeDecodeError):  
        return None  
  
  
async def db_fetchone(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:  
    if db_pool is None:  
        return None  
    async with db_pool.connection() as connection:  
        async with connection.cursor(row_factory=dict_row) as cursor:  
            await cursor.execute(query, params)  
            result = await cursor.fetchone()  
        await connection.commit()  
        return result  
  
  
async def db_fetchall(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:  
    if db_pool is None:  
        return []  
    async with db_pool.connection() as connection:  
        async with connection.cursor(row_factory=dict_row) as cursor:  
            await cursor.execute(query, params)  
            result = await cursor.fetchall()  
        await connection.commit()  
        return result  
  
  
async def db_execute(query: str, params: tuple[Any, ...] = ()) -> None:  
    if db_pool is None:  
        raise HTTPException(status_code=503, detail="Database is not configured")  
    async with db_pool.connection() as connection:  
        async with connection.cursor() as cursor:  
            await cursor.execute(query, params)  
        await connection.commit()  
  
  
async def initialize_database() -> None:  
    if db_pool is None:  
        return  
    statements = [  
        """  
        CREATE TABLE IF NOT EXISTS wt_users (  
            id BIGSERIAL PRIMARY KEY,  
            login VARCHAR(32) UNIQUE NOT NULL,  
            password_hash TEXT NOT NULL,  
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  
        )  
        """,  
        """  
        CREATE TABLE IF NOT EXISTS wt_rooms (  
            room_id VARCHAR(6) PRIMARY KEY,  
            owner_id BIGINT NOT NULL REFERENCES wt_users(id) ON DELETE CASCADE,  
            name VARCHAR(120) NOT NULL,  
            mode VARCHAR(12) NOT NULL CHECK (mode IN ('private', 'open', 'link')),  
            access_password_hash TEXT,  
            description VARCHAR(500),  
            cover_url TEXT,  
            video_id VARCHAR(11),  
            position DOUBLE PRECISION NOT NULL DEFAULT 0,  
            playing BOOLEAN NOT NULL DEFAULT FALSE,  
            changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  
            empty_since TIMESTAMPTZ DEFAULT NOW(),  
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  
        )  
        """,  
        """  
        CREATE TABLE IF NOT EXISTS wt_room_members (  
            room_id VARCHAR(6) NOT NULL REFERENCES wt_rooms(room_id) ON DELETE CASCADE,  
            user_id BIGINT NOT NULL REFERENCES wt_users(id) ON DELETE CASCADE,  
            role VARCHAR(12) NOT NULL DEFAULT 'viewer',  
            can_control BOOLEAN NOT NULL DEFAULT FALSE,  
            can_manage_users BOOLEAN NOT NULL DEFAULT FALSE,  
            can_manage_admins BOOLEAN NOT NULL DEFAULT FALSE,  
            is_muted BOOLEAN NOT NULL DEFAULT FALSE,  
            is_banned BOOLEAN NOT NULL DEFAULT FALSE,  
            PRIMARY KEY (room_id, user_id)  
        )  
        """,  
        """  
        CREATE TABLE IF NOT EXISTS wt_chat_messages (  
            id BIGSERIAL PRIMARY KEY,  
            room_id VARCHAR(6) NOT NULL REFERENCES wt_rooms(room_id) ON DELETE CASCADE,  
            user_id BIGINT REFERENCES wt_users(id) ON DELETE SET NULL,  
            nickname VARCHAR(24) NOT NULL,  
            text VARCHAR(500) NOT NULL,  
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  
        )  
        """,  
        """  
        CREATE TABLE IF NOT EXISTS wt_polls (  
            id BIGSERIAL PRIMARY KEY,  
            room_id VARCHAR(6) NOT NULL REFERENCES wt_rooms(room_id) ON DELETE CASCADE,  
            created_by BIGINT NOT NULL REFERENCES wt_users(id) ON DELETE CASCADE,  
            question VARCHAR(500) NOT NULL,  
            options JSONB NOT NULL,  
            is_active BOOLEAN NOT NULL DEFAULT TRUE,  
            is_pinned BOOLEAN NOT NULL DEFAULT FALSE,  
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  
        )  
        """,  
        """  
        CREATE TABLE IF NOT EXISTS wt_poll_votes (  
            poll_id BIGINT NOT NULL REFERENCES wt_polls(id) ON DELETE CASCADE,  
            user_id BIGINT NOT NULL REFERENCES wt_users(id) ON DELETE CASCADE,  
            option_index INTEGER NOT NULL,  
            PRIMARY KEY (poll_id, user_id)  
        )  
        """,  
    ]  
    async with db_pool.connection() as connection:  
        async with connection.cursor() as cursor:  
            for statement in statements:  
                await cursor.execute(statement)  
            await cursor.execute(  
                """  
                ALTER TABLE wt_rooms  
                ADD COLUMN IF NOT EXISTS empty_since TIMESTAMPTZ DEFAULT NOW()  
                """  
            )  
            await cursor.execute(  
                "ALTER TABLE wt_rooms ALTER COLUMN empty_since DROP NOT NULL"  
            )  
        await connection.commit()  
  
  
async def cleanup_empty_rooms() -> None:  
    while True:  
        await asyncio.sleep(60)  
        if db_pool is None:  
            continue  
        try:  
            expired = await db_fetchall(  
                """  
                DELETE FROM wt_rooms  
                WHERE empty_since IS NOT NULL  
                  AND empty_since < NOW() - INTERVAL '5 minutes'  
                RETURNING room_id  
                """  
            )  
            for item in expired:  
                rooms.pop(str(item["room_id"]), None)  
        except Exception:  
            continue  
  
  
@app.on_event("startup")  
async def open_database() -> None:  
    global db_pool, cleanup_task  
    connection_string = os.environ.get(DB_URL_ENV)  
    if not connection_string:  
        return  
    db_pool = AsyncConnectionPool(  
        conninfo=connection_string,  
        min_size=1,  
        max_size=5,  
        open=False,  
    )  
    await db_pool.open()  
    await initialize_database()  
    cleanup_task = asyncio.create_task(cleanup_empty_rooms())  
  
  
@app.on_event("shutdown")  
async def close_database() -> None:  
    global cleanup_task  
    if cleanup_task is not None:  
        cleanup_task.cancel()  
        cleanup_task = None  
    if db_pool is not None:  
        await db_pool.close()  
  
  
def bearer_session_token(request: Request) -> str:
    value = str(request.headers.get("Authorization") or "").strip()
    return value[7:].strip() if value.lower().startswith("bearer ") else ""


async def current_user(request: Request) -> dict[str, Any] | None:
    session = request.cookies.get(SESSION_COOKIE) or bearer_session_token(request)
    user_id = session_user_id(session)
    if user_id is None:
        return None
    return await db_fetchone(
        "SELECT id, login, created_at FROM wt_users WHERE id = %s",
        (user_id,),
    )


def websocket_session_token(websocket: WebSocket) -> str:
    auth = str(websocket.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return str(websocket.query_params.get("token") or "").strip()


async def websocket_user(websocket: WebSocket) -> dict[str, Any] | None:
    session = websocket.cookies.get(SESSION_COOKIE) or websocket_session_token(websocket)
    user_id = session_user_id(session)
    if user_id is None:
        return None
    return await db_fetchone(
        "SELECT id, login, created_at FROM wt_users WHERE id = %s",
        (user_id,),
    )


async def room_is_accessible(  
    room_id: str, user_id: int | None, access_token: str | None  
) -> bool:  
    room = await db_fetchone(  
        "SELECT owner_id, mode FROM wt_rooms WHERE room_id = %s", (room_id,)  
    )  
    if not room or room["mode"] != "private":  
        return True  
    if user_id is not None and int(room["owner_id"]) == int(user_id):  
        return True  
    return valid_room_access_token(room_id, access_token)  
  
  
async def room_permission(room_id: str, user_id: int | None) -> dict[str, Any]:  
    if user_id is None:  
        return {  
            "role": "guest",  
            "can_control": False,  
            "can_manage_users": False,  
            "can_manage_admins": False,  
            "is_muted": False,  
            "is_banned": False,  
        }  
    room = await db_fetchone(  
        "SELECT owner_id FROM wt_rooms WHERE room_id = %s",  
        (room_id,),  
    )  
    if not room:  
        return {  
            "role": "guest",  
            "can_control": False,  
            "can_manage_users": False,  
            "can_manage_admins": False,  
            "is_muted": False,  
            "is_banned": False,  
        }  
    if room["owner_id"] == user_id:  
        return {  
            "role": "owner",  
            "can_control": True,  
            "can_manage_users": True,  
            "can_manage_admins": True,  
        }  
    member = await db_fetchone(  
        """  
        SELECT role, can_control, can_manage_users, can_manage_admins,  
               is_muted, is_banned  
        FROM wt_room_members  
        WHERE room_id = %s AND user_id = %s  
        """,  
        (room_id, user_id),  
    )  
    return member or {  
        "role": "viewer",  
        "can_control": False,  
        "can_manage_users": False,  
        "can_manage_admins": False,  
        "is_muted": False,  
        "is_banned": False,  
    }  
  
  
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
    owner_id: int | None = None  
    name: str = "Watch Together room"  
    mode: str = "link"  
    description: str = ""  
    cover_url: str | None = None  
    video_id: str | None = None  
    playing: bool = False  
    position: float = 0.0  
    changed_at: float = field(default_factory=now)  
    clients: dict[str, WebSocket] = field(default_factory=dict)  
    nicknames: dict[str, str] = field(default_factory=dict)  
    client_users: dict[str, int | None] = field(default_factory=dict)  
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
  
    def participants(self) -> list[dict[str, Any]]:  
        return [  
            {  
                "client_id": client_id,  
                "nickname": self.nicknames.get(client_id, "Guest"),  
                "user_id": self.client_users.get(client_id),  
            }  
            for client_id in self.clients  
        ]  
  
    def state_payload(self, message_type: str = "state") -> dict[str, Any]:  
        return {  
            "type": message_type,  
            "room_id": self.room_id,  
            "room": {  
                "name": self.name,  
                "mode": self.mode,  
                "description": self.description,  
                "cover_url": self.cover_url,  
            },  
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
            stored = await db_fetchone(  
                """  
                SELECT room_id, owner_id, name, mode, description, cover_url,  
                       video_id, position, playing  
                FROM wt_rooms  
                WHERE room_id = %s  
                """,  
                (selected_id,),  
            )  
            if stored:  
                room = Room(  
                    room_id=selected_id,  
                    owner_id=stored["owner_id"],  
                    name=stored["name"],  
                    mode=stored["mode"],  
                    description=stored["description"] or "",  
                    cover_url=stored["cover_url"],  
                    video_id=stored["video_id"],  
                    position=float(stored["position"] or 0),  
                    playing=bool(stored["playing"]),  
                )  
                messages = await db_fetchall(  
                    """  
                    SELECT id, user_id, nickname, text,  
                           EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at  
                    FROM wt_chat_messages  
                    WHERE room_id = %s  
                    ORDER BY id DESC  
                    LIMIT 100  
                    """,  
                    (selected_id,),  
                )  
                room.chat = list(reversed(messages))  
            else:  
                room = Room(room_id=selected_id)  
            rooms[selected_id] = room  
        return room  
  
  
async def persist_room_timing(room: Room) -> None:  
    if db_pool is None or room.owner_id is None:  
        return  
    await db_execute(  
        """  
        UPDATE wt_rooms  
        SET video_id = %s, position = %s, playing = %s, changed_at = NOW()  
        WHERE room_id = %s  
        """,  
        (  
            room.video_id,  
            room.position,  
            room.playing,  
            room.room_id,  
        ),  
    )  
  
  
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
  
  
def normalized_login(value: Any) -> str:  
    return " ".join(str(value or "").split()).strip().lower()  
  
  
def valid_room_name(value: Any) -> str:  
    return " ".join(str(value or "").split()).strip()[:120]  
  
  
async def require_user(request: Request) -> dict[str, Any]:  
    user = await current_user(request)  
    if not user:  
        raise HTTPException(status_code=401, detail="Войдите в аккаунт")  
    return user  
  
  
@app.get("/")
async def api_root() -> dict[str, str]:
    return {"name": "Watch Together API", "status": "ok"}


@app.get("/api/me")  
async def api_me(request: Request) -> dict[str, Any]:  
    user = await current_user(request)  
    return {"authenticated": bool(user), "user": user}  
  
  
@app.post("/api/auth/register")  
async def register(request: Request) -> JSONResponse:  
    body = await request.json()  
    login = normalized_login(body.get("login"))  
    password = str(body.get("password") or "")  
    repeat = str(body.get("repeat_password") or body.get("password_repeat") or "")  
    if not re.fullmatch(r"[a-z0-9_.-]{3,32}", login):  
        raise HTTPException(  
            status_code=400,  
            detail="Логин: от 3 до 32 символов, только буквы, цифры, точка, _ или -.",  
        )  
    if len(password) < 6:  
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 6 символов.")  
    if password != repeat:  
        raise HTTPException(status_code=400, detail="Пароли не совпадают.")  
    if db_pool is None:  
        raise HTTPException(status_code=503, detail="База данных не настроена.")  
    try:  
        user = await db_fetchone(  
            """  
            INSERT INTO wt_users (login, password_hash)  
            VALUES (%s, %s)  
            RETURNING id, login, created_at  
            """,  
            (login, password_digest(password)),  
        )  
    except Exception:  
        raise HTTPException(status_code=409, detail="Такой логин уже занят.")  
    session_token = make_session(int(user["id"]))
    response = JSONResponse(jsonable_encoder({"ok": True, "user": user, "session_token": session_token}))  
    response.set_cookie(  
        SESSION_COOKIE,  
        session_token,  
        max_age=SESSION_MAX_AGE,  
        httponly=True,  
        samesite="lax",  
        secure=False,  
    )  
    return response  
  
  
@app.post("/api/auth/login")  
async def login(request: Request) -> JSONResponse:  
    body = await request.json()  
    login_value = normalized_login(body.get("login"))  
    password = str(body.get("password") or "")  
    user = await db_fetchone(  
        "SELECT id, login, password_hash, created_at FROM wt_users WHERE login = %s",  
        (login_value,),  
    )  
    if not user or not password_matches(password, user["password_hash"]):  
        raise HTTPException(status_code=401, detail="Неверный логин или пароль.")  
    user.pop("password_hash", None)  
    session_token = make_session(int(user["id"]))
    response = JSONResponse(jsonable_encoder({"ok": True, "user": user, "session_token": session_token}))  
    response.set_cookie(  
        SESSION_COOKIE,  
        session_token,  
        max_age=SESSION_MAX_AGE,  
        httponly=True,  
        samesite="lax",  
        secure=False,  
    )  
    return response  
  
  
@app.post("/api/auth/logout")  
async def logout() -> JSONResponse:  
    response = JSONResponse({"ok": True})  
    response.delete_cookie(SESSION_COOKIE)  
    return response  
  
  
@app.get("/api/rooms")  
async def list_rooms(request: Request) -> dict[str, Any]:  
    query = " ".join(str(request.query_params.get("q", "")).split())[:120]  
    pattern = f"%{query.lower()}%"  
    rooms_list = await db_fetchall(  
        """  
         SELECT r.room_id, r.name, r.mode, r.description,  
                COALESCE(  
                  r.cover_url,  
                  CASE WHEN r.video_id IS NOT NULL  
                    THEN 'https://img.youtube.com/vi/' || r.video_id || '/hqdefault.jpg'  
                  END  
                ) AS cover_url,  
               r.video_id, r.created_at, u.login AS owner_login,  
               (SELECT COUNT(*) FROM wt_room_members m WHERE m.room_id = r.room_id) AS members  
        FROM wt_rooms r  
        JOIN wt_users u ON u.id = r.owner_id  
         WHERE r.mode = 'open'  
           AND (  
             %s  
            OR LOWER(r.name) LIKE %s  
            OR LOWER(COALESCE(r.description, '')) LIKE %s  
            OR LOWER(COALESCE(r.video_id, '')) LIKE %s  
          )  
        ORDER BY r.created_at DESC  
        LIMIT 100  
        """,  
         (not query, pattern, pattern, pattern),  
    )  
    return {"rooms": rooms_list, "query": query}  
  
  
@app.post("/api/rooms")  
async def create_persistent_room(request: Request) -> JSONResponse:  
    user = await require_user(request)  
    body = await request.json()  
    name = valid_room_name(body.get("name"))  
    mode = str(body.get("mode") or "open").lower()  
    description = str(body.get("description") or "").strip()[:500]  
    cover_url = str(body.get("cover_url") or "").strip()  
    if cover_url and not (  
        cover_url.startswith("https://")  
        or cover_url.startswith("http://")  
        or cover_url.startswith("data:image/")  
    ):  
        raise HTTPException(status_code=400, detail="Обложка должна быть изображением или ссылкой.")  
    if len(cover_url) > 2_000_000:  
        raise HTTPException(status_code=400, detail="Обложка слишком большая.")  
    cover_url = cover_url or None  
    video_id = extract_youtube_video_id(str(body.get("video") or ""))  
    password = str(body.get("password") or "")  
    if not name:  
        raise HTTPException(status_code=400, detail="Укажите название комнаты.")  
    if mode not in {"private", "open", "link"}:  
        raise HTTPException(status_code=400, detail="Неверный режим комнаты.")  
    if mode == "private" and len(password) < 1:  
        raise HTTPException(status_code=400, detail="Для приватной комнаты нужен пароль.")  
    if mode == "link":  
        description = ""  
        cover_url = None  
        password = ""  
  
    room_id = make_room_id()  
    while await db_fetchone("SELECT room_id FROM wt_rooms WHERE room_id = %s", (room_id,)):  
        room_id = make_room_id()  
    await db_execute(  
        """  
        INSERT INTO wt_rooms  
          (room_id, owner_id, name, mode, access_password_hash, description, cover_url, video_id)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)  
        """,  
        (  
            room_id,  
            user["id"],  
            name,  
            mode,  
            password_digest(password) if password else None,  
            description or None,  
            cover_url,  
            video_id,  
        ),  
    )  
    await db_execute(  
        """  
        INSERT INTO wt_room_members  
          (room_id, user_id, role, can_control, can_manage_users, can_manage_admins)  
        VALUES (%s, %s, 'owner', TRUE, TRUE, TRUE)  
        ON CONFLICT (room_id, user_id) DO NOTHING  
        """,  
        (room_id, user["id"]),  
    )  
    return JSONResponse({"ok": True, "room_id": room_id, "url": f"/r/{room_id}"})  
  
  
@app.get("/api/rooms/{room_id}")  
async def room_details(room_id: str, request: Request) -> dict[str, Any]:  
    room_id = room_id.upper()  
    user = await current_user(request)  
    room = await db_fetchone(  
        """  
        SELECT r.room_id, r.name, r.mode, r.description, r.cover_url,  
               r.video_id, r.owner_id, u.login AS owner_login  
        FROM wt_rooms r JOIN wt_users u ON u.id = r.owner_id  
        WHERE r.room_id = %s  
        """,  
        (room_id,),  
    )  
    if not room:  
        raise HTTPException(status_code=404, detail="Комната не найдена.")  
    permission = await room_permission(room_id, int(user["id"]) if user else None)  
    room["permission"] = permission  
    room["is_owner"] = bool(user and room["owner_id"] == user["id"])  
    room["user_login"] = user["login"] if user else None  
    room.pop("owner_id", None)  
    return room  
  
  
@app.post("/api/rooms/{room_id}/access")  
async def access_room(room_id: str, request: Request) -> dict[str, Any]:  
    """Validate a private-room password without exposing its stored hash."""  
    room_id = room_id.upper()  
    body = await request.json()  
    password = str(body.get("password") or "")  
    room = await db_fetchone(  
        "SELECT mode, access_password_hash FROM wt_rooms WHERE room_id = %s",  
        (room_id,),  
    )  
    if not room:  
        raise HTTPException(status_code=404, detail="Комната не найдена.")  
    if room["mode"] != "private":  
        return {"ok": True}  
    if not room["access_password_hash"] or not password_matches(  
        password, room["access_password_hash"]  
    ):  
        raise HTTPException(status_code=403, detail="Неверный пароль комнаты.")  
    response = JSONResponse({"ok": True})  
    response.set_cookie(  
        f"wt_room_access_{room_id}",  
        make_room_access_token(room_id),  
        max_age=SESSION_MAX_AGE,  
        httponly=True,  
        samesite="lax",  
        secure=False,  
    )  
    return response  
  
  
@app.delete("/api/rooms/{room_id}")  
async def delete_room(room_id: str, request: Request) -> dict[str, Any]:  
    user = await require_user(request)  
    room_id = room_id.upper()  
    room = await db_fetchone(  
        "SELECT owner_id FROM wt_rooms WHERE room_id = %s",  
        (room_id,),  
    )  
    if not room:  
        raise HTTPException(status_code=404, detail="Комната не найдена.")  
    if int(room["owner_id"]) != int(user["id"]):  
        raise HTTPException(status_code=403, detail="Удалить комнату может только владелец.")  
    await db_execute("DELETE FROM wt_rooms WHERE room_id = %s", (room_id,))  
    rooms.pop(room_id, None)  
    return {"ok": True}  
  
  
async def require_room_manager(request: Request, room_id: str) -> tuple[dict[str, Any], dict[str, Any]]:  
    user = await require_user(request)  
    permission = await room_permission(room_id, int(user["id"]))  
    if not permission.get("can_manage_users"):  
        raise HTTPException(status_code=403, detail="Недостаточно прав.")  
    return user, permission  
  
  
@app.post("/api/rooms/{room_id}/members")  
async def manage_member(room_id: str, request: Request) -> dict[str, Any]:  
    user, permission = await require_room_manager(request, room_id.upper())  
    body = await request.json()  
    target_id = int(body.get("user_id") or 0)  
    action = str(body.get("action") or "").lower()  
    target = await db_fetchone(  
        "SELECT user_id, role FROM wt_room_members WHERE room_id = %s AND user_id = %s",  
        (room_id.upper(), target_id),  
    )  
    if not target or target_id == user["id"]:  
        raise HTTPException(status_code=404, detail="Участник не найден.")  
    if action in {"set_admin", "remove_admin"} and not permission.get("can_manage_admins"):  
        raise HTTPException(status_code=403, detail="Назначать админов может только владелец.")  
    if action == "ban":  
        await db_execute(  
            "UPDATE wt_room_members SET is_banned = TRUE WHERE room_id = %s AND user_id = %s",  
            (room_id.upper(), target_id),  
        )  
    elif action == "mute":  
        await db_execute(  
            "UPDATE wt_room_members SET is_muted = TRUE WHERE room_id = %s AND user_id = %s",  
            (room_id.upper(), target_id),  
        )  
    elif action == "unmute":  
        await db_execute(  
            "UPDATE wt_room_members SET is_muted = FALSE WHERE room_id = %s AND user_id = %s",  
            (room_id.upper(), target_id),  
        )  
    elif action in {"set_admin", "remove_admin"}:  
        role = "admin" if action == "set_admin" else "viewer"  
        await db_execute(  
            """  
            UPDATE wt_room_members  
            SET role = %s, can_control = %s, can_manage_users = %s  
            WHERE room_id = %s AND user_id = %s  
            """,  
            (role, role == "admin", role == "admin", room_id.upper(), target_id),  
        )  
    elif action.startswith("permission:"):  
        key = action.split(":", 1)[1]  
        if key not in {"can_control", "can_manage_users", "can_manage_admins"}:  
            raise HTTPException(status_code=400, detail="Неизвестное право.")  
        value = bool(body.get("value"))  
        await db_execute(  
            f"UPDATE wt_room_members SET {key} = %s WHERE room_id = %s AND user_id = %s",  
            (value, room_id.upper(), target_id),  
        )  
    else:  
        raise HTTPException(status_code=400, detail="Неизвестное действие.")  
    return {"ok": True}  
  
  
@app.get("/api/rooms/{room_id}/polls")
async def list_polls(room_id: str, request: Request) -> dict[str, Any]:
    user = await current_user(request)
    user_id = int(user["id"]) if user else None
    polls = await db_fetchall(
        """
        SELECT p.id, p.question, p.options, p.is_active, p.is_pinned,
               p.created_at, u.login AS creator_login,
               COALESCE((SELECT COUNT(*) FROM wt_poll_votes v0 WHERE v0.poll_id = p.id), 0)::INTEGER AS total_votes,
               my_vote.option_index AS selected_option,
               COALESCE((
                 SELECT jsonb_agg(
                   jsonb_build_object('option_index', vc.option_index, 'votes', vc.votes)
                   ORDER BY vc.option_index
                 )
                 FROM (
                   SELECT option_index, COUNT(*)::INTEGER AS votes
                   FROM wt_poll_votes v
                   WHERE v.poll_id = p.id
                   GROUP BY option_index
                 ) vc
               ), '[]'::jsonb) AS vote_counts
        FROM wt_polls p
        JOIN wt_users u ON u.id = p.created_by
        LEFT JOIN wt_poll_votes my_vote
          ON my_vote.poll_id = p.id AND my_vote.user_id = %s
        WHERE p.room_id = %s
        GROUP BY p.id, u.login, my_vote.option_index
        ORDER BY p.id DESC
        LIMIT 20
        """,
        (user_id, room_id.upper()),
    )
    for poll in polls:
        counts = [0] * len(poll.get("options") or [])
        for item in poll.get("vote_counts") or []:
            try:
                index = int(item["option_index"])
                if 0 <= index < len(counts):
                    counts[index] = int(item["votes"])
            except (KeyError, TypeError, ValueError):
                continue
        poll["vote_counts"] = counts
        poll["total_votes"] = int(poll.get("total_votes") or 0)
    return {"polls": polls}


async def broadcast_polls(room_id: str) -> None:
    room = rooms.get(room_id.upper())
    if room is None or not room.clients:
        return
    payload = await list_polls_payload(room_id)
    await broadcast(room, {"type": "polls", **payload})


async def list_polls_payload(room_id: str) -> dict[str, Any]:
    # Shared internal implementation used by both HTTP and WebSocket broadcasts.
    polls = await db_fetchall(
        """
        SELECT p.id, p.question, p.options, p.is_active, p.is_pinned, p.created_at,
               u.login AS creator_login,
               COALESCE((SELECT COUNT(*) FROM wt_poll_votes v0 WHERE v0.poll_id = p.id), 0)::INTEGER AS total_votes,
               COALESCE((SELECT jsonb_agg(jsonb_build_object('option_index', vc.option_index, 'votes', vc.votes) ORDER BY vc.option_index)
                         FROM (SELECT option_index, COUNT(*)::INTEGER AS votes FROM wt_poll_votes v WHERE v.poll_id = p.id GROUP BY option_index) vc), '[]'::jsonb) AS vote_counts
        FROM wt_polls p JOIN wt_users u ON u.id = p.created_by
        WHERE p.room_id = %s ORDER BY p.id DESC LIMIT 20
        """, (room_id.upper(),),
    )
    for poll in polls:
        counts = [0] * len(poll.get("options") or [])
        for item in poll.get("vote_counts") or []:
            try:
                i = int(item["option_index"]); counts[i] = int(item["votes"])
            except (KeyError, TypeError, ValueError, IndexError):
                pass
        poll["vote_counts"] = counts
        poll["total_votes"] = int(poll.get("total_votes") or 0)
    return {"polls": polls}


@app.post("/api/rooms/{room_id}/polls")  
async def create_poll(room_id: str, request: Request) -> dict[str, Any]:  
    user = await require_user(request)  
    permission = await room_permission(room_id.upper(), int(user["id"]))  
    if permission.get("role") not in {"owner", "admin"}:  
        raise HTTPException(status_code=403, detail="Создавать голосования могут только владелец и администраторы.")  
    body = await request.json()  
    question = " ".join(str(body.get("question") or "").split()).strip()[:500]  
    options = [  
        " ".join(str(option).split()).strip()[:120]  
        for option in (body.get("options") or [])  
    ]  
    options = [option for option in options if option][:15]  
    if not question or len(options) < 2:  
        raise HTTPException(status_code=400, detail="Нужен вопрос и минимум два варианта.")  
    poll = await db_fetchone(  
        """  
        INSERT INTO wt_polls (room_id, created_by, question, options)  
        VALUES (%s, %s, %s, %s)  
        RETURNING id, question, options, is_active, is_pinned  
        """,  
        (room_id.upper(), user["id"], question, Json(options)),  
    )  
    return {"ok": True, "poll": poll}  
  
  
@app.post("/api/polls/{poll_id}/vote")
async def vote_poll(poll_id: int, request: Request) -> dict[str, Any]:
    user = await require_user(request)
    body = await request.json()
    try:
        option_index = int(body.get("option_index", -1))
    except (TypeError, ValueError):
        option_index = -1
    poll = await db_fetchone(
        "SELECT room_id, options, is_active FROM wt_polls WHERE id = %s",
        (poll_id,),
    )
    if not poll or not poll["is_active"] or option_index < 0 or option_index >= len(poll["options"]):
        raise HTTPException(status_code=400, detail="Голосование недоступно.")
    await db_execute(
        """
        INSERT INTO wt_poll_votes (poll_id, user_id, option_index)
        VALUES (%s, %s, %s)
        ON CONFLICT (poll_id, user_id) DO UPDATE SET option_index = EXCLUDED.option_index
        """,
        (poll_id, user["id"], option_index),
    )
    payload = await list_polls_payload(str(poll["room_id"]))
    room = rooms.get(str(poll["room_id"]).upper())
    if room and room.clients:
        await broadcast(room, {"type": "polls", **payload})
    selected = next((p for p in payload["polls"] if int(p["id"]) == int(poll_id)), None)
    return {"ok": True, "poll": selected}


@app.patch("/api/polls/{poll_id}")  
async def update_poll(poll_id: int, request: Request) -> dict[str, Any]:  
    body = await request.json()  
    poll = await db_fetchone(  
        "SELECT room_id, is_active, is_pinned FROM wt_polls WHERE id = %s",  
        (poll_id,),  
    )  
    if not poll:  
        raise HTTPException(status_code=404, detail="Голосование не найдено.")  
    user = await require_user(request)  
    permission = await room_permission(str(poll["room_id"]), int(user["id"]))  
    if not permission.get("can_manage_users"):  
        raise HTTPException(status_code=403, detail="Недостаточно прав.")  
    action = str(body.get("action") or "").lower()  
    if action == "pin":  
        await db_execute(  
            "UPDATE wt_polls SET is_pinned = TRUE WHERE id = %s", (poll_id,)  
        )  
    elif action == "unpin":  
        await db_execute(  
            "UPDATE wt_polls SET is_pinned = FALSE WHERE id = %s", (poll_id,)  
        )  
    elif action in {"close", "open"}:  
        await db_execute(  
            "UPDATE wt_polls SET is_active = %s WHERE id = %s",  
            (action == "open", poll_id),  
        )  
    else:  
        raise HTTPException(status_code=400, detail="Неизвестное действие.")  
    return {"ok": True}  
  
  
@app.get("/health")
async def health() -> dict[str, str]:
    if db_pool is None:
        return {"status": "ok", "database": "not_configured"}
    try:
        async with db_pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception:
        return {"status": "ok", "database": "error"}

  
  
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
    connected_user = await websocket_user(websocket)  
    connected_user_id = int(connected_user["id"]) if connected_user else None  
    if not await room_is_accessible(  
        room_id,  
        connected_user_id,  
        websocket.cookies.get(f"wt_room_access_{room_id}")
        or websocket.query_params.get("access_token"),  
    ):  
        await send_json(  
            websocket,  
            {"type": "error", "message": "Введите пароль комнаты на странице входа."},  
        )  
        await websocket.close(code=1008, reason="Private room")  
        return  
    permission = await room_permission(room_id, connected_user_id)  
    if permission.get("is_banned"):  
        await send_json(websocket, {"type": "error", "message": "Вы заблокированы в этой комнате."})  
        await websocket.close(code=1008, reason="Banned")  
        return  
    room.clients[client_id] = websocket  
    room.client_users[client_id] = connected_user_id  
    room.nicknames[client_id] = (  
        connected_user["login"] if connected_user else fallback_nickname  
    )  
    if room.owner_id:  
        await db_execute(  
            "UPDATE wt_rooms SET empty_since = NULL WHERE room_id = %s",  
            (room_id,),  
        )  
    if connected_user_id and room.owner_id:  
        await db_execute(  
            """  
            INSERT INTO wt_room_members (room_id, user_id)  
            VALUES (%s, %s)  
            ON CONFLICT (room_id, user_id) DO NOTHING  
            """,  
            (room_id, connected_user_id),  
        )  
  
    initial_state = room.state_payload()  
    initial_state["client_id"] = client_id  
    initial_state["permission"] = permission  
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
                    room.nicknames.get(client_id, fallback_nickname),  
                )  
                await send_json(  
                    websocket,  
                    {  
                        "type": "state",  
                        "client_id": client_id,  
                        "permission": permission,  
                        **room.state_payload(),  
                    },  
                )  
                await broadcast_participants(room)  
  
            elif message_type == "set_video":  
                if room.owner_id and permission.get("role") not in {"owner", "admin"}:  
                    await send_json(  
                        websocket,  
                        {"type": "error", "message": "Менять видео могут только владелец и админы."},  
                    )  
                    continue  
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
                room.save_timing(position=0.0, playing=True)  
                await persist_room_timing(room)  
                await broadcast(  
                    room,  
                    {  
                        "type": "set_video",  
                        "video_id": video_id,  
                        "video": video_id,  
                        "position": 0.0,  
                        "playing": True,  
                    },  
                )  
  
            elif message_type in {"play", "pause"}:  
                if room.owner_id and permission.get("role") not in {"owner", "admin"}:  
                    await send_json(  
                        websocket,  
                        {"type": "error", "message": "Управлять видео могут только владелец и админы."},  
                    )  
                    continue  
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
                await persist_room_timing(room)  
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
                if room.owner_id and permission.get("role") not in {"owner", "admin"}:  
                    await send_json(  
                        websocket,  
                        {"type": "error", "message": "Перематывать могут только владелец и админы."},  
                    )  
                    continue  
                if not room.video_id:  
                    continue  
                room.save_timing(  
                    position=message.get("position", room.current_position())  
                )  
                await persist_room_timing(room)  
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
                if room.owner_id and permission.get("role") not in {"owner", "admin"}:  
                    await send_json(websocket, room.state_payload("sync"))  
                    continue  
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
                    await persist_room_timing(room)  
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
                # Refresh this permission for every message: a mute issued while  
                # the user is already watching must take effect immediately.  
                live_permission = await room_permission(room_id, connected_user_id)  
                if live_permission.get("is_muted"):  
                    await send_json(websocket, {"type": "error", "message": "Вы не можете писать в чат."})  
                    continue  
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
                if room.owner_id:  
                    await db_execute(  
                        """  
                        INSERT INTO wt_chat_messages (room_id, user_id, nickname, text)  
                        VALUES (%s, %s, %s, %s)  
                        """,  
                        (  
                            room.room_id,  
                            connected_user_id,  
                            chat_message["nickname"],  
                            text,  
                        ),  
                    )  
                    await db_execute(  
                        """  
                        DELETE FROM wt_chat_messages  
                        WHERE room_id = %s  
                          AND id NOT IN (  
                            SELECT id FROM wt_chat_messages  
                            WHERE room_id = %s ORDER BY id DESC LIMIT 100  
                          )  
                        """,  
                        (room.room_id, room.room_id),  
                    )  
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
        room.client_users.pop(client_id, None)  
        if room.clients:  
            await broadcast_participants(room)  
        else:  
            if room.owner_id:  
                await db_execute(  
                    "UPDATE wt_rooms SET empty_since = NOW() WHERE room_id = %s",  
                    (room.room_id,),  
                )  
            async with rooms_lock:  
                if rooms.get(room.room_id) is room:  
                    rooms.pop(room.room_id, None)  
  
  

