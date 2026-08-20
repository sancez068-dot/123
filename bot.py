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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool


app = FastAPI(title="Watch Together")

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


async def current_user(request: Request) -> dict[str, Any] | None:
    user_id = session_user_id(request.cookies.get(SESSION_COOKIE))
    if user_id is None:
        return None
    return await db_fetchone(
        "SELECT id, login, created_at FROM wt_users WHERE id = %s",
        (user_id,),
    )


async def websocket_user(websocket: WebSocket) -> dict[str, Any] | None:
    user_id = session_user_id(websocket.cookies.get(SESSION_COOKIE))
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


@app.get("/", response_class=HTMLResponse)
async def lobby_page() -> HTMLResponse:
    return HTMLResponse(LOBBY_TEMPLATE)


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
    response = JSONResponse({"ok": True, "user": user})
    response.set_cookie(
        SESSION_COOKIE,
        make_session(int(user["id"])),
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
    response = JSONResponse({"ok": True, "user": user})
    response.set_cookie(
        SESSION_COOKIE,
        make_session(int(user["id"])),
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
            %s = '%%'
            OR LOWER(r.name) LIKE %s
            OR LOWER(COALESCE(r.description, '')) LIKE %s
            OR LOWER(COALESCE(r.video_id, '')) LIKE %s
          )
        ORDER BY r.created_at DESC
        LIMIT 100
        """,
        (pattern == "%%", pattern, pattern, pattern),
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
async def list_polls(room_id: str) -> dict[str, Any]:
    polls = await db_fetchall(
        """
        SELECT p.id, p.question, p.options, p.is_active, p.is_pinned,
               p.created_at, u.login AS creator_login,
               COUNT(v.user_id)::INTEGER AS votes
        FROM wt_polls p
        JOIN wt_users u ON u.id = p.created_by
        LEFT JOIN wt_poll_votes v ON v.poll_id = p.id
        WHERE p.room_id = %s
        GROUP BY p.id, u.login
        ORDER BY p.id DESC
        LIMIT 20
        """,
        (room_id.upper(),),
    )
    return {"polls": polls}


@app.post("/api/rooms/{room_id}/polls")
async def create_poll(room_id: str, request: Request) -> dict[str, Any]:
    user, _ = await require_room_manager(request, room_id.upper())
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
    option_index = int(body.get("option_index", -1))
    poll = await db_fetchone(
        "SELECT options, is_active FROM wt_polls WHERE id = %s",
        (poll_id,),
    )
    if not poll or not poll["is_active"] or option_index < 0 or option_index >= len(poll["options"]):
        raise HTTPException(status_code=400, detail="Голосование недоступно.")
    await db_execute(
        """
        INSERT INTO wt_poll_votes (poll_id, user_id, option_index)
        VALUES (%s, %s, %s)
        ON CONFLICT (poll_id, user_id) DO NOTHING
        """,
        (poll_id, user["id"], option_index),
    )
    return {"ok": True}


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
    return {"status": "ok"}


@app.get("/r/{room_id}", response_class=HTMLResponse)
async def room_page(room_id: str) -> HTMLResponse:
    room_id = room_id.upper()
    if not ROOM_ID_RE.fullmatch(room_id):
        raise HTTPException(status_code=404, detail="Room not found")
    await get_or_create_room(room_id)
    return HTMLResponse(PAGE_TEMPLATE.replace("ROOM_ID", room_id))


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
        websocket.cookies.get(f"wt_room_access_{room_id}"),
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
                if room.owner_id and not permission.get("can_control"):
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
                room.save_timing(position=0.0, playing=False)
                await persist_room_timing(room)
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
                if room.owner_id and not permission.get("can_control"):
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
                if room.owner_id and not permission.get("can_control"):
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
                if room.owner_id and not permission.get("can_control"):
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


LOBBY_TEMPLATE = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="theme-color" content="#0b0c0e">
  <title>Watch Together</title>
  <style>
    :root { color-scheme: dark; --bg:#0b0c0e; --surface:#14171b; --raised:#1b1f25; --line:#303640; --text:#f1f3f5; --muted:#99a1ad; --accent:#dbe2ea; --danger:#e98f8f; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100dvh; background:var(--bg); color:var(--text); font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; }
    button,input,textarea,select { font:inherit; }
    button { cursor:pointer; }
    button:focus-visible,input:focus-visible,textarea:focus-visible { outline:2px solid #aab4c3; outline-offset:2px; }
    .shell { min-height:100dvh; }
    .topbar { height:66px; display:flex; align-items:center; justify-content:space-between; gap:20px; padding:0 clamp(16px,4vw,52px); border-bottom:1px solid var(--line); background:#0e1013; }
    .brand { display:flex; align-items:center; gap:11px; font-weight:750; }
    .mark { width:29px; height:29px; display:grid; place-items:center; border:1px solid #69727e; border-radius:8px; font-size:11px; }
    .actions { display:flex; align-items:center; gap:8px; }
    .button { min-height:38px; padding:0 13px; border:1px solid var(--line); border-radius:7px; background:var(--raised); color:var(--text); font-size:13px; font-weight:650; }
    .button:hover { border-color:#65707e; background:#272c33; }
    .button.ghost { background:transparent; color:var(--muted); }
    .button.danger { color:#f2b1b1; border-color:#704444; }
    .main { width:min(1220px,100%); margin:0 auto; padding:clamp(25px,5vw,64px) clamp(16px,4vw,52px); }
    .intro { max-width:650px; margin-bottom:30px; }
    .eyebrow { margin:0 0 10px; color:var(--muted); font-size:12px; letter-spacing:.12em; text-transform:uppercase; }
    h1 { margin:0 0 12px; font-size:clamp(30px,5vw,58px); letter-spacing:-.055em; line-height:1; }
    .intro p { margin:0; color:var(--muted); font-size:15px; line-height:1.6; }
    .toolbar { display:flex; gap:9px; margin-bottom:22px; }
    .input,.textarea,.select { width:100%; min-height:40px; padding:0 12px; border:1px solid var(--line); border-radius:7px; color:var(--text); background:var(--surface); }
    .textarea { min-height:88px; padding-top:10px; resize:vertical; }
    .input::placeholder,.textarea::placeholder { color:#747d89; }
    .input:focus,.textarea:focus,.select:focus { border-color:#6c7683; outline:none; }
    .rooms-heading { display:flex; justify-content:space-between; align-items:center; gap:12px; margin:0 0 12px; }
    .rooms-heading h2 { margin:0; font-size:16px; }
    .room-count { color:var(--muted); font-size:12px; }
    .rooms { display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:11px; }
    .room-card { min-height:190px; display:flex; flex-direction:column; justify-content:space-between; padding:15px; border:1px solid var(--line); border-radius:9px; background:var(--surface); transition:transform .18s,border-color .18s; }
    .room-card:hover { transform:translateY(-2px); border-color:#59636f; }
    .room-cover { height:72px; margin:-15px -15px 14px; border-radius:8px 8px 0 0; background:#20252c center/cover no-repeat; }
    .room-name { margin:0 0 7px; font-size:16px; }
    .room-description { min-height:38px; margin:0 0 13px; color:var(--muted); font-size:12px; line-height:1.45; }
    .room-meta { display:flex; justify-content:space-between; gap:8px; color:#7f8895; font-size:11px; }
    .empty { padding:38px 15px; border:1px dashed var(--line); border-radius:9px; color:var(--muted); text-align:center; font-size:13px; }
    .hidden { display:none !important; }
    .modal-backdrop { position:fixed; inset:0; z-index:10; display:grid; place-items:center; padding:18px; background:rgba(4,5,7,.82); backdrop-filter:blur(8px); }
    .modal-backdrop.hidden { display:none; }
    .modal { width:min(520px,100%); max-height:calc(100dvh - 36px); overflow:auto; padding:22px; border:1px solid #47515e; border-radius:10px; background:var(--surface); box-shadow:0 24px 70px rgba(0,0,0,.45); }
    .modal-head { display:flex; justify-content:space-between; gap:14px; align-items:flex-start; margin-bottom:18px; }
    .modal h2 { margin:0 0 6px; font-size:21px; letter-spacing:-.03em; }
    .modal-copy { margin:0; color:var(--muted); font-size:12px; line-height:1.5; }
    .close { width:30px; height:30px; border:1px solid var(--line); border-radius:6px; color:var(--muted); background:transparent; }
    .form { display:grid; gap:11px; }
    .label { display:grid; gap:6px; color:#b8c0ca; font-size:12px; }
    .mode-options { display:grid; grid-template-columns:repeat(3,1fr); gap:7px; }
    .mode-option { display:flex; align-items:center; gap:6px; padding:9px; border:1px solid var(--line); border-radius:7px; color:var(--muted); font-size:12px; }
    .mode-option:has(input:checked) { border-color:#788391; color:var(--text); background:#252a31; }
    .mode-option input { accent-color:#cbd3dd; }
    .form-actions { display:flex; justify-content:flex-end; gap:8px; margin-top:5px; }
    .auth-tabs { display:flex; gap:3px; margin-bottom:15px; padding:3px; border-radius:7px; background:#0e1013; }
    .auth-tab { flex:1; min-height:34px; border:0; border-radius:5px; color:var(--muted); background:transparent; font-size:12px; }
    .auth-tab.active { color:var(--text); background:var(--raised); }
    .error { min-height:16px; color:var(--danger); font-size:12px; }
    @media (max-width:620px) {
      .topbar { height:auto; min-height:58px; padding:10px 14px; }
      .brand-name { font-size:13px; }
      .actions .button { padding:0 9px; font-size:12px; }
      .main { padding:28px 14px; }
      .toolbar { flex-direction:column; }
      .mode-options { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand"><div class="mark">WT</div><span>Watch Together</span></div>
      <div class="actions" id="auth-actions">
        <button class="button ghost" id="login-button" type="button">Войти / Создать аккаунт</button>
      </div>
    </header>
    <main class="main">
      <section class="intro">
        <p class="eyebrow">Shared rooms</p>
        <h1>Смотри вместе.</h1>
        <p>Открытые комнаты с общим видео, живым чатом и синхронным просмотром.</p>
      </section>
      <div class="toolbar">
        <input class="input" id="room-search" type="search" placeholder="Найти по названию, описанию или видео">
        <button class="button" id="search-button" type="button">Найти</button>
      </div>
      <div class="rooms-heading"><h2>Открытые комнаты</h2><span class="room-count" id="room-count"></span></div>
      <section class="rooms" id="rooms"><div class="empty">Загрузка комнат…</div></section>
    </main>
  </div>

  <div class="modal-backdrop hidden" id="auth-modal">
    <div class="modal">
      <div class="modal-head"><div><h2 id="auth-title">Войти в аккаунт</h2><p class="modal-copy">Создайте аккаунт, чтобы открывать комнаты и управлять ими.</p></div><button class="close" data-close="auth-modal" type="button">×</button></div>
      <div class="auth-tabs"><button class="auth-tab active" id="login-tab" type="button">Войти</button><button class="auth-tab" id="register-tab" type="button">Создать аккаунт</button></div>
      <form class="form" id="auth-form">
        <label class="label">Логин<input class="input" id="auth-login" maxlength="32" required></label>
        <label class="label">Пароль<input class="input" id="auth-password" type="password" minlength="6" required></label>
        <label class="label hidden" id="repeat-label">Повторите пароль<input class="input" id="auth-repeat" type="password" minlength="6"></label>
        <div class="error" id="auth-error"></div>
        <div class="form-actions"><button class="button" type="submit" id="auth-submit">Войти</button></div>
      </form>
    </div>
  </div>

  <div class="modal-backdrop hidden" id="create-modal">
    <div class="modal">
      <div class="modal-head"><div><h2>Создать комнату</h2><p class="modal-copy">Выберите, кто сможет найти и открыть комнату.</p></div><button class="close" data-close="create-modal" type="button">×</button></div>
      <form class="form" id="create-form">
        <label class="label">Название комнаты<input class="input" id="create-name" maxlength="120" required></label>
        <div class="label">Режим комнаты
          <div class="mode-options">
            <label class="mode-option"><input type="radio" name="mode" value="private"> Приватная</label>
            <label class="mode-option"><input type="radio" name="mode" value="open" checked> Открытая</label>
            <label class="mode-option"><input type="radio" name="mode" value="link"> По ссылке</label>
          </div>
        </div>
        <label class="label" id="password-field">Пароль<input class="input" id="create-password" type="password" placeholder="Необязательно для открытой"></label>
        <label class="label" id="description-field">Описание<textarea class="textarea" id="create-description" maxlength="500"></textarea></label>
         <label class="label" id="cover-field">Обложка<input class="input" id="create-cover" type="file" accept="image/*"></label>
        <label class="label" id="video-field">Видео YouTube URL или ID<input class="input" id="create-video"></label>
        <div class="error" id="create-error"></div>
        <div class="form-actions"><button class="button ghost" data-close="create-modal" type="button">Отмена</button><button class="button" type="submit">Создать</button></div>
      </form>
    </div>
  </div>

  <script>
    (() => {
      const rooms = document.getElementById("rooms");
      const count = document.getElementById("room-count");
      const authActions = document.getElementById("auth-actions");
      const authModal = document.getElementById("auth-modal");
      const createModal = document.getElementById("create-modal");
      const authForm = document.getElementById("auth-form");
      const authError = document.getElementById("auth-error");
      const createError = document.getElementById("create-error");
      let authMode = "login";
      let me = null;

      const $ = (id) => document.getElementById(id);
      const open = (node) => node.classList.remove("hidden");
      const close = (node) => node.classList.add("hidden");
       const showError = (node, error) => node.textContent = error?.detail || error?.message || "Произошла ошибка.";

      async function api(url, options = {}) {
        const response = await fetch(url, { headers: {"Content-Type":"application/json"}, ...options });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw body;
        return body;
      }

      function roomCard(room) {
        const card = document.createElement("article");
        card.className = "room-card";
        if (room.cover_url) {
          const cover = document.createElement("div");
          cover.className = "room-cover";
          cover.style.backgroundImage = `url("${room.cover_url.replaceAll('"', "")}")`;
          card.appendChild(cover);
        }
        const content = document.createElement("div");
        const title = document.createElement("h3");
        title.className = "room-name";
        title.textContent = room.name;
        const description = document.createElement("p");
        description.className = "room-description";
        description.textContent = room.description || "Без описания";
        content.append(title, description);
        const footer = document.createElement("div");
        footer.className = "room-meta";
        const owner = document.createElement("span");
        owner.textContent = `@${room.owner_login || "owner"}`;
        const members = document.createElement("span");
        members.textContent = `${room.members || 0} смотрят`;
        footer.append(owner, members);
        const action = document.createElement("button");
        action.className = "button";
        action.type = "button";
        action.textContent = "Открыть";
        action.addEventListener("click", () => window.location.href = `/r/${room.room_id}`);
        card.append(content, footer, action);
        return card;
      }

      async function loadRooms() {
        const query = $("room-search").value.trim();
        const result = await api(`/api/rooms?q=${encodeURIComponent(query)}`);
        rooms.replaceChildren();
        result.rooms.forEach((room) => rooms.appendChild(roomCard(room)));
        count.textContent = `${result.rooms.length} комнат`;
        if (!result.rooms.length) {
          const empty = document.createElement("div");
          empty.className = "empty";
          empty.textContent = query ? "Ничего не найдено." : "Открытых комнат пока нет.";
          rooms.appendChild(empty);
        }
      }

      function renderAuth(user) {
        me = user;
        authActions.replaceChildren();
        if (!user) {
          const login = document.createElement("button");
          login.className = "button ghost";
          login.textContent = "Войти / Создать аккаунт";
          login.addEventListener("click", () => open(authModal));
          authActions.appendChild(login);
          return;
        }
        const greeting = document.createElement("span");
        greeting.style.color = "var(--muted)";
        greeting.style.fontSize = "12px";
        greeting.textContent = `@${user.login}`;
        const create = document.createElement("button");
        create.className = "button";
        create.textContent = "Создать комнату";
        create.addEventListener("click", () => open(createModal));
        const logout = document.createElement("button");
        logout.className = "button ghost";
        logout.textContent = "Выйти";
        logout.addEventListener("click", async () => { await api("/api/auth/logout", {method:"POST"}); renderAuth(null); });
        authActions.append(greeting, create, logout);
      }

      async function loadMe() {
        const result = await api("/api/me");
        renderAuth(result.user);
      }

      function setAuthMode(mode) {
        authMode = mode;
        $("login-tab").classList.toggle("active", mode === "login");
        $("register-tab").classList.toggle("active", mode === "register");
        $("auth-title").textContent = mode === "login" ? "Войти в аккаунт" : "Создать аккаунт";
        $("auth-submit").textContent = mode === "login" ? "Войти" : "Создать аккаунт";
        $("repeat-label").classList.toggle("hidden", mode !== "register");
        $("auth-repeat").required = mode === "register";
        authError.textContent = "";
      }

      document.querySelectorAll("[data-close]").forEach((button) => button.addEventListener("click", () => close($(button.dataset.close))));
      $("login-button").addEventListener("click", () => open(authModal));
      $("login-tab").addEventListener("click", () => setAuthMode("login"));
      $("register-tab").addEventListener("click", () => setAuthMode("register"));
      $("search-button").addEventListener("click", loadRooms);
      $("room-search").addEventListener("keydown", (event) => { if (event.key === "Enter") loadRooms(); });

      authForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        authError.textContent = "";
        const payload = {login:$("auth-login").value, password:$("auth-password").value};
        if (authMode === "register") payload.repeat_password = $("auth-repeat").value;
        try {
          const result = await api(`/api/auth/${authMode === "login" ? "login" : "register"}`, {method:"POST", body:JSON.stringify(payload)});
          renderAuth(result.user);
          close(authModal);
          authForm.reset();
        } catch (error) { showError(authError, error); }
      });

      function updateModeFields() {
        const mode = document.querySelector('input[name="mode"]:checked').value;
        $("password-field").style.display = mode === "private" ? "grid" : "none";
        $("description-field").style.display = mode === "link" ? "none" : "grid";
        $("cover-field").style.display = mode === "link" ? "none" : "grid";
        $("video-field").style.display = mode === "link" ? "none" : "grid";
        $("create-password").required = mode === "private";
      }
      document.querySelectorAll('input[name="mode"]').forEach((input) => input.addEventListener("change", updateModeFields));
      updateModeFields();

       $("create-form").addEventListener("submit", async (event) => {
        event.preventDefault();
        createError.textContent = "";
        const mode = document.querySelector('input[name="mode"]:checked').value;
        try {
           const coverFile = $("create-cover").files[0];
           const cover_url = coverFile
             ? await new Promise((resolve, reject) => {
                 const reader = new FileReader();
                 reader.onload = () => resolve(reader.result);
                 reader.onerror = () => reject(new Error("Не удалось прочитать обложку."));
                 reader.readAsDataURL(coverFile);
               })
             : "";
          const result = await api("/api/rooms", {method:"POST", body:JSON.stringify({
            name:$("create-name").value, mode, password:$("create-password").value,
             description:$("create-description").value, cover_url,
            video:$("create-video").value
          })});
          window.location.href = result.url;
        } catch (error) { showError(createError, error); }
      });

      Promise.all([loadMe(), loadRooms()]).catch((error) => showError(rooms, error));
      window.setInterval(loadRooms, 30000);
    })();
  </script>
</body>
</html>
"""


PAGE_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#0b0c0e">
  <title>Watch Together · Room ROOM_ID</title>
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
    .management-panel {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }
    .management-panel summary { cursor: pointer; color: var(--text); font-size: 13px; font-weight: 700; }
    .management-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 13px; }
    .management-section { display: grid; gap: 8px; }
    .management-title { margin: 0; color: var(--muted); font-size: 11px; letter-spacing: .08em; text-transform: uppercase; }
    .member-row, .poll-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 8px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); font-size: 12px; }
    .member-actions { display: flex; flex-wrap: wrap; gap: 5px; justify-content: flex-end; }
    .mini-button { min-height: 28px; padding: 0 8px; border: 1px solid var(--line-strong); border-radius: 5px; color: var(--text); background: var(--surface-soft); font-size: 11px; }
    .poll-options { display: grid; gap: 5px; }
    .poll-option { display: flex; gap: 7px; align-items: center; padding: 6px 0; color: var(--muted); font-size: 12px; }
    .poll-option input { accent-color: #b8c2cf; }
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
        <span class="room-code" id="room-code">ROOM_ID</span>
        <button class="button subtle" id="copy-room" type="button">Copy link</button>
        <button class="button subtle danger" id="owner-delete" type="button" hidden>Delete room</button>
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
      <details class="management-panel hidden" id="management-panel">
        <summary>Участники и голосования</summary>
        <div class="management-grid">
          <section class="management-section">
            <h3 class="management-title">Сейчас смотрят</h3>
            <div id="participant-admin-list"></div>
          </section>
          <section class="management-section">
            <h3 class="management-title">Новое голосование</h3>
            <form id="poll-form">
              <input class="input" id="poll-question" maxlength="500" placeholder="Вопрос" required>
              <textarea class="input" id="poll-options" rows="5" maxlength="1800" placeholder="Вариант 1&#10;Вариант 2&#10;До 15 вариантов" required></textarea>
              <button class="button" type="submit">Создать голосование</button>
            </form>
            <div id="poll-list"></div>
          </section>
        </div>
      </details>
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

      const roomId = "ROOM_ID";
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
      const ownerDelete = document.getElementById("owner-delete");
      const toast = document.getElementById("toast");
      const managementPanel = document.getElementById("management-panel");
      const participantAdminList = document.getElementById("participant-admin-list");
      const pollForm = document.getElementById("poll-form");
      const pollList = document.getElementById("poll-list");

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
      let roomAccessReady = true;
      let currentPermission = {};
      let currentParticipants = [];

      fetch(`/api/rooms/${roomId}`)
        .then((response) => response.ok ? response.json() : null)
        .then(async (room) => {
          if (!room) return;
          if (room.is_owner) ownerDelete.hidden = false;
          if (room.mode === "private" && !room.is_owner) {
            roomAccessReady = false;
            let password = window.prompt("Введите пароль приватной комнаты:");
            if (!password) {
              setConnection("error", "Нужен пароль комнаты");
              return;
            }
            try {
              const response = await fetch(`/api/rooms/${roomId}/access`, {
                method: "POST",
                headers: {"Content-Type":"application/json"},
                body: JSON.stringify({password})
              });
              const result = await response.json().catch(() => ({}));
              if (!response.ok) throw result;
              roomAccessReady = true;
              if (nickname) connect();
            } catch (error) {
              setConnection("error", error.detail || "Неверный пароль комнаты");
              showToast(error.detail || "Неверный пароль комнаты");
            }
          }
        })
        .catch(() => {});

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
        if (!nickname || !roomAccessReady) return;
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
        currentParticipants = list;
        onlineCount.textContent = `${list.length} online`;
        renderParticipantAdminList();
      }

      function renderParticipantAdminList() {
        participantAdminList.replaceChildren();
        currentParticipants.forEach((participant) => {
          if (!participant.user_id) return;
          const row = document.createElement("div");
          row.className = "member-row";
          const label = document.createElement("span");
          label.textContent = participant.nickname;
          const actions = document.createElement("div");
          actions.className = "member-actions";
          const action = (text, name) => {
            const button = document.createElement("button");
            button.className = "mini-button";
            button.type = "button";
            button.textContent = text;
            button.addEventListener("click", async () => {
              try {
                await fetch(`/api/rooms/${roomId}/members`, {
                  method: "POST",
                  headers: {"Content-Type":"application/json"},
                  body: JSON.stringify({user_id: participant.user_id, action: name})
                }).then(async (response) => {
                  const result = await response.json().catch(() => ({}));
                  if (!response.ok) throw result;
                });
                showToast("Готово");
              } catch (error) {
                showToast(error.detail || "Недостаточно прав");
              }
            });
            actions.appendChild(button);
          };
          if (currentPermission.can_manage_users) {
            action("Мут чата", "mute");
            action("Размутить", "unmute");
            action("Бан", "ban");
          }
          if (currentPermission.can_manage_admins) {
            action("Назначить админом", "set_admin");
            action("Снять админа", "remove_admin");
            const permissions = [
              ["Дать управление видео", "permission:can_control"],
              ["Забрать управление видео", "permission:can_control"],
              ["Разрешить назначать админов", "permission:can_manage_admins"],
              ["Забрать право назначения", "permission:can_manage_admins"]
            ];
            permissions.forEach(([text, name], index) => {
              const button = document.createElement("button");
              button.className = "mini-button";
              button.type = "button";
              button.textContent = text;
              button.addEventListener("click", async () => {
                await fetch(`/api/rooms/${roomId}/members`, {
                  method: "POST",
                  headers: {"Content-Type":"application/json"},
                  body: JSON.stringify({
                    user_id: participant.user_id,
                    action: name,
                    value: index % 2 === 0
                  })
                });
                showToast("Права обновлены");
              });
              actions.appendChild(button);
            });
          }
          row.append(label, actions);
          participantAdminList.appendChild(row);
        });
        if (!participantAdminList.children.length) {
          participantAdminList.textContent = "Нет зарегистрированных зрителей.";
        }
      }

      async function loadPolls() {
        const response = await fetch(`/api/rooms/${roomId}/polls`);
        if (!response.ok) return;
        const result = await response.json();
        pollList.replaceChildren();
        (result.polls || []).forEach((poll) => {
          const row = document.createElement("div");
          row.className = "poll-row";
          const form = document.createElement("form");
          form.className = "poll-options";
          const title = document.createElement("strong");
          title.textContent = poll.question;
          form.appendChild(title);
          poll.options.forEach((option, index) => {
            const label = document.createElement("label");
            label.className = "poll-option";
            label.innerHTML = `<input type="radio" name="poll-${poll.id}" value="${index}"> ${option}`;
            form.appendChild(label);
          });
          const vote = document.createElement("button");
          vote.className = "mini-button";
          vote.type = "submit";
          vote.textContent = "Проголосовать";
          form.appendChild(vote);
          form.addEventListener("submit", async (event) => {
            event.preventDefault();
            const selected = form.querySelector("input:checked");
            if (!selected) return;
            const response = await fetch(`/api/polls/${poll.id}/vote`, {
              method: "POST",
              headers: {"Content-Type":"application/json"},
              body: JSON.stringify({option_index: Number(selected.value)})
            });
            showToast(response.ok ? "Голос принят" : "Голос уже нельзя изменить");
            if (response.ok) loadPolls();
          });
          row.appendChild(form);
          if (currentPermission.can_manage_users) {
            const toggle = document.createElement("button");
            toggle.className = "mini-button";
            toggle.type = "button";
            toggle.textContent = poll.is_pinned ? "Открепить" : "Закрепить";
            toggle.addEventListener("click", async () => {
              await fetch(`/api/polls/${poll.id}`, {
                method: "PATCH",
                headers: {"Content-Type":"application/json"},
                body: JSON.stringify({action: poll.is_pinned ? "unpin" : "pin"})
              });
              loadPolls();
            });
            row.appendChild(toggle);
          }
          pollList.appendChild(row);
        });
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
            currentPermission = message.permission || {};
            managementPanel.classList.toggle(
              "hidden",
              !currentPermission.can_manage_users
            );
            updateParticipants(message.participants);
            renderChat(message.chat);
            loadPolls();
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

      pollForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (!currentPermission.can_manage_users) return;
        const options = $("poll-options").value
          .split("\n")
          .map((option) => option.trim())
          .filter(Boolean)
          .slice(0, 15);
        try {
          const response = await fetch(`/api/rooms/${roomId}/polls`, {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({
              question: $("poll-question").value,
              options
            })
          });
          const result = await response.json().catch(() => ({}));
          if (!response.ok) throw result;
          pollForm.reset();
          loadPolls();
          showToast("Голосование создано");
        } catch (error) {
          showToast(error.detail || "Не удалось создать голосование");
        }
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

      ownerDelete.addEventListener("click", async () => {
        if (!window.confirm("Удалить комнату без возможности восстановления?")) return;
        try {
          const response = await fetch(`/api/rooms/${roomId}`, { method: "DELETE" });
          if (!response.ok) {
            const error = await response.json().catch(() => ({}));
            showToast(error.detail || "Удалить комнату может только владелец.");
            return;
          }
          window.location.href = "/";
        } catch {
          showToast("Не удалось удалить комнату.");
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
    uvicorn.run("main:app", host="0.0.0.0", port=port)

