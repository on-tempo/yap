from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from database import engine, SessionLocal
import models
import asyncio
import json
import os
import redis.asyncio as redis
import time

app = FastAPI()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

ROOM_CHANNEL = "yap:rooms"
PRESENCE_TTL = 30        # seconds before a stale entry expires
HEARTBEAT_INTERVAL = 10  # refresh well before the TTL runs out
RATE_LIMIT = 10          # max messages per window
RATE_WINDOW = 10         # seconds

models.Base.metadata.create_all(bind=engine)

# {username: [connection, connection, ...]}
# one user can have multiple tabs/devices open
connections: defaultdict[str, list[WebSocket]] = defaultdict(list)


def get_room_members(db, room_name: str) -> set[str]:
    """All usernames that have joined this room."""
    rows = (
        db.query(models.RoomMember.username)
        .filter(models.RoomMember.room == room_name)
        .all()
    )
    return {row[0] for row in rows}


def is_room_member(db, room_name: str, username: str) -> bool:
    """Whether this user has joined the room."""
    return (
        db.query(models.RoomMember)
        .filter(
            models.RoomMember.room == room_name,
            models.RoomMember.username == username,
        )
        .first()
        is not None
    )


def count_unread(db, room_name: str, username: str) -> int:
    """Messages in this room newer than the user's last read marker."""
    record = (
        db.query(models.RoomRead)
        .filter(
            models.RoomRead.room == room_name,
            models.RoomRead.username == username,
        )
        .first()
    )
    last_read_id = record.last_read_id if record else 0

    return (
        db.query(models.Message)
        .filter(
            models.Message.room == room_name,
            models.Message.id > last_read_id,
        )
        .count()
    )


async def is_rate_limited(username: str) -> bool:
    """Return True if this user has exceeded the message limit.

    INCR is atomic, so concurrent messages from the same user can never
    read the same counter value.
    """
    window = int(time.time()) // RATE_WINDOW
    key = f"yap:rate:{username}:{window}"

    count = await redis_client.incr(key)
    if count == 1:
        # first message in this window — make sure the key disappears with it
        await redis_client.expire(key, RATE_WINDOW)

    return count > RATE_LIMIT


async def redis_listener():
    """Listen to the shared channel and fan out to this instance's connections."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(ROOM_CHANNEL)

    async for event in pubsub.listen():
        if event["type"] != "message":
            continue                      # skip subscribe confirmations

        payload = json.loads(event["data"])

        msg_type = payload["type"]
        sender = payload["sender"]
        message = payload["content"]

        if msg_type == "room":
            room_name = payload["room"]

            db = SessionLocal()
            try:
                members = get_room_members(db, room_name)
            finally:
                db.close()

            for user in members:
                for conn in connections.get(user, []):
                    await conn.send_text(f"[{room_name}] {sender}: {message}")

        elif msg_type == "dm":
            target = payload["target"]

            # receiver
            for conn in connections.get(target, []):
                await conn.send_text(f"[DM] {sender}: {message}")

            # sender echo
            for conn in connections.get(sender, []):
                await conn.send_text(f"[DM to {target}] {sender}: {message}")


async def presence_heartbeat():
    """Refresh presence keys for everyone connected to this instance.

    A crashed server stops refreshing, so its entries expire on their own.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        for user in list(connections.keys()):
            await redis_client.set(f"yap:online:{user}", "1", ex=PRESENCE_TTL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(redis_listener())
    asyncio.create_task(presence_heartbeat())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    # add to this user's list
    connections[username].append(websocket)
    # mark this user online; the key expires unless a heartbeat refreshes it
    await redis_client.set(f"yap:online:{username}", "1", ex=PRESENCE_TTL)

    try:
        while True:
            data = await websocket.receive_text()

            if await is_rate_limited(username):
                for conn in connections[username]:
                    await conn.send_text("[system] slow down — you are sending too fast")
                continue

            if data.startswith("/read"):
                # ---- read receipt path ----
                # data looks like: "/read room1"
                parts = data.split(" ", 1)
                if len(parts) < 2:
                    for conn in connections[username]:
                        await conn.send_text("[system] usage: /read room_name")
                    continue

                room_name = parts[1]

                db = SessionLocal()
                try:
                    # newest message id in this room (None if the room is empty)
                    latest = (
                        db.query(models.Message.id)
                        .filter(models.Message.room == room_name)
                        .order_by(models.Message.id.desc())
                        .first()
                    )
                    latest_id = latest[0] if latest else 0

                    # upsert: one row per (room, user)
                    record = (
                        db.query(models.RoomRead)
                        .filter(
                            models.RoomRead.room == room_name,
                            models.RoomRead.username == username,
                        )
                        .first()
                    )
                    if record:
                        record.last_read_id = latest_id
                    else:
                        db.add(models.RoomRead(
                            room=room_name,
                            username=username,
                            last_read_id=latest_id,
                        ))
                    db.commit()
                finally:
                    db.close()

                for conn in connections[username]:
                    await conn.send_text(f"[system] marked {room_name} as read")

            elif data.startswith("/unread"):
                # ---- unread count path ----
                # data looks like: "/unread"  (all rooms this user has joined)
                db = SessionLocal()
                try:
                    my_rooms = (
                        db.query(models.RoomMember.room)
                        .filter(models.RoomMember.username == username)
                        .all()
                    )

                    lines = []
                    for (room_name,) in my_rooms:
                        count = count_unread(db, room_name, username)
                        lines.append(f"{room_name}: {count} unread")
                finally:
                    db.close()

                summary = " | ".join(lines) if lines else "no rooms joined"
                for conn in connections[username]:
                    await conn.send_text(f"[system] {summary}")

            elif data.startswith("/join"):
                # ---- join path ----
                # data looks like: "/join room1"
                parts = data.split(" ", 1)
                if len(parts) < 2:
                    # malformed join like "/join" with no room name
                    # -> tell ONLY the sender how to use it, then skip the rest
                    for conn in connections[username]:
                        await conn.send_text("[system] usage: /join room_name")
                    continue

                room_name = parts[1]

                db = SessionLocal()
                try:
                    # a set used to prevent duplicates for free; now we check explicitly
                    if not is_room_member(db, room_name, username):
                        db.add(models.RoomMember(room=room_name, username=username))
                        db.commit()

                    # newest 20 messages in this room
                    recent = (
                        db.query(models.Message)
                        .filter(models.Message.room == room_name)
                        .order_by(models.Message.created_at.desc())
                        .limit(20)
                        .all()
                    )
                    unread = count_unread(db, room_name, username)
                    members = get_room_members(db, room_name)
                finally:
                    db.close()

                # fetched newest-first, but display oldest-first
                for msg in reversed(recent):
                    # send history ONLY to the user who just joined
                    for conn in connections[username]:
                        await conn.send_text(f"[{msg.room}] {msg.sender}: {msg.content}")

                if unread:
                    for conn in connections[username]:
                        await conn.send_text(f"[system] {unread} unread in {room_name}")

                # then announce the join to everyone in the room
                for member in members:
                    for conn in connections.get(member, []):
                        await conn.send_text(f"[system] {username} has joined {room_name}")

            elif data.startswith("#"):
                # ---- room path ----
                # data looks like: "#room1 hello everyone"
                parts = data.split(" ", 1)
                if len(parts) < 2:
                    # malformed room message like "#room1" with no message
                    # -> tell ONLY the sender how to use it, then skip the rest
                    for conn in connections[username]:
                        await conn.send_text("[system] usage: #room_name message")
                    continue

                room_name = parts[0][1:]
                message = parts[1]

                db = SessionLocal()
                try:
                    allowed = is_room_member(db, room_name, username)
                    if allowed:
                        # persist first — delivery can fail, but the record should survive
                        db.add(models.Message(
                            room=room_name,
                            sender=username,
                            content=message,
                        ))
                        db.commit()
                finally:
                    db.close()

                if not allowed:
                    for conn in connections[username]:
                        await conn.send_text(f"[system] You are not in {room_name}")
                    continue

                # publish instead of delivering directly — the listener fans out
                await redis_client.publish(ROOM_CHANNEL, json.dumps({
                    "type": "room",
                    "room": room_name,
                    "sender": username,
                    "content": message,
                }))

            elif data.startswith("@"):
                # ---- DM path ----
                # data looks like: "@bob hello there"
                parts = data.split(" ", 1)

                if len(parts) < 2:
                    # malformed DM like "@jason" with no message
                    # -> tell ONLY the sender how to use it, then skip the rest
                    for conn in connections[username]:
                        await conn.send_text("[system] usage: @username message")
                    continue

                target = parts[0][1:]
                message = parts[1]

                # ask Redis, not this instance's connections — target may be elsewhere
                is_online = await redis_client.exists(f"yap:online:{target}")
                if not is_online:
                    for conn in connections[username]:
                        await conn.send_text(f"[system] {target} is not online")
                    continue

                await redis_client.publish(ROOM_CHANNEL, json.dumps({
                    "type": "dm",
                    "target": target,
                    "sender": username,
                    "content": message,
                }))

            else:
                # ---- broadcast path ----
                for user, conns in connections.items():
                    for conn in conns:
                        await conn.send_text(f"{username}: {data}")

    except WebSocketDisconnect:
        connections[username].remove(websocket)
        if not connections[username]:
            del connections[username]
            # last connection gone -> go offline immediately instead of waiting for TTL
            await redis_client.delete(f"yap:online:{username}")