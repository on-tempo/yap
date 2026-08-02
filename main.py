from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from database import engine, SessionLocal
import models
import asyncio
import json
import os
import redis.asyncio as redis

app = FastAPI()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

ROOM_CHANNEL = "yap:rooms"
PRESENCE_TTL = 30        # seconds before a stale entry expires
HEARTBEAT_INTERVAL = 10  # refresh well before the TTL runs out

async def redis_listener():
    """Listen to the room channel and fan out to this instance's connections."""
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

            for user in rooms.get(room_name, set()):
                for conn in connections.get(user, []):
                    await conn.send_text(
                        f"[{room_name}] {sender}: {message}"
                    )

        elif msg_type == "dm":
            target = payload["target"]

            # receiver
            for conn in connections.get(target, []):
                await conn.send_text(
                    f"[DM] {sender}: {message}"
                )

            # sender echo
            for conn in connections.get(sender, []):
                await conn.send_text(
                    f"[DM to {target}] {sender}: {message}"
                )

async def presence_heartbeat():
    """Refresh presence keys for everyone connected to this instance.

    A crashed server stops refreshing, so its entries expire on their own
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        for user in list(connections.keys()):
            await redis_client.set(f"yap:online:{user}", "1", ex=PRESENCE_TTL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(redis_listener())
    asyncio.create_task(presence_heartbeat())

models.Base.metadata.create_all(bind=engine)

# {username: [connection, connection, ...]}
# one user can have multiple tabs/devices open
connections: defaultdict[str, list[WebSocket]] = defaultdict(list)

# {room_name: {username, username, ...}}
rooms: defaultdict[str, set[str]] = defaultdict(set)


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
                # data looks like: "/unread"  (all rooms this user is in)
                db = SessionLocal()
                try:
                    lines = []
                    for room_name in rooms:
                        if username not in rooms[room_name]:
                            continue

                        record = (
                            db.query(models.RoomRead)
                            .filter(
                                models.RoomRead.room == room_name,
                                models.RoomRead.username == username,
                            )
                            .first()
                        )
                        last_read_id = record.last_read_id if record else 0

                        count = (
                            db.query(models.Message)
                            .filter(
                                models.Message.room == room_name,
                                models.Message.id > last_read_id,
                            )
                            .count()
                        )
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
                rooms[room_name].add(username)

                # open a short-lived session, same pattern as the write path
                db = SessionLocal()
                try:
                    # newest 20 messages in this room
                    recent = (
                        db.query(models.Message)
                        .filter(models.Message.room == room_name)
                        .order_by(models.Message.created_at.desc())
                        .limit(20)
                        .all()
                    )
                finally:
                    db.close()

                # fetched newest-first, but display oldest-first
                for msg in reversed(recent):
                    # send history ONLY to the user who just joined
                    for conn in connections[username]:
                        await conn.send_text(f"[{msg.room}] {msg.sender}: {msg.content}")

                # how much did this user miss since their last read?
                db = SessionLocal()
                try:
                    record = (
                        db.query(models.RoomRead)
                        .filter(
                            models.RoomRead.room == room_name,
                            models.RoomRead.username == username,
                        )
                        .first()
                    )
                    last_read_id = record.last_read_id if record else 0

                    unread = (
                        db.query(models.Message)
                        .filter(
                            models.Message.room == room_name,
                            models.Message.id > last_read_id,
                        )
                        .count()
                    )
                finally:
                    db.close()

                if unread:
                    for conn in connections[username]:
                        await conn.send_text(f"[system] {unread} unread in {room_name}")
                
                # then announce the join to everyone in the room
                for member in rooms[room_name]:
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

                if room_name in rooms and username in rooms[room_name]:
                    # persist first — delivery can fail, but the record should survive
                    db = SessionLocal()
                    try:
                        db.add(models.Message(room=room_name, sender=username, content=message))
                        db.commit()
                    finally:
                        db.close()

                    # publish instead of delivering directly — the listener fans out
                    await redis_client.publish(ROOM_CHANNEL, json.dumps({
                        "type": "room",
                        "room": room_name,
                        "sender": username,
                        "content": message,
                    }))
                else:
                    # user is not in the room or room doesn't exist
                    for conn in connections[username]:
                        await conn.send_text(f"[system] You are not in {room_name}")

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

                await redis_client.publish(
                    ROOM_CHANNEL,
                    json.dumps({
                        "type": "dm",
                        "target": target,
                        "sender": username,
                        "content": message,
                    }),
                )

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