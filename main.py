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


@app.on_event("startup")
async def startup():
    asyncio.create_task(redis_listener())

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

    try:
        while True:
            data = await websocket.receive_text()

            if data.startswith("/join"):
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
        # remove ONLY this connection
        connections[username].remove(websocket)
        # list empty -> user fully offline
        if not connections[username]:
            del connections[username]