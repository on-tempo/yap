from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from database import engine, SessionLocal
import models

app = FastAPI()

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

                    # then deliver to everyone currently in the room
                    for user in rooms[room_name]:
                        for conn in connections.get(user, []):
                            await conn.send_text(f"[{room_name}] {username}: {message}")
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

                if target in connections:
                    # send to ALL of the target's connections
                    for conn in connections[target]:
                        await conn.send_text(f"[DM] {username}: {message}")

                    # sender also sees their own DM (on all their tabs)
                    for conn in connections[username]:
                        await conn.send_text(f"[DM to {target}] {username}: {message}")
                else:
                    # target is offline/unknown -> tell ONLY the sender
                    for conn in connections[username]:
                        await conn.send_text(f"[system] {target} is not online")

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