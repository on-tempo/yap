from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

# {username: [connection, connection, ...]}
# one user can have multiple tabs/devices open
connections: defaultdict[str, list[WebSocket]] = defaultdict(list)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    # add to this user's list
    connections[username].append(websocket)

    try:
        while True:
            data = await websocket.receive_text()

            if data.startswith("@"):
                # ---- DM path ----
                # data looks like: "@bob hello there"

                # 1) split into target and message
                #    "@bob hello there" -> parts[0] = "@bob", parts[1] = "hello there"
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
                    # 2) send to ALL of the target's connections
                    for conn in connections[target]:
                        await conn.send_text(f"[DM] {username}: {message}")

                    # 3) sender also sees their own DM (on all their tabs)
                    for conn in connections[username]:
                        await conn.send_text(f"[DM to {target}] {username}: {message}")
                else:
                    # 4) target is offline/unknown -> tell ONLY the sender
                    for conn in connections[username]:
                        await conn.send_text(f"[system] {target} is not online")

            else:
                # ---- broadcast path (same as before) ----
                for user, conns in connections.items():
                    for conn in conns:
                        await conn.send_text(f"{username}: {data}")
    
    except WebSocketDisconnect:
        # remove ONLY this connection
        connections[username].remove(websocket)
        # list empty -> user fully offline  
        if not connections[username]:            
            del connections[username]