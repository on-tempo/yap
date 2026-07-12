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
            for user, conns in connections.items():
                # a user may have many connections
                for conn in conns:
                    await conn.send_text(f"{username}: {data}")
    except WebSocketDisconnect:
        # remove ONLY this connection
        connections[username].remove(websocket)
        # list empty -> user fully offline  
        if not connections[username]:            
            del connections[username]