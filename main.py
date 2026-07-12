from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

connections: dict[str, WebSocket] = {}   # {username: live connection}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    connections[username] = websocket

    try:
        while True:
            data = await websocket.receive_text()
            for user, conn in connections.items():
                await conn.send_text(f"{username}: {data}")
    except WebSocketDisconnect:
        del connections[username]