# yap 🗣️

Yap is a real-time chat application built with FastAPI, WebSocket, and PostgreSQL.

## Why WebSocket

Traditional polling forces a trade-off: polling frequently reduces message delay but increases unnecessary requests, while polling less often saves resources at the cost of slower message delivery. WebSocket keeps a persistent connection between the client and server, allowing the server to push new messages immediately. This removes the polling trade-off and enables low-latency communication with minimal overhead.

## How it works

### connections

A single user can connect from multiple browser tabs or devices. Storing only one WebSocket per username caused newer connections to overwrite older ones, leaving stale connections behind. `connections` is implemented as a dictionary of lists so every active socket is tracked independently, allowing reliable multi-device support.

### rooms

Each chat room stores its members in a `set`. Since sets ignore duplicate insertions in O(1) average time, users cannot join the same room multiple times, eliminating the need for additional duplicate-checking logic.

### message persistence

Room messages are written to PostgreSQL before they are delivered, so a record survives even if delivery fails. When a user joins a room, the 20 most recent messages are fetched newest-first and replayed oldest-first, giving them the recent conversation as context.

Database sessions are opened per operation rather than per connection. A WebSocket connection can stay open for hours, so holding a session for its lifetime would exhaust the connection pool and keep transactions open unnecessarily.

## Message protocol

| Input | Action |
|-------|--------|
| `Hello` | Broadcast to all connected users |
| `/join room` | Join a chat room and replay its recent history |
| `#room message` | Send a message to everyone in a room |
| `@user message` | Send a private message |

## Input validation

Originally, incomplete commands such as `@jason` or `#general` without a message caused an `IndexError`, terminating the WebSocket connection. Commands are now validated before parsing, and invalid input returns a usage message while keeping the connection alive.

## Running locally

Start PostgreSQL with Docker Compose:

```bash
git clone https://github.com/on-tempo/yap.git
cd yap
docker compose up -d
```

Then run the server:

```bash
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Open the browser developer tools and connect from the console:

```javascript
const ws = new WebSocket("ws://localhost:8000/ws?username=jason");
ws.onmessage = (e) => console.log(e.data);
ws.send("/join general");
ws.send("#general hello");
```

## Current limitations / Roadmap

- Room membership is still in memory, so joins are lost on restart while messages persist.
- The server runs as a single instance; two instances would not see each other's connections.
- Redis Pub/Sub is the next step, letting multiple instances broadcast to each other.
- Authentication is not implemented — usernames are self-declared via a query parameter.