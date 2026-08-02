# yap 🗣️

Yap is a real-time chat application built with FastAPI, WebSocket, Redis, and PostgreSQL.

## Why WebSocket

Traditional polling forces a trade-off: polling frequently reduces message delay but increases unnecessary requests, while polling less often saves resources at the cost of slower message delivery. WebSocket keeps a persistent connection between the client and server, allowing the server to push new messages immediately. This removes the polling trade-off and enables low-latency communication with minimal overhead.

## How it works

### connections

A single user can connect from multiple browser tabs or devices. Storing only one WebSocket per username caused newer connections to overwrite older ones, leaving stale connections behind. `connections` is implemented as a dictionary of lists so every active socket is tracked independently, allowing reliable multi-device support.

### cross-instance messaging

`connections` exists only in process memory, so multiple server instances cannot see each other's WebSocket connections. Instead of sending messages directly between servers, each instance publishes events to Redis Pub/Sub and subscribes to the same channel. Every server forwards incoming events only to the clients it currently owns, allowing horizontal scaling without server-to-server communication or code changes when new instances are added.

Room messages and direct messages share a single channel and are distinguished by a `type` field in the payload. Per-room or per-user channels would let each instance subscribe only to what it needs, but they add subscribe and unsubscribe bookkeeping on every join, connect, and disconnect — not worth it at this scale.

### rooms

Each chat room stores its members in a `set`. Since sets ignore duplicate insertions in O(1) average time, users cannot join the same room multiple times, eliminating the need for additional duplicate-checking logic.

### presence

When a server crashes, its disconnect handler never runs, leaving stale "online" records behind. Instead of relying on explicit deletion, each user is tracked by a Redis key with a 30-second TTL that a heartbeat refreshes every 10 seconds. If the heartbeat stops because the server crashed, Redis expires the key on its own, so no cleanup code has to run on a process that is already gone.

Each user gets an individual key because Redis sets do not support a TTL on individual members.

### message persistence

Room messages are written to PostgreSQL before they are delivered, so a record survives even if delivery fails. When a user joins a room, the 20 most recent messages are fetched newest-first and replayed oldest-first, giving them the recent conversation as context.

Database sessions are opened per operation rather than per connection. A WebSocket connection can stay open for hours, so holding a session for its lifetime would exhaust the connection pool and keep transactions open unnecessarily.

### read receipts

Storing one read record per message and per user scales poorly. A room with 100 members and 1,000 messages would already require 100,000 rows. Instead, each (room, user) pair stores only `last_read_id`, the ID of the latest message that user has read. Unread counts are calculated by counting messages whose IDs are greater than `last_read_id`, so storage size stays independent of how many messages exist.

This design assumes messages are read in order, so it cannot represent per-message read timestamps or skipped messages.

## Message protocol

| Input | Action |
|-------|--------|
| `Hello` | Broadcast to all connected users |
| `/join room` | Join a chat room, replay its recent history, and show unread count |
| `#room message` | Send a message to everyone in a room |
| `@user message` | Send a private message |
| `/read room` | Mark a room as read up to its latest message |
| `/unread` | Show unread counts for the rooms you have joined |

## Input validation

Originally, incomplete commands such as `@jason` or `#general` without a message caused an `IndexError`, terminating the WebSocket connection. Commands are now validated before parsing, and invalid input returns a usage message while keeping the connection alive.

## Tech stack

| Component | Technology |
|-----------|------------|
| Backend | FastAPI |
| Real-time communication | WebSocket |
| Cross-instance messaging & presence | Redis |
| Database | PostgreSQL |
| Containers | Docker Compose |

## Running locally

Start PostgreSQL and Redis with Docker Compose:

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

To see cross-instance messaging, start a second server on another port and connect a second client to it:

```bash
uvicorn main:app --port 8001
```

## Current limitations

- Room membership is still stored in memory, so joins are lost after a server restart even though messages persist.
- Direct messages are not persisted.
- Authentication is not implemented. Usernames are self-declared through a query parameter.
- Automated tests have not yet been added.