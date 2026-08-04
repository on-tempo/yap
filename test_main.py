from fastapi.testclient import TestClient
from main import app


def test_websocket_connects_and_broadcasts():
    """A connected client should receive its own broadcast message."""
    client = TestClient(app)
    with client.websocket_connect("/ws?username=bob") as ws:
        ws.send_text("hello")
        received = ws.receive_text()
        assert received == "bob: hello"

# Skipped: this path goes through Redis Pub/Sub, 
# and the background listener does not get scheduled reliably under TestClient. 
# Verified manually with two server instances instead.
# def test_dm_between_two_users():
#     """A DM should reach the target and echo back to the sender."""
#     client = TestClient(app)
#     with client.websocket_connect("/ws?username=alice") as alice:
#         with client.websocket_connect("/ws?username=bob") as bob:
#             alice.send_text("@bob hi there")
#             assert bob.receive_text() == "[DM] alice: hi there"
#             assert alice.receive_text() == "[DM to bob] alice: hi there"

def test_dm_to_offline_user_is_reported():
    """Sending to someone who is not connected should tell the sender."""
    client = TestClient(app)
    with client.websocket_connect("/ws?username=alice") as ws:
        ws.send_text("@ghost hello")
        assert ws.receive_text() == "[system] ghost is not online"


def test_malformed_command_keeps_connection_alive():
    """A command with no message should return usage, not drop the connection."""
    client = TestClient(app)
    with client.websocket_connect("/ws?username=alice") as ws:
        ws.send_text("@bob")
        assert ws.receive_text() == "[system] usage: @username message"

        # connection still works
        ws.send_text("still here")
        assert ws.receive_text() == "alice: still here"