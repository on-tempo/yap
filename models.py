from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime, timezone
from database import Base

class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    room = Column(String, nullable=False, index=True)
    sender = Column(String, nullable=False)
    content = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)

class RoomRead(Base):
    __tablename__ = "room_reads"

    id = Column(Integer, primary_key=True)
    room = Column(String, nullable=False, index=True)
    username = Column(String, nullable=False, index=True)
    last_read_id = Column(Integer, nullable=False, default=0)

class RoomMember(Base):
    __tablename__ = "room_members"

    id = Column(Integer, primary_key=True)
    room = Column(String, nullable=False, index=True)
    username = Column(String, nullable=False, index=True)