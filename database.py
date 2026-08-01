import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# read from env, fall back to local docker-compose database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://yap:yap@localhost:5433/yap")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()