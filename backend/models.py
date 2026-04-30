from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Float,
    Text,
    JSON as SA_JSON,
    Index,
)
from datetime import datetime

Base = declarative_base()


class Comment(Base):
    __tablename__ = "comments"
    id = Column(String(64), primary_key=True)
    mal_id = Column(Integer, index=True, nullable=False)
    user_id = Column(String(128), index=True, nullable=False)
    user_name = Column(String(256))
    body = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    approved = Column(Boolean, default=True)
    deleted = Column(Boolean, default=False)


class Rating(Base):
    __tablename__ = "ratings"
    id = Column(String(64), primary_key=True)
    user_id = Column(String(128), index=True, nullable=False)
    mal_id = Column(Integer, index=True, nullable=False)
    score = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


class BannedUser(Base):
    __tablename__ = "banned_users"
    user_id = Column(String(128), primary_key=True)
    name = Column(String(256))
    reason = Column(Text)
    banned_at = Column(DateTime, default=datetime.utcnow)


class BannedAnime(Base):
    __tablename__ = "banned_anime"
    mal_id = Column(Integer, primary_key=True)
    reason = Column(Text)
    banned_at = Column(DateTime, default=datetime.utcnow)


class Progress(Base):
    __tablename__ = "progress"
    id = Column(String(64), primary_key=True)
    user_id = Column(String(128), index=True, nullable=False)
    mal_id = Column(Integer, index=True, nullable=False)
    episode = Column(Integer, default=1)
    current_time = Column(Float, default=0.0)
    duration = Column(Float, default=0.0)
    percent = Column(Float, default=0.0)
    completed = Column(Boolean, default=False)
    title = Column(String(512))
    image_url = Column(String(1024))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


class ProxyCache(Base):
    __tablename__ = "proxy_cache"
    key = Column(String(256), primary_key=True)
    data = Column(SA_JSON)
    expires_at = Column(DateTime, index=True)


class AnikotoMalIndex(Base):
    __tablename__ = "anikoto_mal_index"
    mal_id = Column(Integer, primary_key=True)
    anikoto_id = Column(Integer, index=True)
    title = Column(String(1024))
    slug = Column(String(512))
    indexed_at = Column(DateTime, default=datetime.utcnow)


class SecurityLog(Base):
    __tablename__ = "security_log"
    id = Column(String(64), primary_key=True)
    event = Column(String(256))
    details = Column(SA_JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
