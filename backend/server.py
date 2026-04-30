"""
Lumen anime backend — comments, ratings, admin moderation, security stubs.

Auth model:
- Users are stored & authenticated by Supabase (frontend uses Supabase JS).
- This backend validates the user's Supabase JWT by calling
  https://{SUPABASE_URL}/auth/v1/user with the token. No JWT secret needed.
- Admin = user whose Supabase email == ADMIN_EMAIL env.

Storage: MongoDB collections
- comments       : per-anime comments
- ratings        : per-user per-anime 1..5 stars
- banned_users   : list of banned supabase user_ids
- banned_anime   : list of banned mal_ids (cannot be streamed)
"""
from fastapi import FastAPI, APIRouter, HTTPException, Header, Depends, Request
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
import asyncio
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Annotated
import uuid
from datetime import datetime
import httpx

# In-memory fallback cache used when MongoDB is not available (development)
IN_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_NAME = os.environ.get('DB_NAME', 'anime_stream')
SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', '')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@lumen.local').lower()


app = FastAPI(title="Lumen API")
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("lumen")


# ---------------------------------------------------------------------------
# Simple file-backed async collections to replace MongoDB for development.
# Provides a minimal subset of the Motor/PyMongo API used by this service:
#  - find(filter=None, sort=None, limit=None)
#  - find_one(filter)
#  - insert_one(doc)
#  - update_one(filter, update, upsert=False)
#  - aggregate(pipeline) -> object with to_list(n)
#  - create_index(...) (no-op)
#
class AsyncAggregateCursor:
    def __init__(self, rows):
        self._rows = rows

    async def to_list(self, n):
        return self._rows[:n]


class AsyncCollection:
    def __init__(self, name, data_dir: Path):
        self.name = name
        self._path = data_dir / f"{name}.json"
        self._lock = asyncio.Lock()
        self._docs: List[Dict[str, Any]] = []
        # ensure data dir exists
        data_dir.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._path.write_text("[]")

    async def load(self):
        async with self._lock:
            txt = self._path.read_text()
            try:
                self._docs = list(__import__("json").loads(txt))
            except Exception:
                self._docs = []

    async def _persist(self):
        async with self._lock:
            self._path.write_text(__import__("json").dumps(self._docs, default=str))

    async def find(self, filter: Optional[Dict[str, Any]] = None, sort: Optional[List] = None, limit: Optional[int] = None):
        def matches(doc, filt):
            if not filt:
                return True
            for k, v in filt.items():
                if isinstance(v, dict):
                    # support $in and $ne minimally
                    if "$in" in v:
                        if doc.get(k) not in v["$in"]:
                            return False
                    if "$ne" in v:
                        if doc.get(k) == v["$ne"]:
                            return False
                else:
                    if doc.get(k) != v:
                        return False
            return True

        res = [d for d in self._docs if matches(d, filter)]
        if sort:
            for key, direction in reversed(sort):
                res.sort(key=lambda x: x.get(key), reverse=(direction < 0))
        if limit is not None:
            res = res[:limit]
        return res

    async def find_one(self, filter: Dict[str, Any], sort: Optional[List] = None):
        lst = await self.find(filter, sort=sort, limit=1)
        return lst[0] if lst else None

    async def count_documents(self, filter: Optional[Dict[str, Any]] = None):
        lst = await self.find(filter)
        return len(lst)


    async def insert_one(self, doc: Dict[str, Any]):
        if not isinstance(doc, dict):
            raise TypeError("doc must be a dict")
        self._docs.append(doc)
        await self._persist()

    async def update_one(self, filter: Dict[str, Any], update: Dict[str, Any], upsert: bool = False):
        row = await self.find_one(filter)
        if row:
            # apply $set
            if "$set" in update:
                for k, v in update["$set"].items():
                    row[k] = v
            # ignore other update ops except $setOnInsert
            await self._persist()
            return True
        else:
            if upsert:
                new = {}
                # apply $setOnInsert
                if "$setOnInsert" in update:
                    for k, v in update["$setOnInsert"].items():
                        new[k] = v
                # apply $set
                if "$set" in update:
                    for k, v in update["$set"].items():
                        new[k] = v
                # ensure an id exists
                if "id" not in new:
                    new["id"] = str(uuid.uuid4())
                self._docs.append(new)
                await self._persist()
                return True
            return False

    async def delete_one(self, filter: Dict[str, Any]):
        for idx, row in enumerate(self._docs):
            if all(row.get(k) == v for k, v in filter.items()):
                del self._docs[idx]
                await self._persist()
                return True
        return False

    async def delete_many(self, filter: Dict[str, Any]):
        before = len(self._docs)
        self._docs = [r for r in self._docs if not all(r.get(k) == v for k, v in filter.items())]
        if len(self._docs) != before:
            await self._persist()
        return before - len(self._docs)

    def aggregate(self, pipeline: List[Dict[str, Any]]):
        # Minimal implementation for pipelines used in this service: $match followed by $group for avg/count
        rows = self._docs
        for stage in pipeline:
            if "$match" in stage:
                filt = stage["$match"]
                rows = [r for r in rows if all(r.get(k) == v for k, v in filt.items())]
            elif "$group" in stage:
                g = stage["$group"]
                # expect avg and sum like {"_id": "$mal_id", "avg": {"$avg": "$score"}, "count": {"$sum": 1}}
                if "avg" in g and "count" in g:
                    scores = [r.get("score") for r in rows if isinstance(r.get("score"), (int, float))]
                    if scores:
                        avg = sum(scores) / len(scores)
                        cnt = len(scores)
                    else:
                        avg = 0.0
                        cnt = 0
                    return AsyncAggregateCursor([{"_id": None, "avg": avg, "count": cnt}])
        return AsyncAggregateCursor(rows)

    async def create_index(self, *args, **kwargs):
        # no-op for file-backed storage
        return None


# initialize collections
DATA_DIR = ROOT_DIR / "data"
comments = AsyncCollection("comments", DATA_DIR)
ratings = AsyncCollection("ratings", DATA_DIR)
banned_users = AsyncCollection("banned_users", DATA_DIR)
banned_anime = AsyncCollection("banned_anime", DATA_DIR)
security_log = AsyncCollection("security_log", DATA_DIR)
progress = AsyncCollection("progress", DATA_DIR)
proxy_cache = AsyncCollection("proxy_cache", DATA_DIR)
anikoto_mal_index = AsyncCollection("anikoto_mal_index", DATA_DIR)
notifications = AsyncCollection("notifications", DATA_DIR)

# db shim matching previous attribute access
class DBShim:
    def __getitem__(self, key: str):
        return getattr(self, key)

db = DBShim()
db.comments = comments
db.ratings = ratings
db.banned_users = banned_users
db.banned_anime = banned_anime
db.security_log = security_log
db.progress = progress
db.proxy_cache = proxy_cache
db.anikoto_mal_index = anikoto_mal_index
db.notifications = notifications



# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class CommentIn(BaseModel):
    body: Annotated[str, Field(min_length=1, max_length=2000)]


class CommentOut(BaseModel):
    id: str
    mal_id: int
    user_id: str
    user_name: str
    body: str
    created_at: datetime
    approved: bool = True


class RatingIn(BaseModel):
    score: Annotated[int, Field(ge=1, le=5)]


class RatingStats(BaseModel):
    avg: float
    count: int
    my_rating: Optional[int] = None


class BanAnimeIn(BaseModel):
    mal_id: int
    reason: Optional[str] = ""


class BanUserIn(BaseModel):
    user_id: str
    reason: Optional[str] = ""


class NotificationIn(BaseModel):
    title: Annotated[str, Field(min_length=3, max_length=120)]
    body: Annotated[str, Field(min_length=3, max_length=2000)]
    level: Annotated[str, Field(min_length=3, max_length=16)] = "info"
    target: Annotated[str, Field(min_length=3, max_length=16)] = "all"


class AuthedUser(BaseModel):
    id: str
    email: str
    name: str
    is_admin: bool
    is_banned: bool


class AuthCredentialsIn(BaseModel):
    email: Annotated[str, Field(min_length=3, max_length=255)]
    password: Annotated[str, Field(min_length=8, max_length=72)]


# ---------------------------------------------------------------------------
# Auth helper — validate Supabase JWT via /auth/v1/user
# ---------------------------------------------------------------------------
async def _is_user_banned(user_id: str) -> bool:
    return await db.banned_users.find_one({"user_id": user_id}) is not None


async def get_current_user(authorization: Optional[str] = Header(None)) -> AuthedUser:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()

    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        # Misconfigured server
        raise HTTPException(status_code=503, detail="Auth not configured on server")

    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": SUPABASE_ANON_KEY,
                },
            )
    except Exception as e:
        logger.exception("Supabase auth call failed")
        raise HTTPException(status_code=502, detail=f"Auth provider unreachable: {e}")

    if r.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    data = r.json()
    user_id = data.get("id")
    email = (data.get("email") or "").lower()
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    meta = data.get("user_metadata") or {}
    name = meta.get("name") or meta.get("full_name") or (email.split("@")[0] if email else "anon")

    banned = await _is_user_banned(user_id)
    is_admin = bool(email) and email == ADMIN_EMAIL

    return AuthedUser(
        id=user_id, email=email, name=name,
        is_admin=is_admin, is_banned=banned,
    )


async def require_admin(user: AuthedUser = Depends(get_current_user)) -> AuthedUser:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


async def require_active(user: AuthedUser = Depends(get_current_user)) -> AuthedUser:
    if user.is_banned:
        raise HTTPException(status_code=403, detail="Your account is banned")
    return user


async def _supabase_auth_request(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise HTTPException(status_code=503, detail="Auth not configured on server")
    async with httpx.AsyncClient(timeout=10.0) as http:
        res = await http.post(
            f"{SUPABASE_URL}/auth/v1/{path}",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
    try:
        data = res.json()
    except Exception:
        data = {"message": res.text}
    if res.status_code >= 400:
        message = data.get("msg") or data.get("message") or data.get("error_description") or data.get("error") or "Authentication failed"
        raise HTTPException(status_code=res.status_code, detail=message)
    return data


# ---------------------------------------------------------------------------
# Public health
# ---------------------------------------------------------------------------
@api_router.get("/")
async def root():
    return {"status": "ok", "service": "lumen", "time": datetime.utcnow().isoformat()}


@api_router.get("/health")
async def health():
    return {"ok": True}


@api_router.post("/auth/signin")
async def auth_signin(payload: AuthCredentialsIn, request: Request):
    return await _supabase_auth_request("token?grant_type=password", {
        "email": payload.email,
        "password": payload.password,
    })


@api_router.post("/auth/signup")
async def auth_signup(payload: AuthCredentialsIn, request: Request):
    return await _supabase_auth_request("signup", {
        "email": payload.email,
        "password": payload.password,
    })


# ---------------------------------------------------------------------------
# Anime moderation lookup (used by player)
# ---------------------------------------------------------------------------
@api_router.get("/anime/{mal_id}/blocked")
async def is_anime_blocked(mal_id: int):
    found = await db.banned_anime.find_one({"mal_id": mal_id})
    return {"blocked": bool(found), "reason": (found or {}).get("reason", "")}


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------
@api_router.get("/comments/{mal_id}", response_model=List[CommentOut])
async def list_comments(mal_id: int):
    rows = await db.comments.find(
        {"mal_id": mal_id, "approved": {"$ne": False}, "deleted": {"$ne": True}},
        sort=[("created_at", -1)],
        limit=200,
    )
    return [CommentOut(**{
        "id": r["id"], "mal_id": r["mal_id"], "user_id": r["user_id"],
        "user_name": r.get("user_name", "anon"), "body": r["body"],
        "created_at": r["created_at"], "approved": r.get("approved", True),
    }) for r in rows]


@api_router.post("/comments/{mal_id}", response_model=CommentOut)
async def create_comment(mal_id: int, payload: CommentIn, request: Request,
                         user: AuthedUser = Depends(require_active)):
    doc = {
        "id": str(uuid.uuid4()),
        "mal_id": mal_id,
        "user_id": user.id,
        "user_name": user.name,
        "body": payload.body,
        "created_at": datetime.utcnow(),
        "approved": True,
        "deleted": False,
    }
    await db.comments.insert_one(doc)
    return CommentOut(**doc)


@api_router.delete("/comments/{comment_id}")
async def delete_comment(comment_id: str, user: AuthedUser = Depends(get_current_user)):
    row = await db.comments.find_one({"id": comment_id})
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    if row["user_id"] != user.id and not user.is_admin:
        raise HTTPException(status_code=403, detail="Not allowed")
    await db.comments.update_one({"id": comment_id}, {"$set": {"deleted": True}})
    return {"ok": True}


# ---------------------------------------------------------------------------
# Ratings
# ---------------------------------------------------------------------------
@api_router.get("/ratings/{mal_id}", response_model=RatingStats)
async def get_rating(mal_id: int, authorization: Optional[str] = Header(None)):
    pipeline = [
        {"$match": {"mal_id": mal_id}},
        {"$group": {"_id": "$mal_id", "avg": {"$avg": "$score"}, "count": {"$sum": 1}}},
    ]
    agg = await db.ratings.aggregate(pipeline).to_list(1)
    avg = round(agg[0]["avg"], 2) if agg else 0.0
    count = agg[0]["count"] if agg else 0

    my_rating = None
    if authorization:
        try:
            user = await get_current_user(authorization)
            mine = await db.ratings.find_one({"mal_id": mal_id, "user_id": user.id})
            my_rating = mine["score"] if mine else None
        except HTTPException:
            pass
    return RatingStats(avg=avg, count=count, my_rating=my_rating)


async def _rating_stats(mal_id: int, user_id: Optional[str]) -> RatingStats:
    pipeline = [
        {"$match": {"mal_id": mal_id}},
        {"$group": {"_id": "$mal_id", "avg": {"$avg": "$score"}, "count": {"$sum": 1}}},
    ]
    agg = await db.ratings.aggregate(pipeline).to_list(1)
    avg = round(agg[0]["avg"], 2) if agg else 0.0
    count = agg[0]["count"] if agg else 0
    my_rating = None
    if user_id:
        mine = await db.ratings.find_one({"mal_id": mal_id, "user_id": user_id})
        my_rating = mine["score"] if mine else None
    return RatingStats(avg=avg, count=count, my_rating=my_rating)


@api_router.post("/ratings/{mal_id}", response_model=RatingStats)
async def upsert_rating(mal_id: int, payload: RatingIn,
                        user: AuthedUser = Depends(require_active)):
    await db.ratings.update_one(
        {"user_id": user.id, "mal_id": mal_id},
        {"$set": {"score": payload.score, "updated_at": datetime.utcnow()},
         "$setOnInsert": {"id": str(uuid.uuid4()), "created_at": datetime.utcnow(),
                          "user_id": user.id, "mal_id": mal_id}},
        upsert=True,
    )
    return await _rating_stats(mal_id, user.id)


# ---------------------------------------------------------------------------
# Me / who am I
# ---------------------------------------------------------------------------
@api_router.get("/me")
async def me(user: AuthedUser = Depends(get_current_user)):
    return user.dict()


# ---------------------------------------------------------------------------
# Public profiles — shareable read-only view of any user's activity.
# Privacy: never returns email; banned users return 404.
# ---------------------------------------------------------------------------
class PublicProfileOut(BaseModel):
    user_id: str
    user_name: str
    is_admin: bool
    joined_at: Optional[str] = None
    counts: Dict[str, int]


class PublicRatingOut(BaseModel):
    mal_id: int
    score: int
    updated_at: Optional[str] = None
    title: Optional[str] = None
    image_url: Optional[str] = None


class PublicProgressOut(BaseModel):
    mal_id: int
    episode: int
    percent: float
    completed: bool
    title: Optional[str] = None
    image_url: Optional[str] = None
    updated_at: Optional[str] = None


async def _resolve_user_name(user_id: str) -> Optional[str]:
    """Find a user_name from any document where this user has activity."""
    for coll, sort_field in (
        ("comments", "created_at"),
        ("ratings", "updated_at"),
        ("progress", "updated_at"),
    ):
        c = db[coll]
        row = await c.find_one({"user_id": user_id, "user_name": {"$exists": True}},
                               sort=[(sort_field, -1)])
        if row and row.get("user_name"):
            return row["user_name"]
    return None


@api_router.get("/users/{user_id}/profile", response_model=PublicProfileOut)
async def public_profile(user_id: str):
    if await db.banned_users.find_one({"user_id": user_id}):
        raise HTTPException(status_code=404, detail="Profile not found")
    name = await _resolve_user_name(user_id)
    if not name:
        # Maybe a real user with no activity yet — return a minimal stub so
        # /profile/me works for fresh accounts.
        comments = ratings = progress = 0
    else:
        comments, ratings, progress = await asyncio.gather(
            db.comments.count_documents({"user_id": user_id, "deleted": {"$ne": True}}),
            db.ratings.count_documents({"user_id": user_id}),
            db.progress.count_documents({"user_id": user_id}),
        )
    if not name and not (comments or ratings or progress):
        raise HTTPException(status_code=404, detail="Profile not found")

    # First seen ts: oldest activity row across collections.
    first_ts: Optional[datetime] = None
    for coll, field in (("comments", "created_at"),
                        ("ratings", "created_at"),
                        ("progress", "updated_at")):
        row = await db[coll].find_one({"user_id": user_id}, sort=[(field, 1)])
        if row and row.get(field):
            ts = row[field]
            if isinstance(ts, datetime) and (first_ts is None or ts < first_ts):
                first_ts = ts

    return PublicProfileOut(
        user_id=user_id,
        user_name=name or "anon",
        is_admin=False,
        joined_at=first_ts.isoformat() if first_ts else None,
        counts={"comments": comments, "ratings": ratings, "progress": progress},
    )


@api_router.get("/users/{user_id}/ratings", response_model=List[PublicRatingOut])
async def public_user_ratings(user_id: str, limit: int = 50):
    limit = max(1, min(100, limit))
    rows = await db.ratings.find({"user_id": user_id}, sort=[("score", -1)], limit=limit)
    out: List[PublicRatingOut] = []
    for r in rows:
        # Try to enrich with title/poster via the most recent progress row for
        # the same mal_id (cheaper than re-hitting Jikan for every rating).
        prog = await db.progress.find_one(
            {"user_id": user_id, "mal_id": r["mal_id"]},
            sort=[("updated_at", -1)],
        )
        title = (prog or {}).get("title")
        image_url = (prog or {}).get("image_url")
        ts = r.get("updated_at") or r.get("created_at")
        out.append(PublicRatingOut(
            mal_id=r["mal_id"],
            score=int(r["score"]),
            updated_at=ts.isoformat() if isinstance(ts, datetime) else None,
            title=title,
            image_url=image_url,
        ))
    return out


@api_router.get("/users/{user_id}/watchlist", response_model=List[PublicProgressOut])
async def public_user_watchlist(user_id: str, limit: int = 30):
    limit = max(1, min(60, limit))
    rows = await db.progress.find({"user_id": user_id}, sort=[("updated_at", -1)], limit=limit)
    out: List[PublicProgressOut] = []
    seen_mal_ids: set = set()
    for r in rows:
        mid = r["mal_id"]
        if mid in seen_mal_ids:
            continue  # collapse multiple episode rows of the same anime
        seen_mal_ids.add(mid)
        ts = r.get("updated_at")
        out.append(PublicProgressOut(
            mal_id=mid,
            episode=int(r.get("episode", 1)),
            percent=float(r.get("percent", 0)),
            completed=bool(r.get("completed", False)),
            title=r.get("title"),
            image_url=r.get("image_url"),
            updated_at=ts.isoformat() if isinstance(ts, datetime) else None,
        ))
    return out


@api_router.get("/users/{user_id}/comments", response_model=List[CommentOut])
async def public_user_comments(user_id: str, limit: int = 30):
    limit = max(1, min(50, limit))
    rows = await db.comments.find(
        {"user_id": user_id, "deleted": {"$ne": True}, "approved": {"$ne": False}},
        sort=[("created_at", -1)],
        limit=limit,
    )
    return [CommentOut(**{
        "id": r["id"], "mal_id": r["mal_id"], "user_id": r["user_id"],
        "user_name": r.get("user_name", "anon"), "body": r["body"],
        "created_at": r["created_at"], "approved": r.get("approved", True),
    }) for r in rows]


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------
@api_router.get("/admin/stats")
async def admin_stats(_: AuthedUser = Depends(require_admin)):
    comments = await db.comments.count_documents({})
    ratings = await db.ratings.count_documents({})
    banned_users = await db.banned_users.count_documents({})
    banned_anime = await db.banned_anime.count_documents({})
    flagged = await db.security_log.count_documents({})
    # distinct users from comments/ratings
    user_ids = set()
    comments_rows = await db.comments.find({}, limit=None)
    for r in comments_rows:
        user_ids.add(r.get("user_id"))
    ratings_rows = await db.ratings.find({}, limit=None)
    for r in ratings_rows:
        user_ids.add(r.get("user_id"))
    return {
        "comments": comments,
        "ratings": ratings,
        "banned_users": banned_users,
        "banned_anime": banned_anime,
        "flagged_events": flagged,
        "active_users": len(user_ids),
    }


@api_router.get("/admin/users")
async def admin_list_users(_: AuthedUser = Depends(require_admin)):
    """Return users seen via comments/ratings + banned status."""
    seen: Dict[str, Dict[str, Any]] = {}
    comments_all = await db.comments.find({}, limit=None)
    for r in comments_all:
        uid = r.get("user_id")
        if uid and uid not in seen:
            seen[uid] = {"user_id": uid, "name": r.get("user_name", ""), "comments": 0}
        if uid:
            seen[uid]["comments"] = seen[uid].get("comments", 0) + 1
    ratings_all = await db.ratings.find({}, limit=None)
    for r in ratings_all:
        uid = r.get("user_id")
        if uid and uid not in seen:
            seen[uid] = {"user_id": uid, "name": "", "ratings": 0}
        if uid:
            seen[uid]["ratings"] = seen[uid].get("ratings", 0) + 1
    banned_rows = await db.banned_users.find({}, limit=None)
    banned = {b["user_id"] for b in banned_rows}
    out = []
    for uid, info in seen.items():
        out.append({**info, "banned": uid in banned})
    # also include explicitly banned but unseen
    for b in banned_rows:
        if b["user_id"] not in seen:
            out.append({"user_id": b["user_id"], "name": b.get("name", ""),
                        "banned": True, "comments": 0, "ratings": 0})
    return out


@api_router.post("/admin/users/ban")
async def admin_ban_user(payload: BanUserIn, _: AuthedUser = Depends(require_admin)):
    await db.banned_users.update_one(
        {"user_id": payload.user_id},
        {"$set": {"user_id": payload.user_id, "reason": payload.reason or "",
                  "banned_at": datetime.utcnow()}},
        upsert=True,
    )
    return {"ok": True}


@api_router.post("/admin/users/unban")
async def admin_unban_user(payload: BanUserIn, _: AuthedUser = Depends(require_admin)):
    await db.banned_users.delete_one({"user_id": payload.user_id})
    return {"ok": True}


@api_router.get("/admin/anime/banned")
async def admin_list_banned_anime(_: AuthedUser = Depends(require_admin)):
    rows = await db.banned_anime.find({}, sort=[("banned_at", -1)], limit=500)
    return [{"mal_id": r["mal_id"], "reason": r.get("reason", ""),
             "banned_at": r.get("banned_at")} for r in rows]


@api_router.post("/admin/anime/ban")
async def admin_ban_anime(payload: BanAnimeIn, _: AuthedUser = Depends(require_admin)):
    await db.banned_anime.update_one(
        {"mal_id": payload.mal_id},
        {"$set": {"mal_id": payload.mal_id, "reason": payload.reason or "",
                  "banned_at": datetime.utcnow()}},
        upsert=True,
    )
    return {"ok": True}


@api_router.delete("/admin/anime/ban/{mal_id}")
async def admin_unban_anime(mal_id: int, _: AuthedUser = Depends(require_admin)):
    await db.banned_anime.delete_one({"mal_id": mal_id})
    return {"ok": True}


@api_router.get("/admin/comments")
async def admin_list_comments(_: AuthedUser = Depends(require_admin)):
    rows = await db.comments.find({}, sort=[("created_at", -1)], limit=500)
    return [{
        "id": r["id"], "mal_id": r["mal_id"], "user_id": r["user_id"],
        "user_name": r.get("user_name", ""), "body": r["body"],
        "created_at": r["created_at"], "approved": r.get("approved", True),
        "deleted": r.get("deleted", False),
    } for r in rows]


@api_router.post("/admin/comments/{comment_id}/approve")
async def admin_approve_comment(comment_id: str, _: AuthedUser = Depends(require_admin)):
    await db.comments.update_one({"id": comment_id},
                                 {"$set": {"approved": True, "deleted": False}})
    return {"ok": True}


@api_router.delete("/admin/comments/{comment_id}")
async def admin_delete_comment(comment_id: str, _: AuthedUser = Depends(require_admin)):
    await db.comments.update_one({"id": comment_id}, {"$set": {"deleted": True}})
    return {"ok": True}


@api_router.delete("/admin/comments/{comment_id}/hard")
async def admin_hard_delete_comment(comment_id: str, _: AuthedUser = Depends(require_admin)):
    await db.comments.delete_one({"id": comment_id})
    return {"ok": True}


@api_router.get("/notifications")
async def list_notifications(user: AuthedUser = Depends(get_current_user)):
    rows = await db.notifications.find({"active": True}, sort=[("created_at", -1)], limit=50)
    out = []
    for n in rows:
        target = (n.get("target") or "all").lower()
        if target == "admins" and not user.is_admin:
            continue
        out.append({
            "id": n.get("id"),
            "title": n.get("title"),
            "body": n.get("body"),
            "level": n.get("level", "info"),
            "target": target,
            "created_at": n.get("created_at"),
        })
    return out


@api_router.get("/admin/notifications")
async def admin_list_notifications(_: AuthedUser = Depends(require_admin)):
    rows = await db.notifications.find({}, sort=[("created_at", -1)], limit=100)
    return rows


@api_router.post("/admin/notifications")
async def admin_create_notification(payload: NotificationIn,
                                    _: AuthedUser = Depends(require_admin)):
    level = payload.level.lower()
    if level not in {"info", "warning", "critical", "success"}:
        level = "info"
    target = payload.target.lower()
    if target not in {"all", "admins"}:
        target = "all"
    row = {
        "id": str(uuid.uuid4()),
        "title": payload.title,
        "body": payload.body,
        "level": level,
        "target": target,
        "active": True,
        "created_at": datetime.utcnow(),
    }
    await db.notifications.insert_one(row)
    return {"ok": True, "id": row["id"]}


@api_router.delete("/admin/notifications/{notification_id}")
async def admin_delete_notification(notification_id: str,
                                    _: AuthedUser = Depends(require_admin)):
    await db.notifications.delete_one({"id": notification_id})
    return {"ok": True}


# ---------------------------------------------------------------------------
# Jikan proxy with TTL cache. Avoids browser-side rate limits / CORS hiccups.
# Public read-only.
# ---------------------------------------------------------------------------
JIKAN_BASE = "https://api.jikan.moe/v4"
ANILIST_GQL = "https://graphql.anilist.co"


# ---------------------------------------------------------------------------
# AniList fallback — when Jikan is degraded, fetch equivalent lists from AniList
# and shape the response like Jikan: {"data":[{mal_id, title, ...}, ...]}.
# ---------------------------------------------------------------------------
ANILIST_MEDIA_FRAGMENT = """
  id idMal
  title { romaji english native }
  coverImage { extraLarge large }
  bannerImage
  averageScore meanScore
  episodes status format
  seasonYear
  description(asHtml:false)
  genres
  rankings { rank type allTime }
"""


def _anilist_to_jikan_anime(m: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not m or not m.get("idMal"):
        return None
    title = m.get("title") or {}
    img = m.get("coverImage") or {}
    cover = img.get("extraLarge") or img.get("large") or m.get("bannerImage") or ""
    score = m.get("averageScore") or m.get("meanScore")
    return {
        "mal_id": m["idMal"],
        "title": title.get("romaji") or title.get("english") or title.get("native") or "Untitled",
        "title_english": title.get("english"),
        "images": {"jpg": {"large_image_url": cover, "image_url": cover}},
        "score": (score / 10.0) if isinstance(score, (int, float)) and score > 10 else score,
        "episodes": m.get("episodes"),
        "year": m.get("seasonYear"),
        "type": m.get("format"),
        "status": (m.get("status") or "").replace("_", " ").title() or None,
        "synopsis": (m.get("description") or "").replace("<br>", "\n").replace("<i>", "").replace("</i>", ""),
        "genres": [{"name": g} for g in (m.get("genres") or [])],
    }


async def _anilist_query(query: str, variables: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            r = await http.post(ANILIST_GQL,
                                json={"query": query, "variables": variables},
                                headers={"Accept": "application/json"})
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


async def _anilist_list(sort: List[str], extra_filter: Optional[str] = None,
                        per_page: int = 18, page: int = 1) -> Optional[List[Dict[str, Any]]]:
    extra = f", {extra_filter}" if extra_filter else ""
    query = f"""
      query($page:Int,$perPage:Int){{
        Page(page:$page, perPage:$perPage){{
          media(type:ANIME, isAdult:false, sort:[{','.join(sort)}]{extra}) {{
            {ANILIST_MEDIA_FRAGMENT}
          }}
        }}
      }}"""
    res = await _anilist_query(query, {"page": page, "perPage": per_page})
    if not res:
        return None
    media = (((res.get("data") or {}).get("Page") or {}).get("media") or [])
    return [m for m in (_anilist_to_jikan_anime(x) for x in media) if m]


async def _anilist_for_jikan_path(path: str, request: Request) -> Optional[Dict[str, Any]]:
    """Map a Jikan-style path → an AniList equivalent. Returns Jikan-shaped data or None."""
    qp = dict(request.query_params)
    limit = int(qp.get("limit") or 18)
    page = int(qp.get("page") or 1)

    if path == "top/anime":
        f = (qp.get("filter") or "").lower()
        if f == "airing":
            data = await _anilist_list(["TRENDING_DESC"],
                                       extra_filter="status:RELEASING",
                                       per_page=limit, page=page)
        else:
            data = await _anilist_list(["SCORE_DESC"], per_page=limit, page=page)
        if data is not None:
            return {"data": data}
    if path == "seasons/now":
        data = await _anilist_list(["POPULARITY_DESC"],
                                   extra_filter="status:RELEASING",
                                   per_page=limit, page=page)
        if data is not None:
            return {"data": data}
    if path == "seasons/upcoming":
        data = await _anilist_list(["POPULARITY_DESC"],
                                   extra_filter="status:NOT_YET_RELEASED",
                                   per_page=limit, page=page)
        if data is not None:
            return {"data": data}
    if path == "anime":
        if qp.get("status") == "airing" and qp.get("order_by") == "start_date":
            data = await _anilist_list(["START_DATE_DESC"],
                                       extra_filter="status:RELEASING",
                                       per_page=limit, page=page)
            if data is not None:
                return {"data": data}
        elif qp.get("q"):
            q = qp["q"]
            query = f"""
              query($s:String,$page:Int,$perPage:Int){{
                Page(page:$page, perPage:$perPage){{
                  media(type:ANIME, isAdult:false, search:$s, sort:[SEARCH_MATCH]){{
                    {ANILIST_MEDIA_FRAGMENT}
                  }}
                }}
              }}"""
            res = await _anilist_query(query, {"s": q, "page": page, "perPage": limit})
            if res:
                media = (((res.get("data") or {}).get("Page") or {}).get("media") or [])
                return {"data": [m for m in (_anilist_to_jikan_anime(x) for x in media) if m]}
        elif qp.get("genres"):
            try:
                # genres param is mal_id ints in Jikan; AniList uses names. Bail.
                pass
            except Exception:
                pass
            data = await _anilist_list(["SCORE_DESC"], per_page=limit, page=page)
            if data is not None:
                return {"data": data}
        else:
            data = await _anilist_list(["POPULARITY_DESC"], per_page=limit, page=page)
            if data is not None:
                return {"data": data}

    if path.startswith("anime/") and path.endswith("/full"):
        try:
            mal_id = int(path.split("/")[1])
        except Exception:
            return None
        query = f"""
          query($idMal:Int){{
            Media(type:ANIME, idMal:$idMal){{
              {ANILIST_MEDIA_FRAGMENT}
            }}
          }}"""
        res = await _anilist_query(query, {"idMal": mal_id})
        m = ((res or {}).get("data") or {}).get("Media")
        out = _anilist_to_jikan_anime(m) if m else None
        if out:
            return {"data": out}

    return None
# ---------------------------------------------------------------------------


@api_router.get("/jikan/{path:path}")
async def jikan_proxy(path: str, request: Request):
    qs = request.url.query
    cache_key = f"jikan:{path}?{qs}"

    fresh = await _cache_get(cache_key)
    if fresh is not None:
        return fresh

    url = f"{JIKAN_BASE}/{path}"
    last_status = 0
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=15.0,
                                         headers={"User-Agent": "Lumen/1.0"}) as http:
                r = await http.get(url, params=dict(request.query_params))
            last_status = r.status_code
        except Exception:
            last_status = 0
            r = None
        if r is not None and r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                data = None
            # Jikan sometimes returns HTTP 200 with body {"status":500,"type":"Exception"}
            if isinstance(data, dict) and data.get("type") == "Exception":
                last_status = int(data.get("status", 500))
                data = None
            if data is not None:
                ttl = 1800  # 30 min
                if path.startswith("anime/") and ("/episodes" in path or path.endswith("/full")):
                    ttl = 21600  # 6h for show details
                elif path.startswith(("top/", "seasons/", "anime")):
                    ttl = 3600   # 1h for lists
                await _cache_set(cache_key, data, ttl_seconds=ttl)
                return data
        if r is not None and r.status_code == 404:
            empty = {"data": []}
            await _cache_set(cache_key, empty, ttl_seconds=300)
            return empty
        # 429 / 500 → small backoff & retry once
        import asyncio
        await asyncio.sleep(0.7)

    # All retries failed → try AniList fallback
    al = await _anilist_for_jikan_path(path, request)
    logger.info("Jikan→AniList fallback for path=%s qp=%s → %s",
                path, dict(request.query_params),
                f"{len(al.get('data', []))} items" if isinstance(al, dict) else al)
    if al is not None:
        # only cache non-empty fallbacks
        if al.get("data"):
            await _cache_set(cache_key, al, ttl_seconds=900)
        return al

    # Final fallback: serve stale cache if any, else empty
    stale = await _cache_get(cache_key, allow_stale=True)
    if stale is not None:
        return stale
    logger.warning("Jikan proxy %s failed (last=%s) — returning empty", path, last_status)
    return {"data": [], "_stale": True, "_upstream_status": last_status}


# ---------------------------------------------------------------------------
# Anikoto resolver: try to map a MAL ID → Anikoto series ID by fuzzy title match.
# We fetch the Jikan anime title (cached), then walk Anikoto's recent feed
# (also cached) to find the closest title. Falls back to None.
# ---------------------------------------------------------------------------
def _norm_title(s: str) -> str:
    import re
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


async def _jikan_title(mal_id: int) -> Optional[Dict[str, Any]]:
    cache_key = f"jikan:anime/{mal_id}/full?"
    cached = await _cache_get(cache_key)
    if cached:
        return (cached.get("data") or {}) if isinstance(cached, dict) else None
    try:
        async with httpx.AsyncClient(timeout=15.0,
                                     headers={"User-Agent": "Lumen/1.0"}) as http:
            r = await http.get(f"{JIKAN_BASE}/anime/{mal_id}/full")
        if r.status_code != 200:
            return None
        data = r.json()
        await _cache_set(cache_key, data, ttl_seconds=1800)
        return data.get("data")
    except Exception:
        return None


async def _index_anikoto_rows(rows: List[Dict[str, Any]]) -> int:
    """Persist every (mal_id → anikoto_id) mapping we see into a permanent index.
    Returns the number of rows indexed."""
    if not rows:
        return 0
    count = 0
    for row in rows:
        mid = row.get("mal_id") or row.get("malId")
        aid = row.get("id") or row.get("series_id") or row.get("anikoto_id")
        if not mid or not aid:
            continue
        try:
            mid_int = int(str(mid))
            aid_int = int(str(aid))
        except (TypeError, ValueError):
            continue
        await db.anikoto_mal_index.update_one(
            {"mal_id": mid_int},
            {"$set": {
                "mal_id": mid_int,
                "anikoto_id": aid_int,
                "title": row.get("title") or row.get("name"),
                "slug": row.get("slug"),
                "indexed_at": datetime.utcnow(),
            }},
            upsert=True,
        )
        count += 1
    return count


async def _anikoto_crawl_for_mal(target_mal_id: int, max_pages: int = 60,
                                 per_page: int = 50) -> Optional[Dict[str, Any]]:
    """Crawl the Anikoto /recent-anime feed, persisting the MAL→anikoto index
    as we go. Stops early when the target mal_id is found.
    Returns the found row or None."""
    target = int(target_mal_id)
    for p in range(1, max_pages + 1):
        key = f"anikoto:recent:{p}:{per_page}"
        cached = await _cache_get(key)
        if cached:
            data = cached
        else:
            try:
                async with httpx.AsyncClient(timeout=15.0,
                                             headers={"User-Agent": "Lumen/1.0"}) as http:
                    r = await http.get(
                        f"{ANIKOTO_BASE}/recent-anime",
                        params={"page": p, "per_page": per_page},
                    )
                if r.status_code != 200:
                    break
                data = r.json()
                await _cache_set(key, data, ttl_seconds=3600)
            except Exception:
                break
        rows = data.get("data") if isinstance(data, dict) else None
        if not rows:
            break
        await _index_anikoto_rows(rows)
        for row in rows:
            try:
                if int(str(row.get("mal_id") or 0)) == target:
                    return row
            except (TypeError, ValueError):
                continue
        pagination = data.get("pagination") if isinstance(data, dict) else None
        total_pages = (pagination or {}).get("total_pages") if isinstance(pagination, dict) else None
        if total_pages and p >= int(total_pages):
            break
    return None


@api_router.get("/anikoto/resolve")
async def anikoto_resolve(mal_id: int):
    """Map a MAL ID → Anikoto series ID.

    Strategy:
      1. Exact lookup in our persistent `anikoto_mal_index` (Mongo, populated
         by every crawl/resolve call). Anikoto rows carry `mal_id` directly,
         so this is exact & reliable — no fuzzy string matching needed.
      2. Cache miss → incrementally crawl the /recent-anime feed, persisting
         every (mal_id → anikoto_id) we see along the way, stopping as soon
         as we hit the target.
      3. Give up after `max_pages` (covers ~3k most recent entries).
    """
    mid = int(mal_id)

    # 1) Direct index hit
    hit = await db.anikoto_mal_index.find_one({"mal_id": mid}, {"_id": 0})
    if hit and hit.get("anikoto_id"):
        return {
            "anikoto_id": int(hit["anikoto_id"]),
            "matched_title": hit.get("title"),
            "score": 1.0,
            "source": "index",
        }

    # 2) Negative cache (so we don't re-crawl every call for titles
    #    Anikoto genuinely doesn't have).
    neg_key = f"anikoto:resolve:miss:{mid}"
    neg = await _cache_get(neg_key)
    if neg is not None:
        return neg

    # 3) Crawl
    found = await _anikoto_crawl_for_mal(mid, max_pages=60, per_page=50)
    if found:
        anikoto_id = found.get("id") or found.get("series_id") or found.get("anikoto_id")
        try:
            anikoto_id_int = int(str(anikoto_id))
        except (TypeError, ValueError):
            anikoto_id_int = None
        if anikoto_id_int:
            return {
                "anikoto_id": anikoto_id_int,
                "matched_title": found.get("title") or found.get("name"),
                "score": 1.0,
                "source": "crawl",
            }

    out = {
        "anikoto_id": None,
        "matched_title": None,
        "score": 0,
        "reason": "MAL ID not found in Anikoto catalog (scanned recent feed).",
    }
    await _cache_set(neg_key, out, ttl_seconds=6 * 3600)  # 6h negative TTL
    return out


# ---------------------------------------------------------------------------
# Streaming: build embed URL for (mal_id, ep, lang). Source can be:
#   - "mal":     https://megaplay.buzz/stream/mal/{mal_id}/{ep}/{lang}
#   - "anikoto": resolve via Anikoto /series/{anikoto_id} → episode_embed_id
#                → https://megaplay.buzz/stream/s-2/{episode_embed_id}/{lang}
# Anikoto MUST be called server-side (their docs). We cache results.
# ---------------------------------------------------------------------------
ANIKOTO_BASE = "https://anikotoapi.site"
MEGAPLAY_BASE = "https://megaplay.buzz"


class StreamOut(BaseModel):
    embed_url: str
    source: str  # "mal" | "anikoto"
    mal_id: int
    episode: int
    lang: str
    episode_embed_id: Optional[str] = None
    title: Optional[str] = None


@api_router.get("/stream", response_model=StreamOut)
async def get_stream(mal_id: int, ep: int = 1, lang: str = "sub",
                     source: str = "mal", anikoto_id: Optional[int] = None):
    lang = (lang or "sub").lower()
    if lang not in ("sub", "dub"):
        raise HTTPException(status_code=400, detail="lang must be sub or dub")

    # Block check
    if await db.banned_anime.find_one({"mal_id": mal_id}):
        raise HTTPException(status_code=403, detail="This title is unavailable")

    if source == "mal":
        url = f"{MEGAPLAY_BASE}/stream/mal/{mal_id}/{ep}/{lang}"
        return StreamOut(embed_url=url, source="mal", mal_id=mal_id, episode=ep, lang=lang)

    if source == "anikoto":
        if not anikoto_id:
            raise HTTPException(status_code=400, detail="anikoto_id required for anikoto source")
        series = await _anikoto_series_cached(anikoto_id)
        episodes = (series or {}).get("episodes") or []
        # find by episode number; fall back to position
        chosen = None
        for e in episodes:
            num = e.get("number") or e.get("episode_number") or e.get("ep_num")
            try:
                if num is not None and int(num) == ep:
                    chosen = e
                    break
            except (TypeError, ValueError):
                pass
        if chosen is None and 0 < ep <= len(episodes):
            chosen = episodes[ep - 1]
        if chosen is None:
            raise HTTPException(status_code=404, detail="Episode not found in Anikoto series")
        embed_id = (chosen.get("episode_embed_id")
                    or chosen.get("embed_id")
                    or chosen.get("id"))
        if not embed_id:
            # try embed_url first
            emb = chosen.get("embed_url") or {}
            url = emb.get(lang) or emb.get("sub") or emb.get("dub")
            if url:
                return StreamOut(embed_url=url, source="anikoto", mal_id=mal_id,
                                 episode=ep, lang=lang, title=chosen.get("title"))
            raise HTTPException(status_code=502, detail="No embed id from Anikoto")
        url = f"{MEGAPLAY_BASE}/stream/s-2/{embed_id}/{lang}"
        return StreamOut(embed_url=url, source="anikoto", mal_id=mal_id,
                         episode=ep, lang=lang, episode_embed_id=str(embed_id),
                         title=chosen.get("title"))

    raise HTTPException(status_code=400, detail="Unknown source")


# ---------------------------------------------------------------------------
# Anikoto proxy (cached). Front-end never hits Anikoto directly.
# ---------------------------------------------------------------------------
async def _cache_get(key: str, allow_stale: bool = False) -> Optional[dict]:
    # Prefer MongoDB-backed cache, but fall back to in-memory cache if DB is unavailable.
    try:
        row = await db.proxy_cache.find_one({"key": key})
        if not row:
            return None
        if not allow_stale and row.get("expires_at") and row["expires_at"] < datetime.utcnow():
            return None
        return row.get("data")
    except Exception:
        # Fallback: in-memory cache stores { key: {"data": ..., "expires_at": datetime } }
        ent = IN_MEMORY_CACHE.get(key)
        if not ent:
            return None
        if not allow_stale and ent.get("expires_at") and ent["expires_at"] < datetime.utcnow():
            return None
        return ent.get("data")


async def _cache_set(key: str, data: dict, ttl_seconds: int):
    from datetime import timedelta
    try:
        await db.proxy_cache.update_one(
            {"key": key},
            {"$set": {"key": key, "data": data,
                      "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds)}},
            upsert=True,
        )
    except Exception:
        # Fallback to in-memory cache for development when MongoDB is unavailable.
        IN_MEMORY_CACHE[key] = {
            "data": data,
            "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds),
        }


async def _anikoto_series_cached(anikoto_id: int) -> Optional[dict]:
    key = f"anikoto:series:{anikoto_id}"
    cached = await _cache_get(key)
    if cached:
        return cached
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            r = await http.get(f"{ANIKOTO_BASE}/series/{anikoto_id}",
                               headers={"User-Agent": "Lumen/1.0"})
        if r.status_code != 200:
            raise HTTPException(status_code=502,
                                detail=f"Anikoto returned {r.status_code}")
        data = r.json()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Anikoto fetch failed")
        raise HTTPException(status_code=502, detail=f"Anikoto unreachable: {e}")
    await _cache_set(key, data, ttl_seconds=600)  # 10 min
    return data


@api_router.get("/anikoto/recent")
async def anikoto_recent(page: int = 1, per_page: int = 20):
    per_page = max(1, min(50, per_page))
    page = max(1, page)
    key = f"anikoto:recent:{page}:{per_page}"
    cached = await _cache_get(key)
    if cached:
        return cached
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            r = await http.get(
                f"{ANIKOTO_BASE}/recent-anime",
                params={"page": page, "per_page": per_page},
                headers={"User-Agent": "Lumen/1.0"},
            )
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Anikoto {r.status_code}")
        data = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Anikoto unreachable: {e}")
    await _cache_set(key, data, ttl_seconds=120)  # 2 min
    return data


@api_router.get("/anikoto/series/{anikoto_id}")
async def anikoto_series(anikoto_id: int):
    data = await _anikoto_series_cached(anikoto_id)
    return data


# ---------------------------------------------------------------------------
# Watch progress (per user, per mal_id, per episode)
# ---------------------------------------------------------------------------
class ProgressIn(BaseModel):
    mal_id: int
    episode: int
    current_time: float = 0
    duration: float = 0
    percent: float = 0
    completed: bool = False
    title: Optional[str] = None
    image_url: Optional[str] = None


@api_router.post("/progress")
async def save_progress(payload: ProgressIn,
                        user: AuthedUser = Depends(require_active)):
    payload_dict = payload.dict()
    await db.progress.update_one(
        {"user_id": user.id, "mal_id": payload.mal_id, "episode": payload.episode},
        {"$set": {**payload_dict, "user_id": user.id, "updated_at": datetime.utcnow()},
         "$setOnInsert": {"id": str(uuid.uuid4()), "created_at": datetime.utcnow()}},
        upsert=True,
    )
    return {"ok": True}


@api_router.get("/progress/me")
async def my_progress(user: AuthedUser = Depends(get_current_user), limit: int = 20):
    rows = await db.progress.find({"user_id": user.id}, sort=[("updated_at", -1)], limit=limit)
    out = []
    for r in rows:
        out.append({
            "mal_id": r["mal_id"], "episode": r["episode"],
            "current_time": r.get("current_time", 0),
            "duration": r.get("duration", 0),
            "percent": r.get("percent", 0),
            "completed": r.get("completed", False),
            "title": r.get("title"),
            "image_url": r.get("image_url"),
            "updated_at": r.get("updated_at"),
        })
    return out


@api_router.delete("/progress/{mal_id}")
async def delete_progress(mal_id: int, user: AuthedUser = Depends(get_current_user)):
    await db.progress.delete_many({"user_id": user.id, "mal_id": mal_id})
    return {"ok": True}


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    return response


# ---------------------------------------------------------------------------
# Wire up
# ---------------------------------------------------------------------------
app.include_router(api_router)

_cors_origins = [o.strip() for o in os.environ.get('CORS_ORIGINS', 'http://localhost:3000,http://localhost:3001,http://localhost:3002').split(',') if o.strip()]
_allow_all = '*' in _cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_credentials=not _allow_all,
    allow_origins=['*'] if _allow_all else _cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup():
    # Load file-backed collections into memory
    try:
        await db.comments.load()
        await db.ratings.load()
        await db.banned_users.load()
        await db.banned_anime.load()
        await db.security_log.load()
        await db.progress.load()
        await db.proxy_cache.load()
        await db.anikoto_mal_index.load()
        await db.notifications.load()
        logger.info("Lumen API ready (file-backed storage). Admin email: %s", ADMIN_EMAIL)
    except Exception as e:
        logger.warning("Failed loading file-backed collections: %s", e)


@app.on_event("shutdown")
async def shutdown_db_client():
    # Persist collections to disk on shutdown
    try:
        await db.comments._persist()
        await db.ratings._persist()
        await db.banned_users._persist()
        await db.banned_anime._persist()
        await db.security_log._persist()
        await db.progress._persist()
        await db.proxy_cache._persist()
        await db.anikoto_mal_index._persist()
        await db.notifications._persist()
    except Exception:
        pass
