# backend/app/routers/auth.py
"""
Authentication & identity helpers.

* POST /auth/register  → create a user, return JWT
* POST /auth/login     → OAuth2-style login, return JWT
* current_user_id()    → dependency that yields the caller’s UUID

The router now uses plain SQLAlchemy `select()` instead of the SQLModel
helper, so it works with the declarative `models/user.py` you have in
place.
"""

from datetime import datetime, timedelta
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.database import get_db
from ..core.settings import settings
from ..models.user import User                  # ← your declarative model
from ..models.dto_auth import Token, UserCreate

router = APIRouter(prefix="/auth", tags=["auth"])

# ---------------- crypto helpers ---------------- #
pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def _hash(pw: str) -> str:
    return pwd_ctx.hash(pw)


def _verify(pw: str, hashed: str) -> bool:
    return pwd_ctx.verify(pw, hashed)


def _make_jwt(sub: str) -> str:
    """Issue a short-lived (24 h) access-token."""
    exp = datetime.utcnow() + timedelta(days=1)
    return jwt.encode(
        {"sub": sub, "exp": exp},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


# ---------------- API routes ---------------- #
@router.post("/register", response_model=Token, status_code=201)
async def register(body: UserCreate, db: AsyncSession = Depends(get_db)):
    """Create a new user account and return a JWT."""
    # 1️⃣  make sure the e-mail isn't already taken
    exists_stmt = select(User.id).where(User.email == body.email)
    if await db.scalar(exists_stmt):
        raise HTTPException(status_code=400, detail="Account already exists")

    # 2️⃣  insert the user
    user = User(
        id=uuid4(),
        email=body.email,
        full_name=body.full_name,
        hashed_pw=_hash(body.password),
        created_at=datetime.utcnow(),
    )
    db.add(user)
    await db.commit()

    # 3️⃣  return token
    return Token(
        access_token=_make_jwt(str(user.id)),
        token_type="bearer",
    )


@router.post("/login", response_model=Token)
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    """Username = e-mail, password check, return JWT on success."""
    stmt = select(User).where(User.email == form.username)
    user: User | None = await db.scalar(stmt)

    if not user or not _verify(form.password, user.hashed_pw):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect e-mail or password",
        )

    return Token(
        access_token=_make_jwt(str(user.id)),
        token_type="bearer",
    )


# ---------------- dependency ---------------- #
async def current_user_id(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> UUID:
    """
    FastAPI dependency – returns the caller’s UUID if the JWT is valid.
    Raises 401 otherwise.
    """
    cred_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
        uid_str: str | None = payload.get("sub")
        if uid_str is None:
            raise cred_exc
        uid = UUID(uid_str)
    except (JWTError, ValueError):
        raise cred_exc

    # (optional) confirm the user still exists
    if not await db.scalar(select(User.id).where(User.id == uid)):
        raise cred_exc

    return uid
