import asyncio
import os
from sqlalchemy import select

from app.models import Base, User, UserRole
from app.core.database.session import SessionLocal, engine
from app.utils.hashing import hash_password


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with SessionLocal() as session:
        admin_email = os.getenv("ADMIN_EMAIL")
        admin_password = os.getenv("ADMIN_PASSWORD")
        admin_fullname = os.getenv("ADMIN_FULLNAME", "System Admin")

        if admin_email and admin_password:
            result = await session.execute(
                select(User).where(User.email == admin_email)
            )
            existing_admin = result.scalar_one_or_none()

            if not existing_admin:
                new_admin = User(
                    email=admin_email,
                    hashed_password=hash_password(admin_password),
                    full_name=admin_fullname,
                    role=UserRole.ADMIN,
                    is_active=True
                )
                session.add(new_admin)
                await session.commit()

if __name__ == "__main__":
    asyncio.run(init_db())
