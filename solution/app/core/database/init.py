import asyncio
import os
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.database.session import engine
from app.core.database.models.base import Base, User, UserRole
from app.utils.hashing import hash_password

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session = sessionmaker(
        engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    async with async_session() as session:
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