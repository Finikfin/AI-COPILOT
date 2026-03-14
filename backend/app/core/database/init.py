import asyncio
import os
from sqlalchemy import select, text

from app.models import Base, User, UserRole
from app.core.database.session import SessionLocal, engine
from app.utils.hashing import hash_password


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text("ALTER TABLE actions ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE")
        )
        await conn.execute(
            text("ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_status VARCHAR(32) NOT NULL DEFAULT 'SUCCEEDED'")
        )
        await conn.execute(
            text("ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_error TEXT")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_actions_method_path ON actions (method, path)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_actions_is_deleted ON actions (is_deleted)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_actions_ingest_status ON actions (ingest_status)")
        )

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
