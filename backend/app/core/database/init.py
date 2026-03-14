import asyncio
import os
from sqlalchemy import select, text

# Important: import all ORM models before create_all() so SQLAlchemy metadata is complete.
from app.models import Action, Base, Capability, Pipeline, User, UserRole
from app.core.database.session import SessionLocal, engine
from app.utils.hashing import hash_password


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Best-effort schema drift handling without requiring Alembic.
        # Use DO blocks so missing tables don't abort the whole transaction (and roll back create_all()).
        await conn.execute(
            text(
                """
DO $$
BEGIN
  IF to_regclass('public.actions') IS NOT NULL THEN
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_status VARCHAR(32) NOT NULL DEFAULT 'SUCCEEDED';
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_error TEXT;

    CREATE INDEX IF NOT EXISTS ix_actions_method_path ON actions (method, path);
    CREATE INDEX IF NOT EXISTS ix_actions_is_deleted ON actions (is_deleted);
    CREATE INDEX IF NOT EXISTS ix_actions_ingest_status ON actions (ingest_status);
  END IF;
END $$;
"""
            )
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
