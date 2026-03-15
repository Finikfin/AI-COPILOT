import asyncio
import os
from sqlalchemy import select, text

# Important: import all ORM models before create_all() so SQLAlchemy metadata is complete.
from app.models import (
    Action,
    Base,
    Capability,
    DialogMessageRole,
    ExecutionRun,
    ExecutionStepRun,
    Pipeline,
    PipelineDialog,
    PipelineDialogMessage,
    User,
    UserRole,
)
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
DECLARE
  cap_constraint_name TEXT;
  admin_user_id UUID;
BEGIN
  IF to_regclass('public.actions') IS NOT NULL THEN
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_status VARCHAR(32) NOT NULL DEFAULT 'SUCCEEDED';
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS ingest_error TEXT;
    ALTER TABLE actions ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS ix_actions_method_path ON actions (method, path);
    CREATE INDEX IF NOT EXISTS ix_actions_is_deleted ON actions (is_deleted);
    CREATE INDEX IF NOT EXISTS ix_actions_ingest_status ON actions (ingest_status);
    CREATE INDEX IF NOT EXISTS ix_actions_user_id ON actions (user_id);
  END IF;
  IF to_regclass('public.capabilities') IS NOT NULL THEN
    ALTER TABLE capabilities ADD COLUMN IF NOT EXISTS type VARCHAR(50) DEFAULT 'ATOMIC';
    ALTER TABLE capabilities ADD COLUMN IF NOT EXISTS recipe JSONB;
    ALTER TABLE capabilities ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE CASCADE;
    ALTER TABLE capabilities ALTER COLUMN action_id DROP NOT NULL;
    
    CREATE INDEX IF NOT EXISTS ix_capabilities_type ON capabilities (type);
    CREATE INDEX IF NOT EXISTS ix_capabilities_user_id ON capabilities (user_id);

    FOR cap_constraint_name IN
      SELECT c.conname
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace ns ON ns.oid = t.relnamespace
      WHERE ns.nspname = 'public'
        AND t.relname = 'capabilities'
        AND c.contype = 'u'
        AND array_length(c.conkey, 1) = 1
        AND c.conkey[1] = (
          SELECT a.attnum
          FROM pg_attribute a
          WHERE a.attrelid = t.oid
            AND a.attname = 'action_id'
            AND a.attnum > 0
            AND NOT a.attisdropped
          LIMIT 1
        )
    LOOP
      EXECUTE format('ALTER TABLE capabilities DROP CONSTRAINT IF EXISTS %I', cap_constraint_name);
    END LOOP;

    CREATE UNIQUE INDEX IF NOT EXISTS uq_capabilities_user_action
      ON capabilities (user_id, action_id)
      WHERE action_id IS NOT NULL;
  END IF;
  IF to_regclass('public.users') IS NOT NULL THEN
    SELECT id
    INTO admin_user_id
    FROM users
    WHERE role::text = 'ADMIN'
    ORDER BY created_at ASC
    LIMIT 1;

    IF admin_user_id IS NOT NULL THEN
      IF to_regclass('public.actions') IS NOT NULL THEN
        UPDATE actions SET user_id = admin_user_id WHERE user_id IS NULL;
      END IF;
      IF to_regclass('public.capabilities') IS NOT NULL THEN
        UPDATE capabilities SET user_id = admin_user_id WHERE user_id IS NULL;
      END IF;
    END IF;
  END IF;
  IF to_regclass('public.pipeline_dialogs') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS ix_pipeline_dialogs_user_updated_at_desc
      ON pipeline_dialogs (user_id, updated_at DESC);
  END IF;
  IF to_regclass('public.pipeline_dialog_messages') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS ix_pipeline_dialog_messages_dialog_created_at_asc
      ON pipeline_dialog_messages (dialog_id, created_at ASC);
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
