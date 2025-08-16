"""
Database connection and session management for the CSV Import Guardian Agent System.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import create_engine, event, pool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from shared.config import get_settings


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


class DatabaseManager:
    """Database connection manager with async support."""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.async_engine = None
        self.sync_engine = None
        self.async_session_factory = None
        self.sync_session_factory = None
        
    async def initialize(self):
        """Initialize database connections."""
        # Async engine for agents
        self.async_engine = create_async_engine(
            self.database_url.replace("postgresql://", "postgresql+asyncpg://"),
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
        )
        
        # Sync engine for migrations and admin tasks
        self.sync_engine = create_engine(
            self.database_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
        )
        
        # Session factories
        self.async_session_factory = async_sessionmaker(
            self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        self.sync_session_factory = sessionmaker(
            self.sync_engine,
            expire_on_commit=False,
        )
        
        # Set up connection event listeners
        self._setup_connection_events()
        
    def _setup_connection_events(self):
        """Set up database connection event listeners."""
        
        @event.listens_for(self.sync_engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Set SQLite pragmas if using SQLite."""
            if "sqlite" in self.database_url:
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
                
        @event.listens_for(self.async_engine.sync_engine, "connect")
        def set_postgres_settings(dbapi_connection, connection_record):
            """Set PostgreSQL settings."""
            if "postgresql" in self.database_url:
                cursor = dbapi_connection.cursor()
                cursor.execute("SET timezone='UTC'")
                cursor.execute("SET statement_timeout='300s'")
                cursor.close()
    
    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async database session."""
        if not self.async_session_factory:
            raise RuntimeError("Database not initialized. Call initialize() first.")
            
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    def get_sync_session(self):
        """Get a sync database session."""
        if not self.sync_session_factory:
            raise RuntimeError("Database not initialized. Call initialize() first.")
            
        return self.sync_session_factory()
    
    async def close(self):
        """Close database connections."""
        if self.async_engine:
            await self.async_engine.dispose()
        if self.sync_engine:
            self.sync_engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


async def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    
    if _db_manager is None:
        settings = get_settings()
        _db_manager = DatabaseManager(settings.database_url)
        await _db_manager.initialize()
    
    return _db_manager


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session (dependency injection helper)."""
    db_manager = await get_database_manager()
    async with db_manager.get_async_session() as session:
        yield session


def get_sync_session():
    """Get a sync database session."""
    db_manager = asyncio.run(get_database_manager())
    return db_manager.get_sync_session()


# Health check functions

async def check_database_health() -> dict:
    """Check database connectivity and performance."""
    try:
        db_manager = await get_database_manager()
        
        # Test async connection
        async with db_manager.get_async_session() as session:
            result = await session.execute("SELECT 1 as health_check")
            row = result.fetchone()
            
        # Test sync connection
        with db_manager.get_sync_session() as session:
            result = session.execute("SELECT version() as db_version")
            version_row = result.fetchone()
            
        return {
            "status": "healthy",
            "async_connection": "ok",
            "sync_connection": "ok",
            "database_version": version_row[0] if version_row else "unknown",
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }


# Row Level Security helpers

async def set_rls_context(session: AsyncSession, client_id: str):
    """Set row-level security context for a session."""
    await session.execute(
        "SET LOCAL row_security.client_id = :client_id",
        {"client_id": client_id}
    )


def create_rls_policy(table_name: str, client_id_column: str = "client_id") -> str:
    """Generate RLS policy SQL for a table."""
    return f"""
    CREATE POLICY {table_name}_client_isolation ON {table_name}
    FOR ALL TO PUBLIC
    USING ({client_id_column} = current_setting('row_security.client_id', true));
    """


# Connection pooling utilities

class ConnectionPoolMonitor:
    """Monitor database connection pool health."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    async def get_pool_stats(self) -> dict:
        """Get connection pool statistics."""
        stats = {}
        
        if self.db_manager.async_engine:
            pool = self.db_manager.async_engine.pool
            stats["async_pool"] = {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalid": pool.invalid(),
            }
            
        if self.db_manager.sync_engine:
            pool = self.db_manager.sync_engine.pool
            stats["sync_pool"] = {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalid": pool.invalid(),
            }
            
        return stats
    
    async def is_pool_healthy(self) -> bool:
        """Check if connection pools are healthy."""
        try:
            stats = await self.get_pool_stats()
            
            # Check for pool exhaustion
            for pool_name, pool_stats in stats.items():
                if pool_stats["checked_out"] >= pool_stats["size"] + pool_stats["overflow"]:
                    return False
                    
            return True
            
        except Exception:
            return False


# Transaction utilities

@asynccontextmanager
async def atomic_transaction(session: AsyncSession):
    """Context manager for atomic transactions with savepoints."""
    savepoint = await session.begin_nested()
    try:
        yield session
        await savepoint.commit()
    except Exception:
        await savepoint.rollback()
        raise


async def execute_in_transaction(session: AsyncSession, operations: list):
    """Execute multiple operations in a single transaction."""
    async with atomic_transaction(session):
        results = []
        for operation in operations:
            if callable(operation):
                result = await operation(session)
                results.append(result)
            else:
                result = await session.execute(operation)
                results.append(result)
        return results


# Migration utilities

def run_migrations():
    """Run database migrations using Alembic."""
    from alembic import command
    from alembic.config import Config
    
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")


def create_migration(message: str):
    """Create a new database migration."""
    from alembic import command
    from alembic.config import Config
    
    alembic_cfg = Config("alembic.ini")
    command.revision(alembic_cfg, message=message, autogenerate=True)