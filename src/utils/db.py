import os
import json
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def get_db_url_from_env() -> str:
    # Ensure we load .env from repository root if present. This file lives at src/utils,
    # so repo root is two levels up.
    try:
        repo_root_env = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
        if os.path.exists(repo_root_env):
            load_dotenv(repo_root_env)
    except Exception:
        # ignore dotenv loading errors; os.getenv will still work for environment-set vars
        pass

    host = os.getenv("DB_HOST", "127.0.0.200")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "nconsu5_frauddetection")
    user = os.getenv("DB_USER", "nconsu5_frauddata")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE", "prefer")
    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is required to connect to the Postgres instance. Copy .env.example -> .env and fill values.")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}?sslmode={sslmode}"


def get_engine():
    url = get_db_url_from_env()
    # create engine with sensible defaults for local usage
    engine = create_engine(url, pool_size=5, max_overflow=10, pool_pre_ping=True)
    return engine


def create_transactions_table(engine):
    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id BIGSERIAL PRIMARY KEY,
            transaction_id TEXT UNIQUE,
            user_id TEXT,
            payload JSONB,
            amount NUMERIC,
            is_fraud INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(ddl)
    except SQLAlchemyError as e:
        logger.exception("Failed to create transactions table: %s", e)
        raise


def insert_transaction(engine, transaction: dict):
    try:
        payload = json.dumps(transaction, default=str)
        stmt = text(
            "INSERT INTO transactions (transaction_id, user_id, payload, amount, is_fraud) "
            "VALUES (:transaction_id, :user_id, :payload, :amount, :is_fraud) "
            "ON CONFLICT (transaction_id) DO NOTHING"
        )
        params = {
            "transaction_id": transaction.get("transaction_id"),
            "user_id": transaction.get("user_id"),
            "payload": payload,
            "amount": transaction.get("amount"),
            "is_fraud": int(transaction.get("is_fraud", 0))
        }
        with engine.begin() as conn:
            conn.execute(stmt, params)
    except SQLAlchemyError:
        logger.exception("Failed to insert transaction into Postgres")
        # swallow exception so producer can continue
