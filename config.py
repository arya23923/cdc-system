import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: str  # sqlite, postgresql, mysql
    host: Optional[str] = None
    port: Optional[int] = None
    database: str = "cdc_demo.db"
    username: Optional[str] = None
    password: Optional[str] = None
    
    @classmethod
    def from_env(cls, prefix: str = "SOURCE"):
        """Load configuration from environment variables"""
        return cls(
            db_type=os.getenv(f"{prefix}_DB_TYPE", "sqlite"),
            host=os.getenv(f"{prefix}_DB_HOST"),
            port=int(os.getenv(f"{prefix}_DB_PORT", "5432")),
            database=os.getenv(f"{prefix}_DB_NAME", "cdc_demo.db"),
            username=os.getenv(f"{prefix}_DB_USER"),
            password=os.getenv(f"{prefix}_DB_PASSWORD")
        )


@dataclass
class CDCConfig:
    """CDC system configuration"""
    # Replication settings
    batch_size: int = 100
    sync_interval_seconds: int = 5
    max_retries: int = 3
    retry_delay_seconds: int = 10
    
    # Monitoring settings
    enable_metrics: bool = True
    metrics_interval_seconds: int = 60
    
    # Table settings
    source_table: str = "users"
    target_table: str = "users_replica"
    
    # CDC strategy
    cdc_strategy: str = "trigger"  # trigger, timestamp, version
    
    @classmethod
    def from_env(cls):
        """Load CDC configuration from environment variables"""
        return cls(
            batch_size=int(os.getenv("CDC_BATCH_SIZE", "100")),
            sync_interval_seconds=int(os.getenv("CDC_SYNC_INTERVAL", "5")),
            max_retries=int(os.getenv("CDC_MAX_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("CDC_RETRY_DELAY", "10")),
            enable_metrics=os.getenv("CDC_ENABLE_METRICS", "true").lower() == "true",
            metrics_interval_seconds=int(os.getenv("CDC_METRICS_INTERVAL", "60")),
            source_table=os.getenv("CDC_SOURCE_TABLE", "users"),
            target_table=os.getenv("CDC_TARGET_TABLE", "users_replica"),
            cdc_strategy=os.getenv("CDC_STRATEGY", "trigger")
        )


# Default configurations
DEFAULT_SOURCE_CONFIG = DatabaseConfig(
    db_type="sqlite",
    database="source.db"
)

DEFAULT_TARGET_CONFIG = DatabaseConfig(
    db_type="sqlite",
    database="target.db"
)

DEFAULT_CDC_CONFIG = CDCConfig()