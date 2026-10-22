"""
Configuration management for Clickstream Analytics Platform.
Loads from .env and provides typed settings (Pydantic v2 compatible).
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field


# Load the .env file at the repository root regardless of the working directory
# (Airflow and other runners do not start in the repo root).
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


class AWSConfig(BaseModel):
    access_key_id: str = Field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", ""))
    secret_access_key: str = Field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", ""))
    region: str = Field(default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    bucket: str = Field(default_factory=lambda: os.getenv("CLICKSTREAM_BUCKET", "your-bucket"))

    class Config:
        env_prefix = "AWS_"


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic_events: str = Field(default_factory=lambda: os.getenv("KAFKA_TOPIC_EVENTS", "clickstream-events"))
    group_id: str = Field(default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "clickstream_consumer"))
    batch_size: int = 1000
    timeout_ms: int = 10000

    class Config:
        env_prefix = "KAFKA_"


class SparkConfig(BaseModel):
    master: str = Field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[2]"))
    checkpoint_path: str = Field(default_factory=lambda: os.getenv("SPARK_CHECKPOINT_PATH", "./spark-checkpoints"))
    shuffle_partitions: int = Field(default_factory=lambda: int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "2")))
    stream_trigger_interval: str = "10 seconds"
    watermark_delay: str = "10 minutes"

    class Config:
        env_prefix = "SPARK_"


class DeltaConfig(BaseModel):
    # Production: S3 paths (required for cloud deployment)
    s3_base: str = Field(default_factory=lambda: os.getenv("S3_DELTA_BASE", "s3a://your-bucket/delta-warehouse"))
    
    # Development fallback: local paths (for Windows/local dev)
    local_base: str = Field(default_factory=lambda: os.getenv("LOCAL_DELTA_BASE", "./delta-lake"))
    use_s3: bool = False  # Default to local for dev; set True for cloud
    
    @property
    def lake_path(self) -> str:
        """Get the appropriate data lake path (S3 or local)."""
        dev_mode = os.getenv("DEV_MODE", "true").lower() == "true"
        
        if dev_mode:
            # Local development mode
            return self.local_base
        elif self.use_s3 and self.s3_base.startswith("s3a://"):
            # Production S3 mode
            return self.s3_base
        else:
            raise ValueError(
                "Invalid Delta config. Set DEV_MODE=true for local or S3_DELTA_BASE for cloud."
            )
    
    @property
    def checkpoint_path(self) -> str:
        base = self.lake_path
        return f"{base}/checkpoints"
    
    @property
    def bronze_path(self) -> str:
        return f"{self.lake_path}/bronze"
    
    @property
    def silver_path(self) -> str:
        return f"{self.lake_path}/silver"
    
    @property
    def gold_path(self) -> str:
        return f"{self.lake_path}/gold"

    class Config:
        env_prefix = "DELTA_"


class ForecastConfig(BaseModel):
    forecast_days: int = Field(default_factory=lambda: int(os.getenv("FORECAST_DAYS", "14")))
    model_registry: str = Field(default_factory=lambda: os.getenv("FORECAST_MODEL_REGISTRY", "mlflow_models"))
    enabled: bool = True

    class Config:
        env_prefix = "FORECAST_"


class SimulatorConfig(BaseModel):
    event_rate_per_sec: int = Field(default_factory=lambda: int(os.getenv("EVENT_RATE_PER_SEC", "10")))
    simulation_days: int = Field(default_factory=lambda: int(os.getenv("SIMULATION_DAYS", "7")))

    class Config:
        env_prefix = "EVENT_"


class Config:
    """Master configuration object."""
    aws = AWSConfig()
    kafka = KafkaConfig()
    spark = SparkConfig()
    delta = DeltaConfig()
    forecast = ForecastConfig()
    simulator = SimulatorConfig()

    @staticmethod
    def validate():
        """Validate critical configuration."""
        if not Config.kafka.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        return True


# Singleton
config = Config()
