from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional


class SlackConfig(BaseModel):
    webhook_url: str
    channel: str = Field(default="#Alertas")
    enabled: bool = Field(default=True)


class QualityConfig(BaseModel):
    min_completeness: float = Field(ge=0.0, le=1.0, default=0.95)
    min_uniqueness: float = Field(ge=0.0, le=1.0, default=0.90)
    anomaly_threshold: float = Field(gt=0.0, default=3.0)


class DataSource(BaseModel):
    name: str
    type: str = Field(pattern="^(database|csv)$")
    path: Optional[str] = None


class MonitoringConfig(BaseModel):
    check_interval_minutes: int = Field(gt=0, default=30)
    data_sources: List[DataSource]


class Config(BaseModel):
    slack: SlackConfig
    quality: QualityConfig
    monitoring: MonitoringConfig

    class Config:
        validate_assignment = True