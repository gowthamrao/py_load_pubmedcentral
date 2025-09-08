from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Manages application configuration using environment variables.

    Pydantic's BaseSettings will automatically load variables from a .env file
    or from the environment. See Pydantic documentation for more details.
    """

    # Database connection settings
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "pmc"

    # Logging configuration
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_prefix="PMC_")


# Create a single, reusable instance of the settings
settings = Settings()
