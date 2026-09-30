"""Singletons de proceso (config, base de datos) y rutas derivadas de la configuración."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

from settings import ConfigSettings

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

_config: Optional[ConfigSettings] = None
_database = None


@dataclass(frozen=True)
class RuntimePaths:
    project_root: str
    data_dir: str
    logs_dir: str
    sqlite_path: str
    storage_path: str
    env_file: str

    @classmethod
    def from_config(cls, config: ConfigSettings, project_root: str = PROJECT_ROOT) -> "RuntimePaths":
        data_dir = config.resolve_path(config.DATA_DIR, project_root)
        sqlite = config.SQLITE_PATH or os.path.join(data_dir, "trading.db")
        return cls(
            project_root=project_root,
            data_dir=data_dir,
            logs_dir=config.resolve_path(config.LOGS_DIR, project_root),
            sqlite_path=config.resolve_path(sqlite, project_root),
            storage_path=config.resolve_path(config.STORAGE_PATH, project_root),
            env_file=os.path.join(project_root, ".env"),
        )


def default_config() -> ConfigSettings:
    """Config del proceso (carga ``.env`` de la raíz sin pisar variables ya definidas)."""
    global _config
    if _config is None:
        load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"), override=False)
        _config = ConfigSettings()
    return _config


def default_database():
    global _database
    if _database is None:
        from nertz_core.db import Database

        _database = Database(RuntimePaths.from_config(default_config()).sqlite_path)
    return _database
