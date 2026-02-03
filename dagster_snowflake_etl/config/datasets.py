"""Dataset configuration schema and loader."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class EntityConfig:
    """Configuration for a single entity (table) within a dataset."""

    name: str
    key_field: str  # Field in DOC to use as KEY
    prep_columns: list[dict[str, str]] = field(default_factory=list)  # Column extractions for PREP view
    description: str = ""

    @property
    def raw_table(self) -> str:
        return f"RAW_{self.name.upper()}"

    @property
    def psa_table(self) -> str:
        return f"PSA_{self.name.upper()}"

    @property
    def prep_view(self) -> str:
        return f"PREP_{self.name.upper()}"

    @property
    def pres_view(self) -> str:
        return self.name.upper()


@dataclass
class DatabaseConfig:
    """Configuration for a single database (contains RAW/PSA/PREP/PRES schemas)."""

    name: str
    raw_schema: str = "RAW"
    psa_schema: str = "PSA"
    prep_schema: str = "PREP"
    pres_schema: str = "PRES"
    entities: list[EntityConfig] = field(default_factory=list)
    description: str = ""

    def __post_init__(self):
        # Convert dict entities to EntityConfig objects
        if self.entities and isinstance(self.entities[0], dict):
            self.entities = [EntityConfig(**e) for e in self.entities]


@dataclass
class DatasetConfig:
    """Root configuration containing all databases."""

    databases: list[DatabaseConfig] = field(default_factory=list)

    def __post_init__(self):
        # Convert dict databases to DatabaseConfig objects
        if self.databases and isinstance(self.databases[0], dict):
            self.databases = [DatabaseConfig(**db) for db in self.databases]

    def get_database(self, name: str) -> DatabaseConfig | None:
        """Get a database config by name."""
        for db in self.databases:
            if db.name == name:
                return db
        return None

    @property
    def all_entities(self) -> list[tuple[DatabaseConfig, EntityConfig]]:
        """Get all (database, entity) pairs."""
        result = []
        for db in self.databases:
            for entity in db.entities:
                result.append((db, entity))
        return result


def load_datasets_config(config_path: str | Path) -> DatasetConfig:
    """Load datasets configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    return DatasetConfig(**data)


def load_datasets_from_dict(data: dict[str, Any]) -> DatasetConfig:
    """Load datasets configuration from a dictionary."""
    return DatasetConfig(**data)
