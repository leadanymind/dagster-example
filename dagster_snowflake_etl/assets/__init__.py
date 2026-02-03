"""Dagster assets for ETL pipeline.

Assets are generated dynamically from configuration.
See assets/factory.py for the asset generation logic.
"""

from .factory import build_assets_from_config

__all__ = ["build_assets_from_config"]
