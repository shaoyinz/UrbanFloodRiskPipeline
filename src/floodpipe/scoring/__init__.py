"""Damage-and-EAD scoring primitives.

All functions here are pure: no GCS, no Spark, no BigQuery. The Spark
job calls them inside a pandas UDF; dbt tests can import them too.
"""

from floodpipe.scoring.archetypes import (
    ArchetypeConfig,
    assign_archetype,
    load_archetype_config,
)
from floodpipe.scoring.ddf import damage_fraction, replacement_value_usd
from floodpipe.scoring.ead import (
    expected_annual_damage,
    interpolate_depth_gumbel,
)

__all__ = [
    "ArchetypeConfig",
    "assign_archetype",
    "load_archetype_config",
    "damage_fraction",
    "replacement_value_usd",
    "expected_annual_damage",
    "interpolate_depth_gumbel",
]
