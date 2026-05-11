"""Overture class/subtype -> HAZUS occupancy archetype.

Config-driven port of notebook 05's inline ARCHETYPE_RULES. The first
matching rule wins; unmatched footprints get the documented fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class _Rule:
    klass: str
    subtype_contains: str | None
    archetype: str


@dataclass(frozen=True)
class ArchetypeConfig:
    rules: tuple[_Rule, ...]
    fallback: str
    replacement_usd_per_sqft: dict[str, float]
    depth_damage_ft: dict[str, tuple[tuple[float, float], ...]]
    return_periods: tuple[int, ...]


def load_archetype_config(path: str | Path) -> ArchetypeConfig:
    raw = yaml.safe_load(Path(path).read_text())
    rules = tuple(
        _Rule(
            klass=str(r["class"]).lower(),
            subtype_contains=(r["subtype_contains"].lower()
                              if r.get("subtype_contains") else None),
            archetype=str(r["archetype"]),
        )
        for r in raw["mapping"]
    )
    ddf = {
        code: tuple((float(d), float(f)) for d, f in table)
        for code, table in raw["depth_damage_ft"].items()
    }
    return ArchetypeConfig(
        rules=rules,
        fallback=str(raw["fallback_archetype"]),
        replacement_usd_per_sqft={k: float(v) for k, v in raw["replacement_usd_per_sqft"].items()},
        depth_damage_ft=ddf,
        return_periods=tuple(int(t) for t in raw["return_periods"]),
    )


def assign_archetype(klass: str | None, subtype: str | None, config: ArchetypeConfig) -> str:
    k = klass.lower() if isinstance(klass, str) else ""
    s = subtype.lower() if isinstance(subtype, str) else ""
    for rule in config.rules:
        if rule.klass and rule.klass not in k:
            continue
        if rule.subtype_contains and rule.subtype_contains not in s:
            continue
        return rule.archetype
    return config.fallback
