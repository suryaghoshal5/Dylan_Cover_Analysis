"""Decade and named-era tagging for Dylan cover analysis."""

from __future__ import annotations

import pandas as pd

NAMED_ERAS: dict[str, tuple[int, int]] = {
    "Pre-Electric Dylan": (1962, 1964),
    "Electric Revolution": (1965, 1966),
    "Basement Tapes / Nashville": (1967, 1974),
    "Rolling Thunder": (1975, 1978),
    "Gospel Period": (1979, 1981),
    "Comeback / Oh Mercy": (1982, 1990),
    "Time Out of Mind Revival": (1991, 2001),
    "Modern Standards": (2002, 2012),
    "Nobel Laureate": (2013, 2026),
}

POLITICAL_ERAS: dict[str, tuple[int, int]] = {
    "Vietnam": (1963, 1975),
    "Punk / Reagan": (1976, 1989),
    "Post-Cold War": (1990, 2000),
    "Post-9/11": (2001, 2009),
    "Occupy / Trump": (2010, 2019),
    "BLM / COVID": (2020, 2026),
}


def year_to_decade(year: int | float | None) -> str | None:
    if year is None or pd.isna(year):
        return None
    y = int(year)
    return f"{(y // 10) * 10}s"


def year_to_dylan_era(year: int | float | None) -> str | None:
    if year is None or pd.isna(year):
        return None
    y = int(year)
    for era, (start, end) in NAMED_ERAS.items():
        if start <= y <= end:
            return era
    return None


def year_to_political_era(year: int | float | None) -> str | None:
    if year is None or pd.isna(year):
        return None
    y = int(year)
    for era, (start, end) in POLITICAL_ERAS.items():
        if start <= y <= end:
            return era
    if y < 1963:
        return "Pre-Vietnam"
    return None


def add_era_columns(df: pd.DataFrame, year_col: str = "year") -> pd.DataFrame:
    """Add decade, dylan_era, and political_era columns based on a year column."""
    df = df.copy()
    df["decade"] = df[year_col].map(year_to_decade)
    df["dylan_era"] = df[year_col].map(year_to_dylan_era)
    df["political_era"] = df[year_col].map(year_to_political_era)
    return df
